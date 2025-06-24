package benchmark

import (
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config defines the benchmark parameters passed from CLI
type Config struct {
	KeyCount       int     // total number of keys to generate
	ReadRatio      float64 // ratio of reads vs total ops
	ValueSize      int     // size of values in bytes
	Seed           int64   // RNG seed for deterministic behavior
	DBPath         string  // path to PebbleDB instance
	BenchmarkID    string  // optional label for this benchmark run
	WriteEnabled   bool    // whether to write data to the DB
	KeysFile       string  // optional file with pre-existing keys
	Concurrency    int     // number of concurrent workers
	LogFormat      string  // "json" or "console", default is "console"
	BlockCacheSize int64   // in bytes, negative means disabled (nil)
}

// RunBenchmark orchestrates the full benchmark lifecycle
func RunBenchmark(cfg Config) error {
	setupLog(cfg)
	initialLog(cfg)

	dbConn := createDBConn(cfg)
	defer dbConn.Close()

	var keys iter.Seq[[]byte]
	if cfg.WriteEnabled {
		log.Info().Msg("Generating keys for write mode")
		keys = GenerateKeys(cfg.Seed, cfg.KeyCount)
		if err := runWritePhase(dbConn, cfg, keys); err != nil {
			return err
		}
	} else {
		if cfg.KeysFile != "" {
			log.Info().Str("path", cfg.KeysFile).Msg("Loading keys from file")
			keys = loadKeysFromFile(cfg.KeysFile)
		} else {
			log.Info().Msg("Loading keys from standard input")
			keys = loadKeysFromStdin()
		}
	}

	if err := runReadPhase(dbConn, cfg, keys); err != nil {
		return err
	}

	log.Info().Str("benchmark_id", cfg.BenchmarkID).Msg("Benchmark complete")
	return nil
}

func initialLog(cfg Config) {
	blockCacheInfo := "disabled"
	if cfg.BlockCacheSize >= 0 {
		blockCacheInfo = fmt.Sprintf("enabled, size: %d bytes", uint64(cfg.BlockCacheSize))
	}

	log.Info().
		Str("benchmark_id", cfg.BenchmarkID).
		Int("key_count", cfg.KeyCount).
		Int("value_size", cfg.ValueSize).
		Float64("read_ratio", cfg.ReadRatio).
		Int64("seed", cfg.Seed).
		Str("db_path", cfg.DBPath).
		Bool("write_enabled", cfg.WriteEnabled).
		Str("keys_file", cfg.KeysFile).
		Int("concurrency", cfg.Concurrency).
		Str("block_cache", blockCacheInfo).
		Msg("Starting benchmark")
}

func setupLog(cfg Config) {
	if strings.ToLower(cfg.LogFormat) == "json" {
		zerolog.TimeFieldFormat = time.RFC3339Nano
		log.Logger = log.Output(os.Stdout)
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"})
	}
}

func createDBConn(cfg Config) *pebble.DB {
	opts := &pebble.Options{}
	if !cfg.WriteEnabled {
		opts.ReadOnly = true
	}

	var cache *pebble.Cache
	if cfg.BlockCacheSize >= 0 {
		cache = pebble.NewCache(cfg.BlockCacheSize)
		defer cache.Unref()
	}

	db, err := pebble.Open(cfg.DBPath, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open PebbleDB")
	}
	return db
}

// runWritePhase concurrently writes keys to PebbleDB using iterator
func runWritePhase(db *pebble.DB, cfg Config, keys iter.Seq[[]byte]) error {
	log.Info().Int("workers", cfg.Concurrency).Msg("Beginning write loop")

	jobs := make(chan []byte, cfg.KeyCount)
	writeTimeHistory := make(chan time.Duration, cfg.KeyCount)
	var wg sync.WaitGroup
	var failed, successful uint64

	// Feed keys to workers
	go func() {
		for key := range keys {
			jobs <- key
		}
		close(jobs)
	}()

	// this interval is required to ensure the channel is ready before workers start
	time.Sleep(time.Second)

	// Start workers
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// if there is no key to read, just return
			if len(jobs) == 0 {
				return
			}

			rng := rand.New(rand.NewSource(cfg.Seed + int64(workerID)))
			for key := range jobs {
				value := generateValue(rng, cfg.ValueSize)

				writeStart := time.Now()
				err := db.Set(key, value, pebble.NoSync)
				writeTimeHistory <- time.Since(writeStart)

				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}
				atomic.AddUint64(&successful, 1)
			}
		}(w)
	}

	// Collect results
	wg.Wait()
	close(writeTimeHistory)

	var totalWriteTime time.Duration
	for writeTime := range writeTimeHistory {
		totalWriteTime += writeTime
	}

	elapsed := totalWriteTime.Seconds()
	ops := float64(cfg.KeyCount) / elapsed
	avg := float64(totalWriteTime.Microseconds()) / 1000.0 / float64(cfg.KeyCount)

	log.Info().
		Dur("total_elapsed", totalWriteTime).
		Uint64("failed_writes", atomic.LoadUint64(&failed)).
		Uint64("successful_writes", atomic.LoadUint64(&successful)).
		Float64("ops_per_sec", ops).
		Float64("avg_latency_ms", avg).
		Msg("Write benchmark complete")

	if err := db.Flush(); err != nil {
		log.Error().Err(err).Msg("Flush failed")
		return err
	}
	return nil
}

// runReadPhase concurrently reads keys from PebbleDB using iterator
func runReadPhase(db *pebble.DB, cfg Config, keys iter.Seq[[]byte]) error {
	log.Info().Int("workers", cfg.Concurrency).Msg("Beginning read loop")

	channelBufferSize := cfg.Concurrency * 2

	jobs := make(chan []byte, channelBufferSize)
	readTimeHistory := make(chan time.Duration, channelBufferSize)
	var wg sync.WaitGroup
	var totalReads, notFound, failed, successful uint64

	// Feed keys to workers
	go func() {
		for key := range keys {
			jobs <- key
		}
		close(jobs)
	}()

	time.Sleep(time.Second) // ensure channel is ready before workers start

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// if there is no key to read, just return
			if len(jobs) == 0 {
				return
			}

			for key := range jobs {
				readStart := time.Now()
				_, closer, err := db.Get(key)
				readTimeHistory <- time.Since(readStart)

				atomic.AddUint64(&totalReads, 1)

				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						atomic.AddUint64(&notFound, 1)
					} else {
						atomic.AddUint64(&failed, 1)
					}
					continue
				}
				if closer != nil {
					closer.Close()
				}
				atomic.AddUint64(&successful, 1)
			}
		}(w)
	}

	// Summarize read times
	var totalReadTime time.Duration
	go func() {
		for readTime := range readTimeHistory {
			totalReadTime += readTime
		}
	}()

	// print progress every second while workers are running
	chDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-chDone:
				return
			case <-ticker.C:
				log.Info().Uint64("total_reads", atomic.LoadUint64(&totalReads)).Msg("Reads in progress")
			}
		}
	}()

	wg.Wait()
	close(readTimeHistory)
	chDone <- struct{}{}

	elapsed := totalReadTime.Seconds()
	read_ops_per_sec := float64(0)
	if elapsed > 0 {
		read_ops_per_sec = float64(atomic.LoadUint64(&totalReads)) / elapsed
	}
	read_avg_latency_ms := float64(0)
	if atomic.LoadUint64(&totalReads) > 0 {
		read_avg_latency_ms = float64(totalReadTime.Microseconds()) / 1000.0 / float64(atomic.LoadUint64(&totalReads))
	}

	log.Info().
		Float64("read_ops_per_sec", read_ops_per_sec).
		Float64("read_avg_latency_ms", read_avg_latency_ms).
		Uint64("not_found", atomic.LoadUint64(&notFound)).
		Uint64("failed_reads", atomic.LoadUint64(&failed)).
		Uint64("successful_reads", atomic.LoadUint64(&successful)).
		Uint64("total_reads", atomic.LoadUint64(&totalReads)).
		Dur("read_total_elapsed", totalReadTime).
		Msg("Read benchmark complete")

	return nil
}

// generateValue returns a random byte slice of specified size
func generateValue(rng *rand.Rand, size int) []byte {
	buf := make([]byte, size)
	rng.Read(buf)
	return buf
}
