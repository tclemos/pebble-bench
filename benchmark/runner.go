package benchmark

import (
	"errors"
	"fmt"
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
	KeyCount     int     // total number of keys to generate
	ReadRatio    float64 // ratio of reads vs total ops
	ValueSize    int     // size of values in bytes
	Seed         int64   // RNG seed for deterministic behavior
	DBPath       string  // path to PebbleDB instance
	BenchmarkID  string  // optional label for this benchmark run
	WriteEnabled bool    // whether to write data to the DB
	KeysFile     string  // optional file with pre-existing keys
	Concurrency  int     // number of concurrent workers
	LogFormat    string
}

// RunBenchmark orchestrates the full benchmark lifecycle
func RunBenchmark(cfg Config) error {
	if strings.ToLower(cfg.LogFormat) == "json" {
		zerolog.TimeFieldFormat = time.RFC3339Nano
		log.Logger = log.Output(os.Stdout)
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"})
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
		Msg("Starting benchmark")

	opts := &pebble.Options{}
	if !cfg.WriteEnabled {
		opts.ReadOnly = true
	}

	db, err := pebble.Open(cfg.DBPath, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open PebbleDB")
	}
	defer db.Close()

	var keys [][]byte
	if cfg.WriteEnabled {
		log.Info().Msg("Generating keys for write mode")
		keys = GenerateKeys(cfg.Seed, cfg.KeyCount)
		if err := runWritePhase(db, cfg, keys); err != nil {
			return err
		}
	} else {
		if cfg.KeysFile == "" {
			return fmt.Errorf("read-only mode requires --keys-file to be set")
		}
		log.Info().Str("path", cfg.KeysFile).Msg("Loading keys from file")
		loaded, err := loadKeysFromFile(cfg.KeysFile)
		if err != nil {
			return fmt.Errorf("failed to load keys: %w", err)
		}
		keys = loaded
		cfg.KeyCount = len(keys)
	}

	if err := runReadPhase(db, cfg, keys); err != nil {
		return err
	}

	log.Info().Str("benchmark_id", cfg.BenchmarkID).Msg("Benchmark complete")
	return nil
}

// runWritePhase concurrently writes keys to PebbleDB
func runWritePhase(db *pebble.DB, cfg Config, keys [][]byte) error {
	log.Info().Int("workers", cfg.Concurrency).Msg("Beginning write loop")

	jobs := make(chan int, cfg.KeyCount)
	results := make(chan time.Duration, cfg.KeyCount)
	var wg sync.WaitGroup

	for i := range keys {
		jobs <- i
	}
	close(jobs)

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(workerID)))
			for i := range jobs {
				value := generateValue(rng, cfg.ValueSize)
				writeStart := time.Now()
				if err := db.Set(keys[i], value, pebble.NoSync); err != nil {
					log.Error().Err(err).Int("index", i).Msg("Write failed")
					continue
				}
				results <- time.Since(writeStart)
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalWriteTime time.Duration
	for d := range results {
		totalWriteTime += d
	}

	elapsed := totalWriteTime.Seconds()
	ops := float64(cfg.KeyCount) / elapsed
	avg := float64(totalWriteTime.Microseconds()) / 1000.0 / float64(cfg.KeyCount)

	log.Info().Dur("total_elapsed", totalWriteTime).
		Float64("ops_per_sec", ops).
		Float64("avg_latency_ms", avg).
		Msg("Write benchmark complete")

	if err := db.Flush(); err != nil {
		log.Error().Err(err).Msg("Flush failed")
		return err
	}
	return nil
}

// runReadPhase concurrently reads keys from PebbleDB and tracks performance
func runReadPhase(db *pebble.DB, cfg Config, keys [][]byte) error {
	totalReads := int(float64(cfg.KeyCount) * cfg.ReadRatio)
	log.Info().Int("total_reads", totalReads).Int("workers", cfg.Concurrency).Msg("Beginning read loop")

	jobs := make(chan int, totalReads)
	results := make(chan time.Duration, totalReads)
	var wg sync.WaitGroup
	var notFound, successful uint64

	rng := rand.New(rand.NewSource(cfg.Seed + 1))
	for i := 0; i < totalReads; i++ {
		jobs <- rng.Intn(cfg.KeyCount)
	}
	close(jobs)

	start := time.Now()
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range jobs {
				key := keys[i]
				singleStart := time.Now()
				_, closer, err := db.Get(key)
				dur := time.Since(singleStart)
				results <- dur

				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						atomic.AddUint64(&notFound, 1)
					} else {
						log.Error().Err(err).Int("index", i).Msg("Read failed")
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

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalReadTime time.Duration
	for i := 0; i < totalReads; i++ {
		totalReadTime += <-results
		if i > 0 && i%(totalReads/10) == 0 {
			log.Info().Int("progress", i).Msg("Reads completed")
		}
	}

	elapsed := time.Since(start).Seconds()
	ops := float64(totalReads) / elapsed
	avg := float64(totalReadTime.Microseconds()) / 1000.0 / float64(totalReads)

	log.Info().Float64("read_ops_per_sec", ops).
		Float64("read_avg_latency_ms", avg).
		Uint64("not_found", atomic.LoadUint64(&notFound)).
		Uint64("successful_reads", atomic.LoadUint64(&successful)).
		Dur("read_total_elapsed", time.Since(start)).
		Msg("Read benchmark complete")

	return nil
}

// generateValue returns a random byte slice of specified size
func generateValue(rng *rand.Rand, size int) []byte {
	buf := make([]byte, size)
	rng.Read(buf)
	return buf
}
