package cmd

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/tclemos/pebble-bench/benchmark"
)

var (
	keyCount       int
	readRatio      float64
	valueSize      int
	seed           int64
	dbPath         string
	benchmarkID    string
	writeEnabled   bool
	keysFile       string
	concurrency    int
	logFormat      string
	blockCacheSize int64 // in bytes, negative means disabled (nil)
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the PebbleDB benchmark",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := benchmark.Config{
			KeyCount:     keyCount,
			ReadRatio:    readRatio,
			ValueSize:    valueSize,
			Seed:         seed,
			DBPath:       dbPath,
			BenchmarkID:  benchmarkID,
			WriteEnabled: writeEnabled,
			KeysFile:     keysFile,
			Concurrency:  concurrency,
			LogFormat:    logFormat,
		}
		if err := benchmark.RunBenchmark(cfg); err != nil {
			log.Fatalf("Benchmark failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().IntVar(&keyCount, "key-count", 1000000, "Number of keys to use in the benchmark")
	runCmd.Flags().Float64Var(&readRatio, "read-ratio", 0.7, "Read ratio (e.g., 0.7 = 70% reads)")
	runCmd.Flags().IntVar(&valueSize, "value-size", 256, "Size of each value in bytes")
	runCmd.Flags().Int64Var(&seed, "seed", 42, "Seed for deterministic key/value generation")
	runCmd.Flags().StringVar(&dbPath, "db-path", "pebble-test-db", "Path to store PebbleDB files")
	runCmd.Flags().StringVar(&benchmarkID, "benchmark-id", "default", "Optional benchmark ID tag for logs")
	runCmd.Flags().BoolVar(&writeEnabled, "write", false, "If true, write keys to DB before benchmarking")
	runCmd.Flags().StringVar(&keysFile, "keys-file", "", "Path to binary file containing keys to read")
	runCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Number of concurrent workers for reads/writes")
	runCmd.Flags().StringVar(&logFormat, "log-format", "console", "Log format: 'json' or 'console'")
	runCmd.Flags().Int64Var(&blockCacheSize, "block-cache-size", 8<<20, "Block cache size in bytes (negative for disabled, default 8MB)")
}
