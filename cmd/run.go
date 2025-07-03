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
	
	// Database backend configuration
	databaseType   string
	qmdbLibraryPath string
	
	// MDBX-specific configuration
	mdbxMapSize     int64
	mdbxMaxDbs      int
	mdbxMaxReaders  int
	mdbxNoSync      bool
	mdbxNoMetaSync  bool
	mdbxWriteMap    bool
	mdbxNoReadahead bool
	
	// Workload configuration
	workloadType     string
	recentBlockBias  float64
	hotAccountRatio  float64
	stateLocality    float64
	blockRange       int
	accountCount     int
	storageSlotRatio float64
	
	// Transaction execution workload configuration
	networkType              string
	transactionMix           string
	txHotAccountProb         float64
	txStorageLocality        float64
	txCacheHitRatio          float64
	txAccountTrieDepth       int
	txStorageTrieDepth       int
	txReadWriteRatio         float64
	txContractRatio          float64
	txPerBlock               int
	gasTargetPerBlock        uint64
	txSimpleTransferRatio    float64
	txERC20TransferRatio     float64
	txUniswapSwapRatio       float64
	txComplexDeFiRatio       float64
	txContractDeployRatio    float64
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run database benchmark (Pebble, QMDB, or MDBX)",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := benchmark.Config{
			KeyCount:         keyCount,
			ReadRatio:        readRatio,
			ValueSize:        valueSize,
			Seed:             seed,
			DBPath:           dbPath,
			BenchmarkID:      benchmarkID,
			WriteEnabled:     writeEnabled,
			KeysFile:         keysFile,
			Concurrency:      concurrency,
			LogFormat:        logFormat,
			BlockCacheSize:   blockCacheSize,
			DatabaseType:     databaseType,
			QMDBLibraryPath:  qmdbLibraryPath,
			MDBXMapSize:      mdbxMapSize,
			MDBXMaxDbs:       mdbxMaxDbs,
			MDBXMaxReaders:   mdbxMaxReaders,
			MDBXNoSync:       mdbxNoSync,
			MDBXNoMetaSync:   mdbxNoMetaSync,
			MDBXWriteMap:     mdbxWriteMap,
			MDBXNoReadahead:  mdbxNoReadahead,
			WorkloadType:     workloadType,
			RecentBlockBias:  recentBlockBias,
			HotAccountRatio:  hotAccountRatio,
			StateLocality:    stateLocality,
			BlockRange:       blockRange,
			AccountCount:     accountCount,
			StorageSlotRatio: storageSlotRatio,
			// Transaction execution workload parameters
			NetworkType:              networkType,
			TransactionMix:           transactionMix,
			TxHotAccountProb:         txHotAccountProb,
			TxStorageLocality:        txStorageLocality,
			TxCacheHitRatio:          txCacheHitRatio,
			TxAccountTrieDepth:       txAccountTrieDepth,
			TxStorageTrieDepth:       txStorageTrieDepth,
			TxReadWriteRatio:         txReadWriteRatio,
			TxContractRatio:          txContractRatio,
			TxPerBlock:               txPerBlock,
			GasTargetPerBlock:        gasTargetPerBlock,
			TxSimpleTransferRatio:    txSimpleTransferRatio,
			TxERC20TransferRatio:     txERC20TransferRatio,
			TxUniswapSwapRatio:       txUniswapSwapRatio,
			TxComplexDeFiRatio:       txComplexDeFiRatio,
			TxContractDeployRatio:    txContractDeployRatio,
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
	runCmd.Flags().StringVar(&dbPath, "db-path", "dbs/pebble/pebble-test-db", "Path to store database files (use dbs/{engine}/name pattern)")
	runCmd.Flags().StringVar(&benchmarkID, "benchmark-id", "default", "Optional benchmark ID tag for logs")
	runCmd.Flags().BoolVar(&writeEnabled, "write", false, "If true, write keys to DB before benchmarking")
	runCmd.Flags().StringVar(&keysFile, "keys-file", "", "Path to binary file containing keys to read")
	runCmd.Flags().IntVar(&concurrency, "concurrency", 1, "Number of concurrent workers for reads/writes")
	runCmd.Flags().StringVar(&logFormat, "log-format", "console", "Log format: 'json' or 'console'")
	runCmd.Flags().Int64Var(&blockCacheSize, "block-cache-size", 8<<20, "Block cache size in bytes (negative for disabled, default 8MB)")
	
	// Database backend configuration flags
	runCmd.Flags().StringVar(&databaseType, "database", "pebble", "Database backend: 'pebble', 'qmdb', or 'mdbx'")
	runCmd.Flags().StringVar(&qmdbLibraryPath, "qmdb-library", "./lib/libqmdb.dylib", "Path to QMDB shared library")
	
	// MDBX-specific configuration flags
	runCmd.Flags().Int64Var(&mdbxMapSize, "mdbx-map-size", -1, "MDBX: Maximum map size in bytes (-1 for default)")
	runCmd.Flags().IntVar(&mdbxMaxDbs, "mdbx-max-dbs", 0, "MDBX: Maximum number of databases (0 for default: 2)")
	runCmd.Flags().IntVar(&mdbxMaxReaders, "mdbx-max-readers", 0, "MDBX: Maximum number of readers (0 for default: 128)")
	runCmd.Flags().BoolVar(&mdbxNoSync, "mdbx-no-sync", false, "MDBX: Don't fsync after commit (improves performance, reduces durability)")
	runCmd.Flags().BoolVar(&mdbxNoMetaSync, "mdbx-no-meta-sync", false, "MDBX: Don't fsync metapage after commit")
	runCmd.Flags().BoolVar(&mdbxWriteMap, "mdbx-write-map", false, "MDBX: Use writeable memory map")
	runCmd.Flags().BoolVar(&mdbxNoReadahead, "mdbx-no-readahead", false, "MDBX: Disable readahead")
	
	// Workload configuration flags
	runCmd.Flags().StringVar(&workloadType, "workload", "generic", "Workload type: generic, pos-blocks, pos-accounts, pos-state, pos-mixed, pos-accounts-realistic, pos-state-realistic, transaction-execution")
	runCmd.Flags().Float64Var(&recentBlockBias, "recent-block-bias", 0.8, "PoS: Probability of accessing recent blocks (0.0-1.0)")
	runCmd.Flags().Float64Var(&hotAccountRatio, "hot-account-ratio", 0.2, "PoS: Ratio of hot accounts that get most access (0.0-1.0)")
	runCmd.Flags().Float64Var(&stateLocality, "state-locality", 0.3, "PoS: Probability of accessing related state (0.0-1.0)")
	runCmd.Flags().IntVar(&blockRange, "block-range", 100000, "PoS: Range of block numbers to simulate")
	runCmd.Flags().IntVar(&accountCount, "account-count", 100000, "PoS: Number of unique accounts to simulate")
	runCmd.Flags().Float64Var(&storageSlotRatio, "storage-slot-ratio", 5.0, "PoS: Average storage slots per account")
	
	// Transaction execution workload flags
	runCmd.Flags().StringVar(&networkType, "network-type", "ethereum", "TX: Network type (ethereum, polygon, testnet, custom)")
	runCmd.Flags().StringVar(&transactionMix, "transaction-mix", "balanced", "TX: Transaction mix (balanced, ethereum, polygon, defi-heavy, transfer-heavy)")
	runCmd.Flags().Float64Var(&txHotAccountProb, "tx-hot-account-prob", -1, "TX: Hot account probability (0.0-1.0, -1 for network default)")
	runCmd.Flags().Float64Var(&txStorageLocality, "tx-storage-locality", -1, "TX: Storage locality factor (0.0-1.0, -1 for network default)")
	runCmd.Flags().Float64Var(&txCacheHitRatio, "tx-cache-hit-ratio", -1, "TX: Cache hit ratio (0.0-1.0, -1 for network default)")
	runCmd.Flags().IntVar(&txAccountTrieDepth, "tx-account-trie-depth", -1, "TX: Account trie depth (-1 for network default)")
	runCmd.Flags().IntVar(&txStorageTrieDepth, "tx-storage-trie-depth", -1, "TX: Storage trie depth (-1 for network default)")
	runCmd.Flags().Float64Var(&txReadWriteRatio, "tx-read-write-ratio", -1, "TX: Read/write ratio (-1 for network default)")
	runCmd.Flags().Float64Var(&txContractRatio, "tx-contract-ratio", -1, "TX: Contract ratio (0.0-1.0, -1 for network default)")
	runCmd.Flags().IntVar(&txPerBlock, "tx-per-block", 100, "TX: Transactions per block")
	runCmd.Flags().Uint64Var(&gasTargetPerBlock, "gas-target-per-block", 15000000, "TX: Target gas per block")
	runCmd.Flags().Float64Var(&txSimpleTransferRatio, "tx-simple-transfer-ratio", -1, "TX: Simple transfer ratio (0.0-1.0, -1 for mix default)")
	runCmd.Flags().Float64Var(&txERC20TransferRatio, "tx-erc20-transfer-ratio", -1, "TX: ERC-20 transfer ratio (0.0-1.0, -1 for mix default)")
	runCmd.Flags().Float64Var(&txUniswapSwapRatio, "tx-uniswap-swap-ratio", -1, "TX: Uniswap swap ratio (0.0-1.0, -1 for mix default)")
	runCmd.Flags().Float64Var(&txComplexDeFiRatio, "tx-complex-defi-ratio", -1, "TX: Complex DeFi ratio (0.0-1.0, -1 for mix default)")
	runCmd.Flags().Float64Var(&txContractDeployRatio, "tx-contract-deploy-ratio", -1, "TX: Contract deployment ratio (0.0-1.0, -1 for mix default)")
}
