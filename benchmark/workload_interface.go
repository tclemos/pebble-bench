package benchmark

import (
	"iter"
	"math/rand"
)

// Workload defines the interface for different benchmark workload types
type Workload interface {
	// Name returns the human-readable name of this workload
	Name() string
	
	// GenerateKeys produces a sequence of keys for this workload
	GenerateKeys(seed int64, count int) iter.Seq[[]byte]
	
	// GenerateValue creates a value for the given key
	GenerateValue(rng *rand.Rand, key []byte) []byte
	
	// ShouldRead determines if a given key should be read (for read/write mix)
	ShouldRead(key []byte, rng *rand.Rand) bool
	
	// SupportsRangeQueries indicates if this workload supports range queries
	SupportsRangeQueries() bool
	
	// GenerateRangeQuery creates a range query if supported
	GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int)
	
	// GetDescription returns a detailed description of the workload
	GetDescription() string
}

// WorkloadType represents available workload types
type WorkloadType string

const (
	WorkloadGeneric           WorkloadType = "generic"
	WorkloadPoSBlocks         WorkloadType = "pos-blocks"
	WorkloadPoSAccounts       WorkloadType = "pos-accounts"
	WorkloadPoSState          WorkloadType = "pos-state"
	WorkloadPoSMixed          WorkloadType = "pos-mixed"
	WorkloadPoSAccountsReal   WorkloadType = "pos-accounts-realistic"
	WorkloadPoSStateReal      WorkloadType = "pos-state-realistic"
	WorkloadTransactionExecution WorkloadType = "transaction-execution"
)

// WorkloadConfig contains configuration specific to workloads
type WorkloadConfig struct {
	Type            WorkloadType
	ValueSize       int     // Base value size in bytes
	ReadRatio       float64 // Ratio of reads vs writes
	Seed            int64   // RNG seed for deterministic behavior
	
	// PoS-specific configuration
	RecentBlockBias  float64 // Probability of accessing recent blocks (0.0-1.0)
	HotAccountRatio  float64 // Ratio of "hot" accounts that get most access
	StateLocality    float64 // Probability of accessing related state
	BlockRange       int     // Range of block numbers to simulate
	AccountCount     int     // Number of unique accounts to simulate
	StorageSlotRatio float64 // Average storage slots per account
	
	// Transaction execution workload configuration
	NetworkType              string  // Network type: ethereum, polygon, custom
	TransactionMix           string  // Transaction mix: balanced, defi-heavy, transfer-heavy
	TxHotAccountProb         float64 // Hot account probability for transaction workload
	TxStorageLocality        float64 // Storage locality factor for transaction workload
	TxCacheHitRatio          float64 // Cache hit ratio for transaction workload
	TxAccountTrieDepth       int     // Account trie depth for transaction workload
	TxStorageTrieDepth       int     // Storage trie depth for transaction workload
	TxReadWriteRatio         float64 // Read/write ratio for transaction workload
	TxContractRatio          float64 // Contract ratio for transaction workload
	TxPerBlock               int     // Transactions per block
	GasTargetPerBlock        uint64  // Target gas per block
	TxSimpleTransferRatio    float64 // Simple transfer ratio in transaction mix
	TxERC20TransferRatio     float64 // ERC-20 transfer ratio in transaction mix
	TxUniswapSwapRatio       float64 // Uniswap swap ratio in transaction mix
	TxComplexDeFiRatio       float64 // Complex DeFi ratio in transaction mix
	TxContractDeployRatio    float64 // Contract deployment ratio in transaction mix
}

// CreateWorkload creates a workload instance based on the type
func CreateWorkload(cfg WorkloadConfig) Workload {
	switch cfg.Type {
	case WorkloadPoSBlocks:
		return NewPoSBlockWorkload(cfg)
	case WorkloadPoSAccounts:
		return NewPoSAccountWorkload(cfg)
	case WorkloadPoSState:
		return NewPoSStateWorkload(cfg)
	case WorkloadPoSMixed:
		return NewPoSMixedWorkload(cfg)
	case WorkloadPoSAccountsReal:
		return NewRealisticPoSAccountWorkload(cfg)
	case WorkloadPoSStateReal:
		return NewRealisticPoSStateWorkload(cfg)
	case WorkloadTransactionExecution:
		return NewTransactionExecutionWorkload(cfg)
	case WorkloadGeneric:
		fallthrough
	default:
		return NewGenericWorkload(cfg)
	}
}