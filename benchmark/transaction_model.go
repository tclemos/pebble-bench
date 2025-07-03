package benchmark

import (
	"math/rand"
)

// TransactionModelConfig holds all parameters for the mathematical model
type TransactionModelConfig struct {
	// Access pattern parameters
	HotAccountProbability float64 `json:"hot_account_probability"` // H parameter
	StorageLocalityFactor float64 `json:"storage_locality_factor"` // L parameter
	CacheHitRatio         float64 `json:"cache_hit_ratio"`         // C parameter

	// System constants
	AccountTrieDepth int     `json:"account_trie_depth"` // T_depth
	StorageTrieDepth int     `json:"storage_trie_depth"` // S_depth
	ReadWriteRatio   float64 `json:"read_write_ratio"`   // R_ratio
	ContractRatio    float64 `json:"contract_ratio"`     // Contract percentage

	// Operation coefficients
	AccountBaseOps    int     `json:"account_base_ops"`    // Base account operations
	CodeAccessOps     int     `json:"code_access_ops"`     // Contract code access
	UpdateProbability float64 `json:"update_probability"`  // Trie update probability
	CommitRatio       float64 `json:"commit_ratio"`        // Persistence overhead
}

// Default configurations for different networks
var (
	EthereumMainnetConfig = TransactionModelConfig{
		HotAccountProbability: 0.35,
		StorageLocalityFactor: 0.30,
		CacheHitRatio:         0.80,
		AccountTrieDepth:      8,
		StorageTrieDepth:      6,
		ReadWriteRatio:        3.0,
		ContractRatio:         0.4,
		AccountBaseOps:        4,
		CodeAccessOps:         3,
		UpdateProbability:     0.7,
		CommitRatio:           0.1,
	}

	PolygonPosConfig = TransactionModelConfig{
		HotAccountProbability: 0.25,
		StorageLocalityFactor: 0.35,
		CacheHitRatio:         0.85,
		AccountTrieDepth:      7,
		StorageTrieDepth:      6,
		ReadWriteRatio:        2.5,
		ContractRatio:         0.5,
		AccountBaseOps:        4,
		CodeAccessOps:         3,
		UpdateProbability:     0.7,
		CommitRatio:           0.1,
	}

	TestnetConfig = TransactionModelConfig{
		HotAccountProbability: 0.15,
		StorageLocalityFactor: 0.20,
		CacheHitRatio:         0.90,
		AccountTrieDepth:      5,
		StorageTrieDepth:      5,
		ReadWriteRatio:        2.0,
		ContractRatio:         0.3,
		AccountBaseOps:        4,
		CodeAccessOps:         3,
		UpdateProbability:     0.7,
		CommitRatio:           0.1,
	}
)

// TransactionCharacteristics represents the input parameters for the model
type TransactionCharacteristics struct {
	GasUsed              uint64  `json:"gas_used"`               // G parameter
	AccountsTouched      int     `json:"accounts_touched"`       // A parameter
	StorageOpsPerAccount float64 `json:"storage_ops_per_account"` // S parameter
	CallDepth            int     `json:"call_depth"`             // D parameter
	EventsEmitted        int     `json:"events_emitted"`         // E parameter
	TransactionType      string  `json:"transaction_type"`       // For categorization
}

// DatabaseOperationBreakdown represents the output of the mathematical model
type DatabaseOperationBreakdown struct {
	AccountOperations     int `json:"account_operations"`
	StorageOperations     int `json:"storage_operations"`
	TrieOperations        int `json:"trie_operations"`
	PersistenceOperations int `json:"persistence_operations"`
	TotalOperations       int `json:"total_operations"`

	// Additional metrics for analysis
	CacheEffectiveness      float64 `json:"cache_effectiveness"`
	TrieAmplificationFactor float64 `json:"trie_amplification_factor"`
}

// TransactionModel implements the mathematical model
type TransactionModel struct {
	config TransactionModelConfig
	rng    *rand.Rand
}

// NewTransactionModel creates a new transaction model with given configuration
func NewTransactionModel(config TransactionModelConfig, seed int64) *TransactionModel {
	return &TransactionModel{
		config: config,
		rng:    rand.New(rand.NewSource(seed)),
	}
}

// CalculateDatabaseOperations applies the mathematical formula
func (tm *TransactionModel) CalculateDatabaseOperations(chars TransactionCharacteristics) DatabaseOperationBreakdown {
	// Account operations calculation
	accountOps := tm.calculateAccountOperations(chars)

	// Storage operations calculation
	storageOps := tm.calculateStorageOperations(chars)

	// Trie operations calculation
	trieOps := tm.calculateTrieOperations(chars)

	// Persistence operations calculation
	persistenceOps := tm.calculatePersistenceOperations(accountOps + storageOps + trieOps)

	total := accountOps + storageOps + trieOps + persistenceOps

	return DatabaseOperationBreakdown{
		AccountOperations:       accountOps,
		StorageOperations:       storageOps,
		TrieOperations:          trieOps,
		PersistenceOperations:   persistenceOps,
		TotalOperations:         total,
		CacheEffectiveness:      tm.calculateCacheEffectiveness(chars),
		TrieAmplificationFactor: float64(trieOps) / float64(maxInt(accountOps+storageOps, 1)),
	}
}

// calculateAccountOperations implements the account operations formula
func (tm *TransactionModel) calculateAccountOperations(chars TransactionCharacteristics) int {
	// Basic account operations: A × AccountBaseOps × (1 - H × C)
	basicOps := float64(chars.AccountsTouched) * float64(tm.config.AccountBaseOps) *
		(1.0 - tm.config.HotAccountProbability*tm.config.CacheHitRatio)

	// Contract code operations: A × ContractRatio × CodeAccessOps × (1 - H × C)
	contractOps := float64(chars.AccountsTouched) * tm.config.ContractRatio *
		float64(tm.config.CodeAccessOps) *
		(1.0 - tm.config.HotAccountProbability*tm.config.CacheHitRatio)

	return int(basicOps + contractOps)
}

// calculateStorageOperations implements the storage operations formula
func (tm *TransactionModel) calculateStorageOperations(chars TransactionCharacteristics) int {
	if chars.StorageOpsPerAccount == 0 {
		return 0
	}

	// A × S × (R_ratio + 1) × (1 - L × C)
	storageOps := float64(chars.AccountsTouched) * chars.StorageOpsPerAccount *
		(tm.config.ReadWriteRatio + 1.0) *
		(1.0 - tm.config.StorageLocalityFactor*tm.config.CacheHitRatio)

	return int(storageOps)
}

// calculateTrieOperations implements the trie operations formula
func (tm *TransactionModel) calculateTrieOperations(chars TransactionCharacteristics) int {
	callDepthFactor := 1.0 + (float64(chars.CallDepth) * 0.1)

	// Account trie operations: A × T_depth × 2 × (1 + UpdateProbability) × CallDepthFactor
	accountTrieOps := float64(chars.AccountsTouched) * float64(tm.config.AccountTrieDepth) *
		2.0 * (1.0 + tm.config.UpdateProbability) * callDepthFactor

	// Storage trie operations: A × S × S_depth × 2 × (1 + UpdateProbability) × CallDepthFactor
	storageTrieOps := float64(chars.AccountsTouched) * chars.StorageOpsPerAccount *
		float64(tm.config.StorageTrieDepth) * 2.0 *
		(1.0 + tm.config.UpdateProbability) * callDepthFactor

	return int(accountTrieOps + storageTrieOps)
}

// calculatePersistenceOperations implements the persistence operations formula
func (tm *TransactionModel) calculatePersistenceOperations(totalOpsSoFar int) int {
	// WAL operations: constant overhead per transaction
	walOps := 2

	// Commit operations: proportional to total operations
	commitOps := int(float64(totalOpsSoFar) * tm.config.CommitRatio)

	return walOps + commitOps
}

// calculateCacheEffectiveness provides cache analysis
func (tm *TransactionModel) calculateCacheEffectiveness(chars TransactionCharacteristics) float64 {
	// Estimate cache effectiveness based on hot account probability
	hotAccountAccess := tm.config.HotAccountProbability
	return hotAccountAccess * tm.config.CacheHitRatio
}

// GetConfig returns the current model configuration
func (tm *TransactionModel) GetConfig() TransactionModelConfig {
	return tm.config
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}