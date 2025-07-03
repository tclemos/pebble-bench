package benchmark

import (
	"bytes"
	"fmt"
	"iter"
	"math/rand"
)

// TransactionExecutionWorkload implements realistic transaction execution patterns
type TransactionExecutionWorkload struct {
	config      WorkloadConfig
	txModel     *TransactionModel
	txGenerator *TransactionGenerator

	// Transaction mix configuration
	transactionMix TransactionMixConfig

	// State tracking
	blockNumber   uint64
	txInBlock     int
	maxTxPerBlock int
	gasInBlock    uint64
	gasTarget     uint64

	// Hot account tracking for spatial locality
	hotAccounts [][]byte
}

// NewTransactionExecutionWorkload creates the new workload type
func NewTransactionExecutionWorkload(cfg WorkloadConfig) *TransactionExecutionWorkload {
	workload := &TransactionExecutionWorkload{
		config:        cfg,
		maxTxPerBlock: cfg.TxPerBlock,
		gasTarget:     cfg.GasTargetPerBlock,
	}

	// Configure model based on network type and user overrides
	modelConfig := workload.buildModelConfig(cfg)
	workload.txModel = NewTransactionModel(modelConfig, cfg.Seed)

	// Configure transaction mix
	workload.transactionMix = workload.buildTransactionMix(cfg)
	workload.txGenerator = NewTransactionGenerator(workload.txModel, workload.transactionMix, cfg.Seed+1)

	// Initialize hot accounts for spatial locality
	workload.initHotAccounts(cfg.Seed + 2)

	return workload
}

// buildModelConfig creates model configuration with smart defaults and user overrides
func (w *TransactionExecutionWorkload) buildModelConfig(cfg WorkloadConfig) TransactionModelConfig {
	// Start with network defaults
	var modelConfig TransactionModelConfig
	switch cfg.NetworkType {
	case "ethereum":
		modelConfig = EthereumMainnetConfig
	case "polygon":
		modelConfig = PolygonPosConfig
	case "testnet":
		modelConfig = TestnetConfig
	default:
		modelConfig = EthereumMainnetConfig // Default to Ethereum
	}

	// Apply user overrides (only if not -1, which means "use default")
	if cfg.TxHotAccountProb >= 0 {
		modelConfig.HotAccountProbability = cfg.TxHotAccountProb
	}
	if cfg.TxStorageLocality >= 0 {
		modelConfig.StorageLocalityFactor = cfg.TxStorageLocality
	}
	if cfg.TxCacheHitRatio >= 0 {
		modelConfig.CacheHitRatio = cfg.TxCacheHitRatio
	}
	if cfg.TxAccountTrieDepth >= 0 {
		modelConfig.AccountTrieDepth = cfg.TxAccountTrieDepth
	}
	if cfg.TxStorageTrieDepth >= 0 {
		modelConfig.StorageTrieDepth = cfg.TxStorageTrieDepth
	}
	if cfg.TxReadWriteRatio >= 0 {
		modelConfig.ReadWriteRatio = cfg.TxReadWriteRatio
	}
	if cfg.TxContractRatio >= 0 {
		modelConfig.ContractRatio = cfg.TxContractRatio
	}

	return modelConfig
}

// buildTransactionMix creates transaction mix configuration
func (w *TransactionExecutionWorkload) buildTransactionMix(cfg WorkloadConfig) TransactionMixConfig {
	// Start with predefined mix
	var mixConfig TransactionMixConfig
	switch cfg.TransactionMix {
	case "ethereum":
		mixConfig = EthereumMainnetMix
	case "polygon":
		mixConfig = PolygonPoSMix
	case "defi-heavy":
		mixConfig = DeFiHeavyMix
	case "transfer-heavy":
		mixConfig = TransferHeavyMix
	case "balanced":
		fallthrough
	default:
		mixConfig = BalancedTransactionMix
	}

	// Apply user overrides for transaction ratios
	if cfg.TxSimpleTransferRatio >= 0 {
		mixConfig.SimpleTransferRatio = cfg.TxSimpleTransferRatio
	}
	if cfg.TxERC20TransferRatio >= 0 {
		mixConfig.ERC20TransferRatio = cfg.TxERC20TransferRatio
	}
	if cfg.TxUniswapSwapRatio >= 0 {
		mixConfig.UniswapSwapRatio = cfg.TxUniswapSwapRatio
	}
	if cfg.TxComplexDeFiRatio >= 0 {
		mixConfig.ComplexDeFiRatio = cfg.TxComplexDeFiRatio
	}
	if cfg.TxContractDeployRatio >= 0 {
		mixConfig.ContractDeployRatio = cfg.TxContractDeployRatio
	}

	// Validate and normalize the mix
	if !ValidateTransactionMix(mixConfig) {
		// Fallback to balanced mix if invalid
		mixConfig = BalancedTransactionMix
	}

	return mixConfig
}

// initHotAccounts creates the frequently accessed accounts for spatial locality
func (w *TransactionExecutionWorkload) initHotAccounts(seed int64) {
	rng := rand.New(rand.NewSource(seed))
	hotCount := int(float64(w.config.AccountCount) * w.txModel.config.HotAccountProbability)
	if hotCount == 0 {
		hotCount = 10 // Minimum hot accounts
	}

	w.hotAccounts = make([][]byte, hotCount)
	for i := range w.hotAccounts {
		addr := make([]byte, 20)
		rng.Read(addr)
		w.hotAccounts[i] = addr
	}
}

// Name returns workload identifier
func (w *TransactionExecutionWorkload) Name() string {
	return "Transaction-Execution"
}

// GetDescription returns detailed workload description
func (w *TransactionExecutionWorkload) GetDescription() string {
	return fmt.Sprintf("Realistic blockchain transaction execution simulation (%s network, %s mix, %d tx/block, %.0f gas/block)",
		w.config.NetworkType, w.config.TransactionMix, w.maxTxPerBlock, float64(w.gasTarget))
}

// GenerateKeys produces database keys representing transaction execution operations
func (w *TransactionExecutionWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		keysGenerated := 0

		for keysGenerated < count {
			// Generate a transaction
			txChars := w.txGenerator.GenerateTransaction()

			// Calculate database operations for this transaction
			breakdown := w.txModel.CalculateDatabaseOperations(txChars)

			// Generate keys for each operation type
			keysGenerated += w.generateOperationKeys(yield, rng, txChars, breakdown, keysGenerated, count)

			if keysGenerated >= count {
				break
			}

			// Track block progression
			w.gasInBlock += txChars.GasUsed
			w.txInBlock++

			// Simulate end of block when gas target reached or max transactions reached
			if w.gasInBlock >= w.gasTarget || w.txInBlock >= w.maxTxPerBlock {
				// Generate block commit operations
				keysGenerated += w.generateBlockCommitKeys(yield, rng, keysGenerated, count)
				
				// Reset for next block
				w.txInBlock = 0
				w.gasInBlock = 0
				w.blockNumber++
				
				if keysGenerated >= count {
					break
				}
			}
		}
	}
}

// generateOperationKeys generates keys for all operations in a transaction
func (w *TransactionExecutionWorkload) generateOperationKeys(yield func([]byte) bool, rng *rand.Rand, 
	txChars TransactionCharacteristics, breakdown DatabaseOperationBreakdown, keysGenerated, maxKeys int) int {
	
	generated := 0

	// Generate account operation keys
	for i := 0; i < breakdown.AccountOperations && keysGenerated+generated < maxKeys; i++ {
		key := w.generateAccountOperationKey(rng, txChars)
		if !yield(key) {
			return generated
		}
		generated++
	}

	// Generate storage operation keys
	for i := 0; i < breakdown.StorageOperations && keysGenerated+generated < maxKeys; i++ {
		key := w.generateStorageOperationKey(rng, txChars)
		if !yield(key) {
			return generated
		}
		generated++
	}

	// Generate trie operation keys
	for i := 0; i < breakdown.TrieOperations && keysGenerated+generated < maxKeys; i++ {
		key := w.generateTrieOperationKey(rng, txChars)
		if !yield(key) {
			return generated
		}
		generated++
	}

	// Generate persistence operation keys
	for i := 0; i < breakdown.PersistenceOperations && keysGenerated+generated < maxKeys; i++ {
		key := w.generatePersistenceOperationKey(rng, txChars)
		if !yield(key) {
			return generated
		}
		generated++
	}

	return generated
}

// generateBlockCommitKeys generates keys for block commit operations
func (w *TransactionExecutionWorkload) generateBlockCommitKeys(yield func([]byte) bool, rng *rand.Rand, keysGenerated, maxKeys int) int {
	generated := 0
	
	// Block commit involves several operations
	blockCommitOps := 5 + rng.Intn(10) // 5-15 operations per block commit
	
	for i := 0; i < blockCommitOps && keysGenerated+generated < maxKeys; i++ {
		key := w.generateBlockCommitKey(rng)
		if !yield(key) {
			return generated
		}
		generated++
	}
	
	return generated
}

// Helper methods for generating different operation types

func (w *TransactionExecutionWorkload) generateAccountOperationKey(rng *rand.Rand, tx TransactionCharacteristics) []byte {
	// Use hot accounts with high probability for spatial locality
	var accountAddr []byte
	if rng.Float64() < w.txModel.config.HotAccountProbability && len(w.hotAccounts) > 0 {
		accountAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
	} else {
		accountAddr = make([]byte, 20)
		rng.Read(accountAddr)
	}
	
	return append([]byte("account:"), accountAddr...)
}

func (w *TransactionExecutionWorkload) generateStorageOperationKey(rng *rand.Rand, tx TransactionCharacteristics) []byte {
	// Generate realistic storage key with contract address + storage slot
	var contractAddr []byte
	if rng.Float64() < w.txModel.config.HotAccountProbability && len(w.hotAccounts) > 0 {
		contractAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
	} else {
		contractAddr = make([]byte, 20)
		rng.Read(contractAddr)
	}
	
	// Generate storage slot with locality (related slots accessed together)
	storageSlot := make([]byte, 32)
	if rng.Float64() < w.txModel.config.StorageLocalityFactor {
		// Use locality: similar storage slots
		baseSlot := rng.Uint32()
		for i := 0; i < 4; i++ {
			storageSlot[28+i] = byte(baseSlot >> (8 * i))
		}
		// Add small offset for locality
		offset := rng.Intn(16)
		for i := 0; i < 4; i++ {
			if storageSlot[28+i]+byte(offset) > storageSlot[28+i] { // No overflow
				storageSlot[28+i] += byte(offset)
				break
			}
		}
	} else {
		rng.Read(storageSlot)
	}

	key := append([]byte("storage:"), contractAddr...)
	return append(key, storageSlot...)
}

func (w *TransactionExecutionWorkload) generateTrieOperationKey(rng *rand.Rand, tx TransactionCharacteristics) []byte {
	// Generate trie node key representing path from root to leaf
	maxDepth := w.txModel.config.AccountTrieDepth
	if tx.StorageOpsPerAccount > 0 {
		maxDepth = maxInt(maxDepth, w.txModel.config.StorageTrieDepth)
	}
	
	depth := rng.Intn(maxDepth) + 1
	nodeKey := make([]byte, depth)
	rng.Read(nodeKey)
	
	return append([]byte("trie:"), nodeKey...)
}

func (w *TransactionExecutionWorkload) generatePersistenceOperationKey(rng *rand.Rand, tx TransactionCharacteristics) []byte {
	// Generate WAL/commit key representing transaction persistence
	txHash := make([]byte, 32)
	rng.Read(txHash)
	
	return append([]byte("wal:"), txHash...)
}

func (w *TransactionExecutionWorkload) generateBlockCommitKey(rng *rand.Rand) []byte {
	// Generate block commit key
	blockHash := make([]byte, 32)
	rng.Read(blockHash)
	
	return append([]byte("block:"), blockHash...)
}

// GenerateValue creates realistic values based on operation type
func (w *TransactionExecutionWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) < 8 {
		// Default value
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}

	prefix := string(key[:minInt(8, len(key))])

	switch {
	case prefix == "account:":
		// Account data: nonce + balance + storage root + code hash
		return w.generateAccountValue(rng)
	case prefix == "storage:":
		// Storage slot value: 32 bytes
		return w.generateStorageValue(rng)
	case prefix == "trie:___":
		// Trie node: variable size RLP-encoded data
		return w.generateTrieNodeValue(rng)
	case prefix == "wal:____":
		// WAL entry: transaction data
		return w.generateWALValue(rng)
	case prefix == "block:__":
		// Block data: block header and metadata
		return w.generateBlockValue(rng)
	default:
		// Default
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

// Helper methods for generating realistic values

func (w *TransactionExecutionWorkload) generateAccountValue(rng *rand.Rand) []byte {
	// Simulate account state: nonce(8) + balance(32) + storage_root(32) + code_hash(32) = 104 bytes
	value := make([]byte, 104)
	rng.Read(value)
	return value
}

func (w *TransactionExecutionWorkload) generateStorageValue(rng *rand.Rand) []byte {
	// Storage values are always 32 bytes in Ethereum
	value := make([]byte, 32)
	rng.Read(value)
	return value
}

func (w *TransactionExecutionWorkload) generateTrieNodeValue(rng *rand.Rand) []byte {
	// Trie nodes: 64-512 bytes typically, RLP encoded
	size := rng.Intn(450) + 64
	value := make([]byte, size)
	rng.Read(value)
	return value
}

func (w *TransactionExecutionWorkload) generateWALValue(rng *rand.Rand) []byte {
	// WAL entries: variable transaction size, includes transaction data + metadata
	size := rng.Intn(2000) + 100 // 100-2100 bytes
	value := make([]byte, size)
	rng.Read(value)
	return value
}

func (w *TransactionExecutionWorkload) generateBlockValue(rng *rand.Rand) []byte {
	// Block data: block header + transaction list + metadata
	size := rng.Intn(5000) + 500 // 500-5500 bytes
	value := make([]byte, size)
	rng.Read(value)
	return value
}

// ShouldRead determines read vs write based on operation type and realistic ratios
func (w *TransactionExecutionWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	if len(key) < 8 {
		return rng.Float64() < w.config.ReadRatio
	}

	prefix := string(key[:minInt(8, len(key))])

	switch {
	case prefix == "account:":
		// Account operations: mostly reads for balance/nonce checks
		readProbability := w.txModel.config.ReadWriteRatio / (w.txModel.config.ReadWriteRatio + 1.0)
		return rng.Float64() < readProbability
	case prefix == "storage:":
		// Storage operations: based on configured read/write ratio
		readProbability := w.txModel.config.ReadWriteRatio / (w.txModel.config.ReadWriteRatio + 1.0)
		return rng.Float64() < readProbability
	case prefix == "trie:___":
		// Trie operations: many reads for traversal, some writes for updates
		return rng.Float64() < 0.7
	case prefix == "wal:____":
		// WAL operations: mostly writes for transaction logging
		return rng.Float64() < 0.1
	case prefix == "block:__":
		// Block operations: mostly writes for block commits
		return rng.Float64() < 0.2
	default:
		return rng.Float64() < w.config.ReadRatio
	}
}

// SupportsRangeQueries indicates range query support
func (w *TransactionExecutionWorkload) SupportsRangeQueries() bool {
	return true
}

// GenerateRangeQuery creates realistic range queries for blockchain operations
func (w *TransactionExecutionWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	queryTypes := []string{"account_range", "storage_range", "trie_range", "wal_range", "block_range"}
	queryType := queryTypes[rng.Intn(len(queryTypes))]

	limit = rng.Intn(100) + 10 // 10-100 items for most queries

	switch queryType {
	case "account_range":
		// Range over accounts (e.g., for state sync)
		start = append([]byte("account:"), make([]byte, 20)...)
		end = append([]byte("account:"), bytes.Repeat([]byte{0xFF}, 20)...)

	case "storage_range":
		// Range over contract storage (e.g., contract state dump)
		var contractAddr []byte
		if len(w.hotAccounts) > 0 {
			contractAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
		} else {
			contractAddr = make([]byte, 20)
			rng.Read(contractAddr)
		}
		start = append([]byte("storage:"), contractAddr...)
		start = append(start, make([]byte, 32)...)
		end = append([]byte("storage:"), contractAddr...)
		end = append(end, bytes.Repeat([]byte{0xFF}, 32)...)

	case "trie_range":
		// Range over trie nodes at specific depth
		depth := rng.Intn(8) + 1
		prefix := make([]byte, depth)
		rng.Read(prefix)
		start = append([]byte("trie:"), prefix...)
		end = append([]byte("trie:"), prefix...)
		if len(end) > 5 {
			end[len(end)-1] = 0xFF
		}

	case "wal_range":
		// Range over WAL entries for a transaction batch
		start = append([]byte("wal:"), make([]byte, 32)...)
		end = append([]byte("wal:"), make([]byte, 32)...)
		// Set a reasonable range for recent transactions
		for i := 16; i < 32; i++ {
			end[5+i] = 0xFF
		}

	case "block_range":
		// Range over recent blocks
		limit = rng.Intn(50) + 5 // 5-50 blocks
		blockStart := w.blockNumber - uint64(rng.Intn(100)) // Recent blocks
		start = append([]byte("block:"), uint64ToBytes(blockStart)...)
		end = append([]byte("block:"), uint64ToBytes(blockStart+uint64(limit))...)
	}

	return start, end, limit
}

// Utility functions

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}


func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(n >> (8 * (7 - i)))
	}
	return b
}