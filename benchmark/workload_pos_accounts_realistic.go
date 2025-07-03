package benchmark

import (
	"fmt"
	"iter"
	"math/rand"
)

// RealisticPoSAccountWorkload simulates actual blockchain account operations
// with proper trie traversal and multiple database operations per logical operation
type RealisticPoSAccountWorkload struct {
	config         WorkloadConfig
	trieSimulation *TrieSimulation
	hotAccounts    [][]byte // Pre-generated hot accounts
	
	// Batch tracking for commit simulation
	pendingBatches []TrieBatch
	commitCounter  int
}

// NewRealisticPoSAccountWorkload creates a workload that properly simulates trie operations
func NewRealisticPoSAccountWorkload(cfg WorkloadConfig) *RealisticPoSAccountWorkload {
	if cfg.AccountCount == 0 {
		cfg.AccountCount = 10000 // Smaller default due to complexity
	}
	if cfg.HotAccountRatio == 0 {
		cfg.HotAccountRatio = 0.2
	}
	if cfg.StorageSlotRatio == 0 {
		cfg.StorageSlotRatio = 3.0 // Fewer slots due to higher per-slot cost
	}
	
	return &RealisticPoSAccountWorkload{
		config:         cfg,
		trieSimulation: NewTrieSimulation(),
		pendingBatches: make([]TrieBatch, 0),
	}
}

func (w *RealisticPoSAccountWorkload) Name() string {
	return "PoS-Accounts-Realistic"
}

func (w *RealisticPoSAccountWorkload) GetDescription() string {
	return fmt.Sprintf("Realistic PoS account simulation with trie operations (%d accounts, %.1f%% hot, %.1fx storage ratio)",
		w.config.AccountCount, w.config.HotAccountRatio*100, w.config.StorageSlotRatio)
}

// GenerateKeys produces database keys that represent the actual operations needed
func (w *RealisticPoSAccountWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		w.initHotAccounts(rng)
		
		keysGenerated := 0
		batchOperations := []DatabaseOperation{}
		
		// Operation mix that reflects real blockchain usage
		operationTypes := []string{"account_read", "account_update", "storage_read", "storage_update", "commit_flush"}
		operationWeights := []float64{0.4, 0.15, 0.3, 0.1, 0.05} // Reads dominate, commits are periodic
		
		for keysGenerated < count {
			operationType := selectWeightedChoice(rng, operationTypes, operationWeights)
			
			var batch TrieBatch
			
			switch operationType {
			case "account_read":
				address := w.selectAccount(rng)
				batch = w.trieSimulation.SimulateAccountRead(address)
				
			case "account_update":
				address := w.selectAccount(rng)
				accountData := w.generateAccountData(rng)
				batch = w.trieSimulation.SimulateAccountUpdate(address, accountData)
				
			case "storage_read":
				address := w.selectAccount(rng)
				storageKey := w.generateStorageKey(rng)
				// For reads, we simulate the traversal but without writes
				readBatch := w.trieSimulation.SimulateAccountRead(address)
				// Add storage-specific reads
				storageBatch := w.simulateStorageRead(address, storageKey)
				batch = TrieBatch{
					LogicalOperation: "storage_read",
					DatabaseOps:      append(readBatch.DatabaseOps, storageBatch.DatabaseOps...),
					AddressHash:      readBatch.AddressHash,
				}
				
			case "storage_update":
				address := w.selectAccount(rng)
				storageKey := w.generateStorageKey(rng)
				storageValue := w.generateStorageValue(rng)
				batch = w.trieSimulation.SimulateStorageUpdate(address, storageKey, storageValue)
				
			case "commit_flush":
				// Simulate commit operation that flushes multiple pending operations
				batch = w.simulateCommitFlush(rng)
			}
			
			// Add all database operations from this logical operation
			for _, op := range batch.DatabaseOps {
				batchOperations = append(batchOperations, op)
				
				// Each database operation becomes a key in our benchmark
				if !yield(op.Key) {
					return
				}
				keysGenerated++
				
				// Stop if we've generated enough keys
				if keysGenerated >= count {
					break
				}
			}
			
			// Track batch for commit simulation
			if operationType != "commit_flush" {
				w.pendingBatches = append(w.pendingBatches, batch)
			}
		}
	}
}

// initHotAccounts creates the frequently accessed accounts
func (w *RealisticPoSAccountWorkload) initHotAccounts(rng *rand.Rand) {
	hotCount := int(float64(w.config.AccountCount) * w.config.HotAccountRatio)
	w.hotAccounts = make([][]byte, hotCount)
	
	for i := range w.hotAccounts {
		addr := make([]byte, 20)
		rng.Read(addr)
		w.hotAccounts[i] = addr
	}
}

// selectAccount chooses an account with hot account bias
func (w *RealisticPoSAccountWorkload) selectAccount(rng *rand.Rand) []byte {
	if rng.Float64() < 0.8 && len(w.hotAccounts) > 0 {
		return w.hotAccounts[rng.Intn(len(w.hotAccounts))]
	}
	
	// Generate random account
	addr := make([]byte, 20)
	rng.Read(addr)
	return addr
}

// simulateStorageRead simulates reading a storage slot (separate from account read)
func (w *RealisticPoSAccountWorkload) simulateStorageRead(address, storageKey []byte) TrieBatch {
	// This is a simplified version - in reality would traverse storage trie
	ops := []DatabaseOperation{
		{
			Type:        "READ",
			Key:         w.trieSimulation.computeStorageValueKey(address, storageKey),
			Description: "Read storage slot value",
		},
	}
	
	return TrieBatch{
		LogicalOperation: "storage_read_simple",
		DatabaseOps:      ops,
		AddressHash:      address,
	}
}

// simulateCommitFlush simulates a commit operation that processes multiple pending changes
func (w *RealisticPoSAccountWorkload) simulateCommitFlush(rng *rand.Rand) TrieBatch {
	ops := []DatabaseOperation{}
	
	// In real blockchain, commit processes many operations at once
	// Simulate state root calculation and final writes
	ops = append(ops, DatabaseOperation{
		Type:        "READ",
		Key:         append([]byte("stateroot"), w.trieSimulation.stateRoot...),
		Description: "Read current state root for commit",
	})
	
	// Simulate writing multiple dirty nodes to disk
	numDirtyNodes := rng.Intn(50) + 10 // 10-60 dirty nodes per commit
	for i := 0; i < numDirtyNodes; i++ {
		nodeKey := make([]byte, 40)
		rng.Read(nodeKey)
		nodeValue := w.trieSimulation.generateUpdatedTrieNode(nodeKey, i%8)
		
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         append([]byte("commit_node"), nodeKey...),
			Value:       nodeValue,
			Description: fmt.Sprintf("Commit dirty node %d to disk", i),
		})
	}
	
	// Final state root update
	newStateRoot := w.trieSimulation.generateNewStateRoot()
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         append([]byte("stateroot"), newStateRoot...),
		Value:       newStateRoot,
		Description: "Update final state root",
	})
	
	// Clear pending batches
	w.pendingBatches = w.pendingBatches[:0]
	w.commitCounter++
	
	return TrieBatch{
		LogicalOperation: "commit_flush",
		DatabaseOps:      ops,
		AddressHash:      nil,
	}
}

// GenerateValue creates realistic values based on the operation type
func (w *RealisticPoSAccountWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) == 0 {
		return make([]byte, w.config.ValueSize)
	}
	
	// Determine value type based on key prefix
	if len(key) >= 8 {
		keyPrefix := string(key[:8])
		
		switch {
		case keyPrefix == "stateroot" || keyPrefix == "commit_n":
			// Trie nodes: Variable size, 64-512 bytes typical
			size := rng.Intn(450) + 64
			return w.generateTrieNodeValue(rng, size)
			
		case keyPrefix == "account":
			// Account data: ~100-200 bytes
			return w.generateAccountData(rng)
			
		case keyPrefix == "storage":
			// Storage values: 32 bytes typically
			return w.generateStorageValue(rng)
			
		case keyPrefix[:4] == "trie":
			// Intermediate trie nodes
			size := rng.Intn(200) + 32
			return w.generateTrieNodeValue(rng, size)
		}
	}
	
	// Default value
	value := make([]byte, w.config.ValueSize)
	rng.Read(value)
	return value
}

// generateAccountData creates realistic account state data
func (w *RealisticPoSAccountWorkload) generateAccountData(rng *rand.Rand) []byte {
	// Realistic account: nonce + balance + storage root + code hash
	data := make([]byte, 128) // Typical account size
	rng.Read(data)
	return data
}

// generateStorageKey creates a storage slot key
func (w *RealisticPoSAccountWorkload) generateStorageKey(rng *rand.Rand) []byte {
	key := make([]byte, 32)
	rng.Read(key)
	return key
}

// generateStorageValue creates a storage slot value
func (w *RealisticPoSAccountWorkload) generateStorageValue(rng *rand.Rand) []byte {
	value := make([]byte, 32)
	rng.Read(value)
	return value
}

// generateTrieNodeValue creates realistic trie node data
func (w *RealisticPoSAccountWorkload) generateTrieNodeValue(rng *rand.Rand, size int) []byte {
	value := make([]byte, size)
	rng.Read(value)
	return value
}

// ShouldRead determines read vs write based on realistic trie operation patterns
func (w *RealisticPoSAccountWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	if len(key) == 0 {
		return rng.Float64() < w.config.ReadRatio
	}
	
	// Analyze key to determine operation type and realistic read/write ratio
	
	switch {
	case len(key) >= 8 && string(key[:8]) == "stateroot":
		// State root: frequently read, occasionally written
		return rng.Float64() < 0.9
		
	case len(key) >= 7 && string(key[:7]) == "account":
		// Account data: mostly reads
		return rng.Float64() < 0.85
		
	case len(key) >= 7 && string(key[:7]) == "storage":
		// Storage: very read-heavy
		return rng.Float64() < 0.95
		
	case len(key) >= 4 && string(key[:4]) == "trie":
		// Trie nodes: read during traversal, written during updates
		return rng.Float64() < 0.7
		
	case len(key) >= 10 && string(key[:10]) == "commit_nod":
		// Commit operations: pure writes
		return false
		
	default:
		return rng.Float64() < w.config.ReadRatio
	}
}

// SupportsRangeQueries indicates range query support
func (w *RealisticPoSAccountWorkload) SupportsRangeQueries() bool {
	return true
}

// GenerateRangeQuery creates realistic range queries (e.g., iterating storage slots)
func (w *RealisticPoSAccountWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	queryTypes := []string{"storage_range", "account_range", "trie_range"}
	queryType := queryTypes[rng.Intn(len(queryTypes))]
	
	limit = rng.Intn(100) + 10 // 10-100 items (more realistic than 1000)
	
	switch queryType {
	case "storage_range":
		// Range over storage slots for a specific account
		address := w.selectAccount(rng)
		prefix := append([]byte("storage"), address...)
		
		start = make([]byte, len(prefix)+32)
		copy(start, prefix)
		// Zero storage key for start
		
		end = make([]byte, len(prefix)+32)
		copy(end, prefix)
		// Max storage key for end
		for i := len(prefix); i < len(end); i++ {
			end[i] = 0xFF
		}
		
	case "account_range":
		// Range over accounts
		start = append([]byte("account"), make([]byte, 32)...)
		end = append([]byte("account"), make([]byte, 32)...)
		for i := 7; i < len(end); i++ {
			end[i] = 0xFF
		}
		
	case "trie_range":
		// Range over trie nodes at a specific depth
		depth := rng.Intn(8) + 1
		prefix := append([]byte("trie"), make([]byte, depth)...)
		
		start = make([]byte, len(prefix))
		copy(start, prefix)
		
		end = make([]byte, len(prefix))
		copy(end, prefix)
		if len(end) > 4 {
			end[len(end)-1] = 0xFF
		}
	}
	
	return start, end, limit
}