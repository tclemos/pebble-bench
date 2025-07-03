package benchmark

import (
	"fmt"
	"iter"
	"math/rand"
)

// PoSMixedWorkload combines all PoS workload types for comprehensive testing
// This simulates a realistic blockchain environment with mixed access patterns
type PoSMixedWorkload struct {
	config         WorkloadConfig
	blockWorkload  *PoSBlockWorkload
	accountWorkload *PoSAccountWorkload
	stateWorkload  *PoSStateWorkload
}

// NewPoSMixedWorkload creates a mixed PoS workload combining all patterns
func NewPoSMixedWorkload(cfg WorkloadConfig) *PoSMixedWorkload {
	return &PoSMixedWorkload{
		config:          cfg,
		blockWorkload:   NewPoSBlockWorkload(cfg),
		accountWorkload: NewPoSAccountWorkload(cfg),
		stateWorkload:   NewPoSStateWorkload(cfg),
	}
}

func (w *PoSMixedWorkload) Name() string {
	return "PoS-Mixed"
}

func (w *PoSMixedWorkload) GetDescription() string {
	return fmt.Sprintf("Mixed PoS workload combining blocks, accounts, and state trie access patterns")
}

// GenerateKeys creates a realistic mix of all blockchain key types
func (w *PoSMixedWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		keysGenerated := 0
		
		// Distribution of workload types in a realistic blockchain environment
		workloadTypes := []string{"blocks", "accounts", "state"}
		// Accounts and state access dominate, blocks are accessed less frequently
		workloadWeights := []float64{0.2, 0.5, 0.3}
		
		// Create iterators for each workload type
		blockKeys := w.blockWorkload.GenerateKeys(seed, count)
		accountKeys := w.accountWorkload.GenerateKeys(seed+1, count)
		stateKeys := w.stateWorkload.GenerateKeys(seed+2, count)
		
		// Convert to slices for random access
		var blockKeyList, accountKeyList, stateKeyList [][]byte
		
		for key := range blockKeys {
			blockKeyList = append(blockKeyList, key)
			if len(blockKeyList) >= count/3 { // Limit to prevent memory issues
				break
			}
		}
		
		for key := range accountKeys {
			accountKeyList = append(accountKeyList, key)
			if len(accountKeyList) >= count/3 {
				break
			}
		}
		
		for key := range stateKeys {
			stateKeyList = append(stateKeyList, key)
			if len(stateKeyList) >= count/3 {
				break
			}
		}
		
		// Generate mixed keys based on weights
		for keysGenerated < count {
			workloadType := selectWeightedChoice(rng, workloadTypes, workloadWeights)
			
			var key []byte
			switch workloadType {
			case "blocks":
				if len(blockKeyList) > 0 {
					key = blockKeyList[rng.Intn(len(blockKeyList))]
				}
			case "accounts":
				if len(accountKeyList) > 0 {
					key = accountKeyList[rng.Intn(len(accountKeyList))]
				}
			case "state":
				if len(stateKeyList) > 0 {
					key = stateKeyList[rng.Intn(len(stateKeyList))]
				}
			}
			
			if key != nil {
				if !yield(key) {
					return
				}
				keysGenerated++
			}
		}
	}
}

func (w *PoSMixedWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) == 0 {
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
	
	// Route to appropriate workload based on key prefix
	prefix := string(key[0:1])
	
	switch prefix {
	case "h", "b", "r", "l": // Block-related keys
		return w.blockWorkload.GenerateValue(rng, key)
	case "a", "o": // Account-related keys
		return w.accountWorkload.GenerateValue(rng, key)
	case "A", "O", "s": // State trie keys
		return w.stateWorkload.GenerateValue(rng, key)
	default:
		// Fallback to random value
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

func (w *PoSMixedWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	if len(key) == 0 {
		return rng.Float64() < w.config.ReadRatio
	}
	
	// Route to appropriate workload based on key prefix
	prefix := string(key[0:1])
	
	switch prefix {
	case "h", "b", "r", "l":
		return w.blockWorkload.ShouldRead(key, rng)
	case "a", "o":
		return w.accountWorkload.ShouldRead(key, rng)
	case "A", "O", "s":
		return w.stateWorkload.ShouldRead(key, rng)
	default:
		return rng.Float64() < w.config.ReadRatio
	}
}

func (w *PoSMixedWorkload) SupportsRangeQueries() bool {
	return true
}

func (w *PoSMixedWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	// Randomly select which workload type to generate range query for
	workloadTypes := []string{"blocks", "accounts", "state"}
	workloadType := workloadTypes[rng.Intn(len(workloadTypes))]
	
	switch workloadType {
	case "blocks":
		return w.blockWorkload.GenerateRangeQuery(rng)
	case "accounts":
		return w.accountWorkload.GenerateRangeQuery(rng)
	case "state":
		return w.stateWorkload.GenerateRangeQuery(rng)
	default:
		return w.blockWorkload.GenerateRangeQuery(rng)
	}
}

// PoSStateWorkload simulates state trie and snapshot access patterns
type PoSStateWorkload struct {
	config WorkloadConfig
}

// NewPoSStateWorkload creates a new PoS state-focused workload
func NewPoSStateWorkload(cfg WorkloadConfig) *PoSStateWorkload {
	return &PoSStateWorkload{
		config: cfg,
	}
}

func (w *PoSStateWorkload) Name() string {
	return "PoS-State"
}

func (w *PoSStateWorkload) GetDescription() string {
	return "PoS state trie and snapshot access patterns"
}

func (w *PoSStateWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		keysGenerated := 0
		
		// State-related key types
		keyTypes := []string{"snapshot-account", "snapshot-storage", "trie-node"}
		keyWeights := []float64{0.3, 0.4, 0.3}
		
		for keysGenerated < count {
			keyType := selectWeightedChoice(rng, keyTypes, keyWeights)
			
			var key []byte
			switch keyType {
			case "snapshot-account":
				key = w.generateSnapshotAccountKey(rng)
			case "snapshot-storage":
				key = w.generateSnapshotStorageKey(rng)
			case "trie-node":
				key = w.generateTrieNodeKey(rng)
			}
			
			if !yield(key) {
				return
			}
			keysGenerated++
		}
	}
}

func (w *PoSStateWorkload) generateSnapshotAccountKey(rng *rand.Rand) []byte {
	prefix := []byte("s") // Snapshot prefix
	accountHash := make([]byte, 32)
	rng.Read(accountHash)
	return append(prefix, accountHash...)
}

func (w *PoSStateWorkload) generateSnapshotStorageKey(rng *rand.Rand) []byte {
	prefix := []byte("S") // Snapshot storage prefix
	accountHash := make([]byte, 32)
	rng.Read(accountHash)
	storageHash := make([]byte, 32)
	rng.Read(storageHash)
	
	key := append(prefix, accountHash...)
	key = append(key, storageHash...)
	return key
}

func (w *PoSStateWorkload) generateTrieNodeKey(rng *rand.Rand) []byte {
	// Generic trie node (could be any type)
	prefix := []byte("t")
	nodeHash := make([]byte, 32)
	rng.Read(nodeHash)
	return append(prefix, nodeHash...)
}

func (w *PoSStateWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	// State values are typically smaller and more structured
	if len(key) > 0 {
		prefix := string(key[0:1])
		switch prefix {
		case "s", "S":
			// Snapshot data is usually compressed
			value := make([]byte, rng.Intn(512)+32) // 32-544 bytes
			rng.Read(value)
			return value
		case "t":
			// Trie node data
			value := make([]byte, rng.Intn(1024)+64) // 64-1088 bytes
			rng.Read(value)
			return value
		}
	}
	
	value := make([]byte, w.config.ValueSize)
	rng.Read(value)
	return value
}

func (w *PoSStateWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	// State data is read much more than written
	return rng.Float64() < 0.95
}

func (w *PoSStateWorkload) SupportsRangeQueries() bool {
	return true
}

func (w *PoSStateWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	// Range queries over snapshot data
	queryTypes := []string{"snapshot-accounts", "snapshot-storage"}
	queryType := queryTypes[rng.Intn(len(queryTypes))]
	
	limit = rng.Intn(5000) + 100 // 100-5000 items (larger for snapshots)
	
	switch queryType {
	case "snapshot-accounts":
		prefix := []byte("s")
		startHash := make([]byte, 32)
		rng.Read(startHash)
		start = append(prefix, startHash...)
		
		endHash := make([]byte, 32)
		copy(endHash, startHash)
		// Increment for range
		for i := len(endHash) - 1; i >= 0; i-- {
			if endHash[i] < 255 {
				endHash[i]++
				break
			}
			endHash[i] = 0
		}
		end = append(prefix, endHash...)
		
	case "snapshot-storage":
		prefix := []byte("S")
		// Similar to account case but with account+storage hash
		startHash := make([]byte, 64) // account + storage hash
		rng.Read(startHash)
		start = append(prefix, startHash...)
		
		endHash := make([]byte, 64)
		copy(endHash, startHash)
		for i := len(endHash) - 1; i >= 0; i-- {
			if endHash[i] < 255 {
				endHash[i]++
				break
			}
			endHash[i] = 0
		}
		end = append(prefix, endHash...)
	}
	
	return start, end, limit
}