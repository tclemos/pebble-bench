package benchmark

import (
	"fmt"
	"iter"
	"math/rand"
)

// RealisticPoSStateWorkload simulates state trie operations with proper traversal patterns
type RealisticPoSStateWorkload struct {
	config         WorkloadConfig
	trieSimulation *TrieSimulation
	
	// Track common trie paths for spatial locality
	commonPaths   [][]byte
	rootAccesses  int
	commitCycle   int
}

// NewRealisticPoSStateWorkload creates a state-focused workload with realistic trie patterns
func NewRealisticPoSStateWorkload(cfg WorkloadConfig) *RealisticPoSStateWorkload {
	if cfg.StateLocality == 0 {
		cfg.StateLocality = 0.4 // Higher spatial locality for state operations
	}
	
	w := &RealisticPoSStateWorkload{
		config:         cfg,
		trieSimulation: NewTrieSimulation(),
		commonPaths:    make([][]byte, 0),
	}
	
	// Pre-populate some common paths for spatial locality
	w.initCommonPaths()
	
	return w
}

func (w *RealisticPoSStateWorkload) Name() string {
	return "PoS-State-Realistic"
}

func (w *RealisticPoSStateWorkload) GetDescription() string {
	return fmt.Sprintf("Realistic PoS state trie simulation with proper traversal patterns (locality: %.1f%%)",
		w.config.StateLocality*100)
}

// initCommonPaths creates frequently accessed paths for spatial locality
func (w *RealisticPoSStateWorkload) initCommonPaths() {
	// Simulate common prefixes that get accessed together
	commonPrefixes := [][]byte{
		{0x0, 0x1},     // Common contract address prefix
		{0x0, 0x0},     // Zero address prefix  
		{0x7, 0x5},     // Common wallet prefix
		{0xd, 0xf},     // Common DeFi protocol prefix
		{0x1, 0x2},     // Another common prefix
	}
	
	for _, prefix := range commonPrefixes {
		// Create multiple paths with this prefix
		for i := 0; i < 10; i++ {
			path := make([]byte, len(prefix)+4)
			copy(path, prefix)
			
			// Add random suffix
			for j := len(prefix); j < len(path); j++ {
				path[j] = byte(i*2 + j) % 16
			}
			
			w.commonPaths = append(w.commonPaths, path)
		}
	}
}

// GenerateKeys produces database operations that reflect real state trie usage
func (w *RealisticPoSStateWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		keysGenerated := 0
		
		// State operations with realistic frequency distribution
		operationTypes := []string{
			"state_root_read",    // Very frequent - every block/transaction
			"trie_traversal",     // Frequent - during state access
			"leaf_read",          // Common - reading final values
			"branch_read",        // Common - reading intermediate nodes
			"node_update",        // Less common - updating nodes
			"state_commit",       // Periodic - committing state changes
			"state_snapshot",     // Rare - creating snapshots
		}
		
		operationWeights := []float64{0.25, 0.3, 0.2, 0.15, 0.06, 0.03, 0.01}
		
		for keysGenerated < count {
			operationType := selectWeightedChoice(rng, operationTypes, operationWeights)
			
			var operations []DatabaseOperation
			
			switch operationType {
			case "state_root_read":
				operations = w.generateStateRootRead()
				w.rootAccesses++
				
			case "trie_traversal":
				operations = w.generateTrieTraversal(rng)
				
			case "leaf_read":
				operations = w.generateLeafRead(rng)
				
			case "branch_read":
				operations = w.generateBranchRead(rng)
				
			case "node_update":
				operations = w.generateNodeUpdate(rng)
				
			case "state_commit":
				operations = w.generateStateCommit(rng)
				w.commitCycle++
				
			case "state_snapshot":
				operations = w.generateStateSnapshot(rng)
			}
			
			// Yield all database operations from this logical operation
			for _, op := range operations {
				if !yield(op.Key) {
					return
				}
				keysGenerated++
				
				if keysGenerated >= count {
					break
				}
			}
		}
	}
}

// generateStateRootRead simulates reading the state root (most frequent operation)
func (w *RealisticPoSStateWorkload) generateStateRootRead() []DatabaseOperation {
	return []DatabaseOperation{
		{
			Type:        "READ",
			Key:         append([]byte("state_root"), w.trieSimulation.stateRoot...),
			Description: "Read current state root hash",
		},
	}
}

// generateTrieTraversal simulates traversing the trie to find a specific account/storage
func (w *RealisticPoSStateWorkload) generateTrieTraversal(rng *rand.Rand) []DatabaseOperation {
	ops := []DatabaseOperation{}
	
	// Start with state root
	ops = append(ops, DatabaseOperation{
		Type:        "READ",
		Key:         append([]byte("state_root"), w.trieSimulation.stateRoot...),
		Description: "Read state root for traversal",
	})
	
	// Choose traversal path (with spatial locality)
	var path []byte
	if rng.Float64() < w.config.StateLocality && len(w.commonPaths) > 0 {
		// Use common path for spatial locality
		basePath := w.commonPaths[rng.Intn(len(w.commonPaths))]
		path = make([]byte, len(basePath))
		copy(path, basePath)
		
		// Add small variation
		if len(path) > 0 {
			path[len(path)-1] = (path[len(path)-1] + byte(rng.Intn(4))) % 16
		}
	} else {
		// Generate random path
		pathLen := rng.Intn(8) + 4 // 4-12 nibbles
		path = make([]byte, pathLen)
		for i := range path {
			path[i] = byte(rng.Intn(16))
		}
	}
	
	// Simulate reading nodes along the path
	currentPath := []byte{}
	for i, nibble := range path {
		currentPath = append(currentPath, nibble)
		
		nodeKey := w.generateTrieNodeKey(currentPath, i)
		ops = append(ops, DatabaseOperation{
			Type:        "READ",
			Key:         nodeKey,
			Description: fmt.Sprintf("Read trie node at depth %d (nibble: %x)", i+1, nibble),
		})
		
		// Sometimes stop early (didn't find complete path)
		if rng.Float64() < 0.15 {
			break
		}
	}
	
	return ops
}

// generateLeafRead simulates reading a leaf node (final account/storage value)
func (w *RealisticPoSStateWorkload) generateLeafRead(rng *rand.Rand) []DatabaseOperation {
	// Generate account or storage key
	var key []byte
	var description string
	
	if rng.Float64() < 0.6 {
		// Account leaf
		accountHash := make([]byte, 32)
		rng.Read(accountHash)
		key = append([]byte("account_leaf"), accountHash...)
		description = "Read account leaf node"
	} else {
		// Storage leaf
		accountHash := make([]byte, 32)
		storageHash := make([]byte, 32)
		rng.Read(accountHash)
		rng.Read(storageHash)
		key = append([]byte("storage_leaf"), accountHash...)
		key = append(key, storageHash...)
		description = "Read storage leaf node"
	}
	
	return []DatabaseOperation{
		{
			Type:        "READ",
			Key:         key,
			Description: description,
		},
	}
}

// generateBranchRead simulates reading branch nodes (intermediate trie nodes)
func (w *RealisticPoSStateWorkload) generateBranchRead(rng *rand.Rand) []DatabaseOperation {
	// Branch nodes at different depths have different access patterns
	depth := rng.Intn(8) + 1
	
	// Shallow branch nodes are accessed more frequently
	var accessMultiplier int
	if depth <= 2 {
		accessMultiplier = 3 // Shallow nodes accessed 3x more
	} else if depth <= 4 {
		accessMultiplier = 2 // Medium depth 2x more
	} else {
		accessMultiplier = 1 // Deep nodes normal access
	}
	
	ops := []DatabaseOperation{}
	
	for i := 0; i < accessMultiplier; i++ {
		path := make([]byte, depth)
		for j := range path {
			path[j] = byte(rng.Intn(16))
		}
		
		nodeKey := w.generateTrieNodeKey(path, depth-1)
		ops = append(ops, DatabaseOperation{
			Type:        "READ",
			Key:         nodeKey,
			Description: fmt.Sprintf("Read branch node at depth %d", depth),
		})
		
		// Sometimes read adjacent nodes (spatial locality)
		if rng.Float64() < w.config.StateLocality {
			adjacentPath := make([]byte, len(path))
			copy(adjacentPath, path)
			if len(adjacentPath) > 0 {
				adjacentPath[len(adjacentPath)-1] = (adjacentPath[len(adjacentPath)-1] + 1) % 16
			}
			
			adjacentKey := w.generateTrieNodeKey(adjacentPath, depth-1)
			ops = append(ops, DatabaseOperation{
				Type:        "READ",
				Key:         adjacentKey,
				Description: fmt.Sprintf("Read adjacent branch node at depth %d", depth),
			})
		}
	}
	
	return ops
}

// generateNodeUpdate simulates updating trie nodes (writes)
func (w *RealisticPoSStateWorkload) generateNodeUpdate(rng *rand.Rand) []DatabaseOperation {
	ops := []DatabaseOperation{}
	
	// Updates typically happen in cascades (bottom-up)
	updateDepth := rng.Intn(6) + 2 // 2-8 levels
	
	path := make([]byte, updateDepth)
	for i := range path {
		path[i] = byte(rng.Intn(16))
	}
	
	// Update from leaf to root
	for i := updateDepth - 1; i >= 0; i-- {
		currentPath := path[:i+1]
		nodeKey := w.generateTrieNodeKey(currentPath, i)
		nodeValue := w.generateRealisticTrieNodeValue(rng, i)
		
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         nodeKey,
			Value:       nodeValue,
			Description: fmt.Sprintf("Update trie node at depth %d", i+1),
		})
	}
	
	// Finally update state root
	newStateRoot := w.trieSimulation.generateNewStateRoot()
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         append([]byte("state_root"), newStateRoot...),
		Value:       newStateRoot,
		Description: "Update state root after node changes",
	})
	
	return ops
}

// generateStateCommit simulates committing pending state changes
func (w *RealisticPoSStateWorkload) generateStateCommit(rng *rand.Rand) []DatabaseOperation {
	ops := []DatabaseOperation{}
	
	// Read current state for commit preparation
	ops = append(ops, DatabaseOperation{
		Type:        "READ",
		Key:         append([]byte("state_root"), w.trieSimulation.stateRoot...),
		Description: "Read state root for commit",
	})
	
	// Simulate writing dirty nodes (batch operation)
	numDirtyNodes := rng.Intn(100) + 20 // 20-120 dirty nodes
	for i := 0; i < numDirtyNodes; i++ {
		// Generate realistic node distribution
		depth := w.selectNodeDepthForCommit(rng)
		path := make([]byte, depth)
		for j := range path {
			path[j] = byte(rng.Intn(16))
		}
		
		nodeKey := w.generateTrieNodeKey(path, depth-1)
		nodeValue := w.generateRealisticTrieNodeValue(rng, depth-1)
		
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         nodeKey,
			Value:       nodeValue,
			Description: fmt.Sprintf("Commit dirty node at depth %d", depth),
		})
	}
	
	// Final state root commit
	newStateRoot := w.trieSimulation.generateNewStateRoot()
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         append([]byte("state_root_final"), newStateRoot...),
		Value:       newStateRoot,
		Description: "Commit final state root",
	})
	
	return ops
}

// generateStateSnapshot simulates creating state snapshots (rare but expensive)
func (w *RealisticPoSStateWorkload) generateStateSnapshot(rng *rand.Rand) []DatabaseOperation {
	ops := []DatabaseOperation{}
	
	// Snapshots read many nodes across the entire trie
	numReads := rng.Intn(500) + 100 // 100-600 reads for snapshot
	
	for i := 0; i < numReads; i++ {
		// Random depth and path for comprehensive snapshot
		depth := rng.Intn(8) + 1
		path := make([]byte, depth)
		for j := range path {
			path[j] = byte(rng.Intn(16))
		}
		
		nodeKey := w.generateTrieNodeKey(path, depth-1)
		ops = append(ops, DatabaseOperation{
			Type:        "READ",
			Key:         nodeKey,
			Description: fmt.Sprintf("Snapshot read: node at depth %d", depth),
		})
	}
	
	// Write snapshot metadata
	snapshotKey := fmt.Sprintf("snapshot_%d", rng.Int63())
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         []byte(snapshotKey),
		Value:       []byte("snapshot_metadata"),
		Description: "Write snapshot metadata",
	})
	
	return ops
}

// Helper functions

func (w *RealisticPoSStateWorkload) generateTrieNodeKey(path []byte, depth int) []byte {
	key := append([]byte("trie_node"), path...)
	key = append(key, byte(depth))
	return key
}

func (w *RealisticPoSStateWorkload) generateRealisticTrieNodeValue(rng *rand.Rand, depth int) []byte {
	// Node size varies by depth and type
	baseSize := 64
	if depth == 0 {
		baseSize = 128 // Root nodes larger
	} else if depth < 3 {
		baseSize = 96  // Shallow nodes larger
	}
	
	size := baseSize + rng.Intn(baseSize/2)
	value := make([]byte, size)
	rng.Read(value)
	return value
}

func (w *RealisticPoSStateWorkload) selectNodeDepthForCommit(rng *rand.Rand) int {
	// Deeper nodes are more likely to be dirty during commits
	weights := []float64{0.1, 0.15, 0.2, 0.25, 0.15, 0.1, 0.05} // Depths 1-7
	
	r := rng.Float64()
	cumulative := 0.0
	for i, weight := range weights {
		cumulative += weight
		if r <= cumulative {
			return i + 1
		}
	}
	return 4 // Default depth
}

// Workload interface methods

func (w *RealisticPoSStateWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) == 0 {
		return make([]byte, w.config.ValueSize)
	}
	
	keyStr := string(key)
	
	switch {
	case len(key) >= 10 && keyStr[:10] == "state_root":
		// State root: 32 bytes
		value := make([]byte, 32)
		rng.Read(value)
		return value
		
	case len(key) >= 9 && keyStr[:9] == "trie_node":
		// Trie nodes: variable size based on depth
		depth := 4
		if len(key) > 0 {
			depth = int(key[len(key)-1]) % 8
		}
		return w.generateRealisticTrieNodeValue(rng, depth)
		
	case len(key) >= 12 && keyStr[:12] == "account_leaf":
		// Account data: ~128 bytes
		value := make([]byte, 128)
		rng.Read(value)
		return value
		
	case len(key) >= 12 && keyStr[:12] == "storage_leaf":
		// Storage value: 32 bytes
		value := make([]byte, 32)
		rng.Read(value)
		return value
		
	default:
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

func (w *RealisticPoSStateWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	if len(key) == 0 {
		return rng.Float64() < w.config.ReadRatio
	}
	
	keyStr := string(key)
	
	switch {
	case len(key) >= 10 && keyStr[:10] == "state_root":
		// State root: mostly reads
		return rng.Float64() < 0.95
		
	case len(key) >= 9 && keyStr[:9] == "trie_node":
		// Trie nodes: read during traversal, written during updates
		return rng.Float64() < 0.8
		
	case len(key) >= 12 && (keyStr[:12] == "account_leaf" || keyStr[:12] == "storage_leaf"):
		// Leaf nodes: read-heavy
		return rng.Float64() < 0.9
		
	default:
		return rng.Float64() < w.config.ReadRatio
	}
}

func (w *RealisticPoSStateWorkload) SupportsRangeQueries() bool {
	return true
}

func (w *RealisticPoSStateWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	// State tries don't use range queries as much, but when they do:
	queryTypes := []string{"node_range", "account_range"}
	queryType := queryTypes[rng.Intn(len(queryTypes))]
	
	limit = rng.Intn(50) + 5 // Smaller ranges for state operations
	
	switch queryType {
	case "node_range":
		// Range over nodes at same depth
		depth := rng.Intn(6) + 1
		prefix := append([]byte("trie_node"), make([]byte, depth)...)
		
		start = make([]byte, len(prefix))
		copy(start, prefix)
		
		end = make([]byte, len(prefix))
		copy(end, prefix)
		if len(end) > 9 {
			end[len(end)-1] = 0xFF
		}
		
	case "account_range":
		start = []byte("account_leaf")
		end = []byte("account_leafz") // Lexicographically after
	}
	
	return start, end, limit
}