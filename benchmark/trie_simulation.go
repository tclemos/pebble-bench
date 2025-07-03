package benchmark

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// TrieSimulation models the actual database operations that happen during trie traversal and updates
type TrieSimulation struct {
	// Current state root - most frequently accessed node
	stateRoot []byte
	
	// Cache of known node paths to simulate realistic access patterns
	knownPaths map[string][]byte
	
	// Track trie depth for realistic traversal patterns
	averageDepth int
	maxDepth     int
}

// DatabaseOperation represents a single database operation with metadata
type DatabaseOperation struct {
	Type        string // "READ", "WRITE", "DELETE"
	Key         []byte
	Value       []byte
	Description string // Human readable description of what this operation does
}

// TrieBatch represents a set of operations that happen together (like during a commit)
type TrieBatch struct {
	LogicalOperation string              // "account_update", "storage_update", "commit_flush"
	DatabaseOps      []DatabaseOperation
	AddressHash      []byte              // The account being operated on
}

// NewTrieSimulation creates a new trie simulation
func NewTrieSimulation() *TrieSimulation {
	stateRoot := make([]byte, 32)
	rand.Read(stateRoot)
	
	return &TrieSimulation{
		stateRoot:    stateRoot,
		knownPaths:   make(map[string][]byte),
		averageDepth: 6,  // Typical trie depth in Ethereum
		maxDepth:     16, // Maximum practical depth
	}
}

// SimulateAccountRead simulates reading an account's state, which requires trie traversal
func (ts *TrieSimulation) SimulateAccountRead(address []byte) TrieBatch {
	addressHash := crypto.Keccak256(address)
	ops := []DatabaseOperation{}
	
	// 1. Always read the state root first
	ops = append(ops, DatabaseOperation{
		Type:        "READ",
		Key:         append([]byte("stateroot"), ts.stateRoot...),
		Description: "Read state trie root node",
	})
	
	// 2. Traverse the trie path to find the account
	path := ts.computeTriePath(addressHash)
	for i, nodeKey := range path {
		ops = append(ops, DatabaseOperation{
			Type:        "READ", 
			Key:         nodeKey,
			Description: fmt.Sprintf("Read trie node at depth %d", i+1),
		})
	}
	
	// 3. Read the final account data
	accountKey := ts.computeAccountKey(addressHash)
	ops = append(ops, DatabaseOperation{
		Type:        "READ",
		Key:         accountKey,
		Description: "Read account state data",
	})
	
	return TrieBatch{
		LogicalOperation: "account_read",
		DatabaseOps:      ops,
		AddressHash:      addressHash,
	}
}

// SimulateAccountUpdate simulates updating an account, which is much more complex
func (ts *TrieSimulation) SimulateAccountUpdate(address []byte, newAccountData []byte) TrieBatch {
	addressHash := crypto.Keccak256(address)
	ops := []DatabaseOperation{}
	
	// 1. Read current state (same as read path)
	readBatch := ts.SimulateAccountRead(address)
	ops = append(ops, readBatch.DatabaseOps...)
	
	// 2. Write updated account data
	accountKey := ts.computeAccountKey(addressHash)
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         accountKey,
		Value:       newAccountData,
		Description: "Write updated account data",
	})
	
	// 3. Update intermediate trie nodes (bottom-up)
	path := ts.computeTriePath(addressHash)
	for i := len(path) - 1; i >= 0; i-- {
		// Simulate updating the node hash due to child changes
		updatedNodeData := ts.generateUpdatedTrieNode(path[i], i)
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         path[i],
			Value:       updatedNodeData,
			Description: fmt.Sprintf("Update trie node at depth %d due to child changes", i+1),
		})
	}
	
	// 4. Update state root
	newStateRoot := ts.generateNewStateRoot()
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         append([]byte("stateroot"), newStateRoot...),
		Value:       newStateRoot,
		Description: "Update state root hash",
	})
	
	// Update our simulation state
	ts.stateRoot = newStateRoot
	
	return TrieBatch{
		LogicalOperation: "account_update",
		DatabaseOps:      ops,
		AddressHash:      addressHash,
	}
}

// SimulateStorageUpdate simulates updating a storage slot, which involves both state and storage tries
func (ts *TrieSimulation) SimulateStorageUpdate(address []byte, storageKey []byte, value []byte) TrieBatch {
	addressHash := crypto.Keccak256(address)
	storageKeyHash := crypto.Keccak256(storageKey)
	ops := []DatabaseOperation{}
	
	// 1. Read account to get storage root
	accountBatch := ts.SimulateAccountRead(address)
	ops = append(ops, accountBatch.DatabaseOps...)
	
	// 2. Traverse storage trie (separate from state trie)
	storagePath := ts.computeStorageTriePath(addressHash, storageKeyHash)
	for i, nodeKey := range storagePath {
		ops = append(ops, DatabaseOperation{
			Type:        "READ",
			Key:         nodeKey,
			Description: fmt.Sprintf("Read storage trie node at depth %d", i+1),
		})
	}
	
	// 3. Write new storage value
	storageValueKey := ts.computeStorageValueKey(addressHash, storageKeyHash)
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         storageValueKey,
		Value:       value,
		Description: "Write storage slot value",
	})
	
	// 4. Update storage trie nodes (bottom-up)
	for i := len(storagePath) - 1; i >= 0; i-- {
		updatedNodeData := ts.generateUpdatedTrieNode(storagePath[i], i)
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         storagePath[i],
			Value:       updatedNodeData,
			Description: fmt.Sprintf("Update storage trie node at depth %d", i+1),
		})
	}
	
	// 5. Update account with new storage root
	newStorageRoot := ts.generateNewStorageRoot(addressHash)
	updatedAccountData := ts.generateAccountWithStorageRoot(addressHash, newStorageRoot)
	accountKey := ts.computeAccountKey(addressHash)
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         accountKey,
		Value:       updatedAccountData,
		Description: "Update account with new storage root",
	})
	
	// 6. Update state trie nodes due to account change
	statePath := ts.computeTriePath(addressHash)
	for i := len(statePath) - 1; i >= 0; i-- {
		updatedNodeData := ts.generateUpdatedTrieNode(statePath[i], i)
		ops = append(ops, DatabaseOperation{
			Type:        "WRITE",
			Key:         statePath[i],
			Value:       updatedNodeData,
			Description: fmt.Sprintf("Update state trie node at depth %d due to account change", i+1),
		})
	}
	
	// 7. Update state root
	newStateRoot := ts.generateNewStateRoot()
	ops = append(ops, DatabaseOperation{
		Type:        "WRITE",
		Key:         append([]byte("stateroot"), newStateRoot...),
		Value:       newStateRoot,
		Description: "Update state root hash",
	})
	
	ts.stateRoot = newStateRoot
	
	return TrieBatch{
		LogicalOperation: "storage_update",
		DatabaseOps:      ops,
		AddressHash:      addressHash,
	}
}

// computeTriePath simulates the path traversal through the trie
func (ts *TrieSimulation) computeTriePath(hash []byte) [][]byte {
	path := [][]byte{}
	currentPath := []byte{}
	
	// Simulate traversing 4-8 levels (realistic for Ethereum)
	depth := ts.averageDepth
	if len(hash) > 0 {
		// Use hash to determine depth variation
		depth += int(hash[0]%4) - 2 // +/- 2 levels
		if depth < 3 {
			depth = 3
		}
		if depth > ts.maxDepth {
			depth = ts.maxDepth
		}
	}
	
	for i := 0; i < depth; i++ {
		// Add nibble to path (4 bits)
		if i < len(hash)*2 {
			nibble := hash[i/2]
			if i%2 == 0 {
				nibble = nibble >> 4
			} else {
				nibble = nibble & 0x0F
			}
			currentPath = append(currentPath, nibble)
		} else {
			currentPath = append(currentPath, byte(i%16))
		}
		
		// Create node key for this path
		nodeKey := append([]byte("trie"), currentPath...)
		path = append(path, nodeKey)
	}
	
	return path
}

// computeStorageTriePath simulates storage trie traversal
func (ts *TrieSimulation) computeStorageTriePath(accountHash, storageKeyHash []byte) [][]byte {
	// Storage tries are typically shallower than state trie
	depth := ts.averageDepth - 2
	if depth < 2 {
		depth = 2
	}
	
	path := [][]byte{}
	currentPath := append([]byte("storage"), accountHash...)
	
	for i := 0; i < depth; i++ {
		if i < len(storageKeyHash)*2 {
			nibble := storageKeyHash[i/2]
			if i%2 == 0 {
				nibble = nibble >> 4
			} else {
				nibble = nibble & 0x0F
			}
			currentPath = append(currentPath, nibble)
		}
		
		nodeKey := make([]byte, len(currentPath))
		copy(nodeKey, currentPath)
		path = append(path, nodeKey)
	}
	
	return path
}

// computeAccountKey generates the final account storage key
func (ts *TrieSimulation) computeAccountKey(addressHash []byte) []byte {
	return append([]byte("account"), addressHash...)
}

// computeStorageValueKey generates the storage value key
func (ts *TrieSimulation) computeStorageValueKey(accountHash, storageKeyHash []byte) []byte {
	key := append([]byte("storage"), accountHash...)
	return append(key, storageKeyHash...)
}

// generateUpdatedTrieNode simulates creating updated trie node data
func (ts *TrieSimulation) generateUpdatedTrieNode(nodeKey []byte, depth int) []byte {
	// Simulate realistic trie node sizes (typically 32-200 bytes)
	baseSize := 64
	if depth == 0 {
		baseSize = 128 // Root nodes tend to be larger
	}
	
	// Create realistic node structure
	node := struct {
		NodeType uint8    // branch, extension, or leaf
		Children [][32]byte // Child hashes
		Value    []byte   // Node value
		Path     []byte   // Path information
	}{
		NodeType: uint8(depth % 3), // Vary node types
	}
	
	// Add some children (branch nodes have up to 16)
	numChildren := (depth + 1) % 8
	node.Children = make([][32]byte, numChildren)
	for i := range node.Children {
		rand.Read(node.Children[i][:])
	}
	
	// Add path and value data
	node.Path = nodeKey[min(len(nodeKey), 8):] // Use part of key as path
	node.Value = make([]byte, baseSize)
	rand.Read(node.Value)
	
	encoded, _ := rlp.EncodeToBytes(node)
	return encoded
}

// generateNewStateRoot creates a new state root hash
func (ts *TrieSimulation) generateNewStateRoot() []byte {
	// In reality, this would be computed from all trie nodes
	newRoot := make([]byte, 32)
	rand.Read(newRoot)
	return newRoot
}

// generateNewStorageRoot creates a new storage root for an account
func (ts *TrieSimulation) generateNewStorageRoot(accountHash []byte) []byte {
	// Use account hash as seed for deterministic but varied roots
	combined := append(accountHash, ts.stateRoot...)
	return crypto.Keccak256(combined)
}

// generateAccountWithStorageRoot creates account data with updated storage root
func (ts *TrieSimulation) generateAccountWithStorageRoot(addressHash, storageRoot []byte) []byte {
	account := struct {
		Nonce    uint64
		Balance  *big.Int
		Root     []byte // Storage root
		CodeHash []byte
	}{
		Nonce:    binary.BigEndian.Uint64(addressHash[:8]),
		Balance:  big.NewInt(1000000000000000000), // 1 ETH
		Root:     storageRoot,
		CodeHash: crypto.Keccak256([]byte("contract_code")),
	}
	
	encoded, _ := rlp.EncodeToBytes(account)
	return encoded
}

// GetRealisticReadWriteRatio returns the actual read/write ratio for trie operations
func (ts *TrieSimulation) GetRealisticReadWriteRatio(operationType string) float64 {
	switch operationType {
	case "account_read":
		return 1.0 // Pure read
	case "account_update":
		return 0.3 // 30% reads, 70% writes (reads during traversal, then many writes)
	case "storage_update":
		return 0.25 // 25% reads, 75% writes (even more writes due to dual trie update)
	default:
		return 0.7 // Default read ratio
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}