package benchmark

import (
	"fmt"
	"iter"
	"math/big"
	"math/rand"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// PoSAccountWorkload simulates account state access patterns
// This includes account data, storage slots, and state trie access
type PoSAccountWorkload struct {
	config      WorkloadConfig
	hotAccounts [][]byte // Pre-generated "hot" accounts that get frequent access
}

// NewPoSAccountWorkload creates a new PoS account-focused workload
func NewPoSAccountWorkload(cfg WorkloadConfig) *PoSAccountWorkload {
	// Set reasonable defaults
	if cfg.AccountCount == 0 {
		cfg.AccountCount = 100000 // 100k accounts
	}
	if cfg.HotAccountRatio == 0 {
		cfg.HotAccountRatio = 0.2 // 20% of accounts are "hot"
	}
	if cfg.StorageSlotRatio == 0 {
		cfg.StorageSlotRatio = 5.0 // Average 5 storage slots per account
	}
	if cfg.StateLocality == 0 {
		cfg.StateLocality = 0.3 // 30% chance to access related state
	}
	
	return &PoSAccountWorkload{
		config: cfg,
	}
}

func (w *PoSAccountWorkload) Name() string {
	return "PoS-Accounts"
}

func (w *PoSAccountWorkload) GetDescription() string {
	return fmt.Sprintf("PoS account state simulation (%d accounts, %.1f%% hot, %.1fx storage ratio)", 
		w.config.AccountCount, w.config.HotAccountRatio*100, w.config.StorageSlotRatio)
}

// initHotAccounts pre-generates the hot accounts for consistent access patterns
func (w *PoSAccountWorkload) initHotAccounts(rng *rand.Rand) {
	hotCount := int(float64(w.config.AccountCount) * w.config.HotAccountRatio)
	w.hotAccounts = make([][]byte, hotCount)
	
	for i := range w.hotAccounts {
		w.hotAccounts[i] = w.generateAccountAddress(rng)
	}
}

// GenerateKeys creates realistic account and storage keys
func (w *PoSAccountWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		w.initHotAccounts(rng)
		
		keysGenerated := 0
		
		// Key types: account state, storage slots, state trie nodes
		keyTypes := []string{"account", "storage", "statenode", "storagenode"}
		keyWeights := []float64{0.25, 0.4, 0.2, 0.15} // Storage access is most common

		for keysGenerated < count {
			keyType := selectWeightedChoice(rng, keyTypes, keyWeights)
			
			var key []byte
			switch keyType {
			case "account":
				key = w.generateAccountKey(rng)
			case "storage":
				key = w.generateStorageKey(rng)
			case "statenode":
				key = w.generateStateTrieNodeKey(rng)
			case "storagenode":
				key = w.generateStorageTrieNodeKey(rng)
			}

			if !yield(key) {
				return
			}
			keysGenerated++
		}
	}
}

// generateAccountKey creates an account state key: "a" + accountHash
func (w *PoSAccountWorkload) generateAccountKey(rng *rand.Rand) []byte {
	prefix := []byte("a")
	
	var accountAddr []byte
	
	// Use hot account bias
	if rng.Float64() < 0.8 && len(w.hotAccounts) > 0 { // 80% chance to use hot account
		accountAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
	} else {
		accountAddr = w.generateAccountAddress(rng)
	}
	
	// Hash the account address for the key
	accountHash := crypto.Keccak256(accountAddr)
	
	return append(prefix, accountHash...)
}

// generateStorageKey creates a storage slot key: "o" + accountHash + storageHash
func (w *PoSAccountWorkload) generateStorageKey(rng *rand.Rand) []byte {
	prefix := []byte("o")
	
	var accountAddr []byte
	
	// Use hot account bias for storage access too
	if rng.Float64() < 0.8 && len(w.hotAccounts) > 0 {
		accountAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
	} else {
		accountAddr = w.generateAccountAddress(rng)
	}
	
	accountHash := crypto.Keccak256(accountAddr)
	
	// Generate storage slot hash
	storageSlot := make([]byte, 32)
	rng.Read(storageSlot)
	storageHash := crypto.Keccak256(storageSlot)
	
	key := append(prefix, accountHash...)
	key = append(key, storageHash...)
	
	return key
}

// generateStateTrieNodeKey creates a state trie node key: "A" + hexPath
func (w *PoSAccountWorkload) generateStateTrieNodeKey(rng *rand.Rand) []byte {
	prefix := []byte("A")
	
	// Generate hex path for trie traversal (variable length)
	pathLength := rng.Intn(64) + 1 // 1-64 nibbles
	hexPath := make([]byte, pathLength)
	
	for i := range hexPath {
		hexPath[i] = byte(rng.Intn(16)) // 0-15 (hex digit)
	}
	
	return append(prefix, hexPath...)
}

// generateStorageTrieNodeKey creates a storage trie node key: "O" + accountHash + hexPath  
func (w *PoSAccountWorkload) generateStorageTrieNodeKey(rng *rand.Rand) []byte {
	prefix := []byte("O")
	
	// Generate account hash
	accountAddr := w.generateAccountAddress(rng)
	accountHash := crypto.Keccak256(accountAddr)
	
	// Generate hex path
	pathLength := rng.Intn(64) + 1
	hexPath := make([]byte, pathLength)
	
	for i := range hexPath {
		hexPath[i] = byte(rng.Intn(16))
	}
	
	key := append(prefix, accountHash...)
	key = append(key, hexPath...)
	
	return key
}

// generateAccountAddress creates a realistic 20-byte Ethereum address
func (w *PoSAccountWorkload) generateAccountAddress(rng *rand.Rand) []byte {
	addr := make([]byte, 20)
	rng.Read(addr)
	return addr
}

func (w *PoSAccountWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) == 0 {
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
	
	prefix := string(key[0:1])
	
	switch prefix {
	case "a":
		// Account state data
		return w.generateAccountValue(rng)
	case "o":
		// Storage slot value
		return w.generateStorageValue(rng)
	case "A", "O":
		// Trie node data
		return w.generateTrieNodeValue(rng)
	default:
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

func (w *PoSAccountWorkload) generateAccountValue(rng *rand.Rand) []byte {
	// Simulate account state structure
	account := struct {
		Nonce    uint64
		Balance  *big.Int
		Root     [32]byte // Storage trie root
		CodeHash [32]byte
	}{
		Nonce:   rng.Uint64(),
		Balance: big.NewInt(rng.Int63()),
	}
	
	rng.Read(account.Root[:])
	rng.Read(account.CodeHash[:])
	
	encoded, _ := rlp.EncodeToBytes(account)
	return encoded
}

func (w *PoSAccountWorkload) generateStorageValue(rng *rand.Rand) []byte {
	// Storage values are typically 32-byte words
	value := make([]byte, 32)
	rng.Read(value)
	return value
}

func (w *PoSAccountWorkload) generateTrieNodeValue(rng *rand.Rand) []byte {
	// Simulate trie node structure (simplified)
	// Trie nodes can be leaf nodes, extension nodes, or branch nodes
	nodeType := rng.Intn(3)
	
	switch nodeType {
	case 0: // Leaf node
		keyEnd := make([]byte, rng.Intn(32)+1)
		rng.Read(keyEnd)
		
		value := make([]byte, rng.Intn(1024)+1) // Variable size value
		rng.Read(value)
		
		node := []interface{}{keyEnd, value}
		encoded, _ := rlp.EncodeToBytes(node)
		return encoded
		
	case 1: // Extension node
		sharedKey := make([]byte, rng.Intn(16)+1)
		rng.Read(sharedKey)
		
		nextHash := make([]byte, 32)
		rng.Read(nextHash)
		
		node := []interface{}{sharedKey, nextHash}
		encoded, _ := rlp.EncodeToBytes(node)
		return encoded
		
	case 2: // Branch node
		branches := make([]interface{}, 17) // 16 hex + value
		for i := 0; i < 16; i++ {
			if rng.Float64() < 0.3 { // 30% chance of having a branch
				hash := make([]byte, 32)
				rng.Read(hash)
				branches[i] = hash
			} else {
				branches[i] = []byte{}
			}
		}
		
		// Value at this node (optional)
		if rng.Float64() < 0.1 { // 10% chance of having value
			nodeValue := make([]byte, rng.Intn(256))
			rng.Read(nodeValue)
			branches[16] = nodeValue
		} else {
			branches[16] = []byte{}
		}
		
		encoded, _ := rlp.EncodeToBytes(branches)
		return encoded
		
	default:
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

func (w *PoSAccountWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	// Account reads are more common than writes in typical blockchain usage
	// Storage reads are very common, writes less so
	if len(key) > 0 {
		prefix := string(key[0:1])
		switch prefix {
		case "a":
			// Account state: 90% reads
			return rng.Float64() < 0.9
		case "o":
			// Storage: 95% reads (most storage access is reads)
			return rng.Float64() < 0.95
		case "A", "O":
			// Trie nodes: 98% reads (very rarely modified)
			return rng.Float64() < 0.98
		}
	}
	
	return rng.Float64() < w.config.ReadRatio
}

func (w *PoSAccountWorkload) SupportsRangeQueries() bool {
	return true
}

func (w *PoSAccountWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	// Generate range queries for account/storage enumeration
	queryTypes := []string{"accounts", "storage"}
	queryType := queryTypes[rng.Intn(len(queryTypes))]
	
	limit = rng.Intn(1000) + 10 // 10-1000 items
	
	switch queryType {
	case "accounts":
		// Range over account state keys
		prefix := []byte("a")
		
		// Generate starting hash
		startHash := make([]byte, 32)
		rng.Read(startHash)
		start = append(prefix, startHash...)
		
		// Generate ending hash (higher value)
		endHash := make([]byte, 32)
		copy(endHash, startHash)
		// Increment the hash to create a range
		for i := len(endHash) - 1; i >= 0; i-- {
			if endHash[i] < 255 {
				endHash[i]++
				break
			}
			endHash[i] = 0
		}
		end = append(prefix, endHash...)
		
	case "storage":
		// Range over storage keys for a specific account
		prefix := []byte("o")
		
		// Select account (prefer hot accounts)
		var accountAddr []byte
		if rng.Float64() < 0.8 && len(w.hotAccounts) > 0 {
			accountAddr = w.hotAccounts[rng.Intn(len(w.hotAccounts))]
		} else {
			accountAddr = w.generateAccountAddress(rng)
		}
		
		accountHash := crypto.Keccak256(accountAddr)
		
		// Start with account hash + zero storage hash
		start = append(prefix, accountHash...)
		zeroStorage := make([]byte, 32)
		start = append(start, zeroStorage...)
		
		// End with account hash + max storage hash
		end = append(prefix, accountHash...)
		maxStorage := make([]byte, 32)
		for i := range maxStorage {
			maxStorage[i] = 0xFF
		}
		end = append(end, maxStorage...)
	}
	
	return start, end, limit
}