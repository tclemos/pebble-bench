package benchmark

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"

	"github.com/ethereum/go-ethereum/rlp"
)

// PoSBlockWorkload simulates blockchain block storage patterns
// This includes block headers, block bodies, and transaction lookups
type PoSBlockWorkload struct {
	config WorkloadConfig
}

// NewPoSBlockWorkload creates a new PoS block-focused workload
func NewPoSBlockWorkload(cfg WorkloadConfig) *PoSBlockWorkload {
	// Set reasonable defaults for PoS block workload
	if cfg.BlockRange == 0 {
		cfg.BlockRange = 100000 // Simulate 100k blocks
	}
	if cfg.RecentBlockBias == 0 {
		cfg.RecentBlockBias = 0.8 // 80% of accesses to recent 20% of blocks
	}
	
	return &PoSBlockWorkload{
		config: cfg,
	}
}

func (w *PoSBlockWorkload) Name() string {
	return "PoS-Blocks"
}

func (w *PoSBlockWorkload) GetDescription() string {
	return fmt.Sprintf("PoS blockchain block storage simulation (range: %d blocks, recent bias: %.1f%%)", 
		w.config.BlockRange, w.config.RecentBlockBias*100)
}

// GenerateKeys creates realistic blockchain keys for blocks, headers, and transactions
func (w *PoSBlockWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))
		keysGenerated := 0

		// Generate different types of keys found in blockchain storage
		keyTypes := []string{"header", "body", "receipts", "txlookup"}
		keyWeights := []float64{0.3, 0.2, 0.2, 0.3} // Distribution of key types

		for keysGenerated < count {
			// Select key type based on weights
			keyType := selectWeightedChoice(rng, keyTypes, keyWeights)
			
			var key []byte
			switch keyType {
			case "header":
				key = w.generateHeaderKey(rng)
			case "body":
				key = w.generateBodyKey(rng)
			case "receipts":
				key = w.generateReceiptsKey(rng)
			case "txlookup":
				key = w.generateTxLookupKey(rng)
			}

			if !yield(key) {
				return
			}
			keysGenerated++
		}
	}
}

// generateHeaderKey creates a header key: "h" + blockNumber + blockHash
func (w *PoSBlockWorkload) generateHeaderKey(rng *rand.Rand) []byte {
	prefix := []byte("h")
	
	// Generate block number with recent bias
	var blockNum uint64
	if rng.Float64() < w.config.RecentBlockBias {
		// Recent blocks (last 20% of range)
		recentRange := uint64(float64(w.config.BlockRange) * 0.2)
		blockNum = uint64(w.config.BlockRange) - rng.Uint64()%recentRange
	} else {
		// Older blocks
		blockNum = rng.Uint64() % uint64(w.config.BlockRange)
	}
	
	// Encode block number (8 bytes)
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNum)
	
	// Generate block hash (32 bytes)
	blockHash := make([]byte, 32)
	rng.Read(blockHash)
	
	// Key format: prefix + blockNumber + blockHash
	key := append(prefix, blockNumBytes...)
	key = append(key, blockHash...)
	
	return key
}

// generateBodyKey creates a body key: "b" + blockNumber + blockHash  
func (w *PoSBlockWorkload) generateBodyKey(rng *rand.Rand) []byte {
	prefix := []byte("b")
	
	// Use same logic as header key but with "b" prefix
	var blockNum uint64
	if rng.Float64() < w.config.RecentBlockBias {
		recentRange := uint64(float64(w.config.BlockRange) * 0.2)
		blockNum = uint64(w.config.BlockRange) - rng.Uint64()%recentRange
	} else {
		blockNum = rng.Uint64() % uint64(w.config.BlockRange)
	}
	
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNum)
	
	blockHash := make([]byte, 32)
	rng.Read(blockHash)
	
	key := append(prefix, blockNumBytes...)
	key = append(key, blockHash...)
	
	return key
}

// generateReceiptsKey creates a receipts key: "r" + blockNumber + blockHash
func (w *PoSBlockWorkload) generateReceiptsKey(rng *rand.Rand) []byte {
	prefix := []byte("r")
	
	var blockNum uint64
	if rng.Float64() < w.config.RecentBlockBias {
		recentRange := uint64(float64(w.config.BlockRange) * 0.2)
		blockNum = uint64(w.config.BlockRange) - rng.Uint64()%recentRange
	} else {
		blockNum = rng.Uint64() % uint64(w.config.BlockRange)
	}
	
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNum)
	
	blockHash := make([]byte, 32)
	rng.Read(blockHash)
	
	key := append(prefix, blockNumBytes...)
	key = append(key, blockHash...)
	
	return key
}

// generateTxLookupKey creates a transaction lookup key: "l" + txHash
func (w *PoSBlockWorkload) generateTxLookupKey(rng *rand.Rand) []byte {
	prefix := []byte("l")
	
	// Generate transaction hash (32 bytes)
	txHash := make([]byte, 32)
	rng.Read(txHash)
	
	return append(prefix, txHash...)
}

func (w *PoSBlockWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	if len(key) == 0 {
		// Fallback to random value
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
	
	// Generate realistic values based on key type
	prefix := string(key[0:1])
	
	switch prefix {
	case "h":
		// Block header: realistic RLP-encoded header structure
		return w.generateBlockHeaderValue(rng)
	case "b":
		// Block body: array of transactions
		return w.generateBlockBodyValue(rng)
	case "r":
		// Receipts: array of transaction receipts
		return w.generateReceiptsValue(rng)
	case "l":
		// Transaction lookup: compact block reference
		return w.generateTxLookupValue(rng)
	default:
		// Default random value
		value := make([]byte, w.config.ValueSize)
		rng.Read(value)
		return value
	}
}

func (w *PoSBlockWorkload) generateBlockHeaderValue(rng *rand.Rand) []byte {
	// Simulate a realistic block header structure
	header := struct {
		ParentHash  [32]byte
		UncleHash   [32]byte
		Coinbase    [20]byte
		Root        [32]byte
		TxHash      [32]byte
		ReceiptHash [32]byte
		Bloom       [256]byte
		Difficulty  uint64
		Number      uint64
		GasLimit    uint64
		GasUsed     uint64
		Time        uint64
		Extra       []byte
		MixDigest   [32]byte
		Nonce       uint64
	}{}
	
	// Fill with random data
	rng.Read(header.ParentHash[:])
	rng.Read(header.UncleHash[:])
	rng.Read(header.Coinbase[:])
	rng.Read(header.Root[:])
	rng.Read(header.TxHash[:])
	rng.Read(header.ReceiptHash[:])
	rng.Read(header.Bloom[:])
	header.Difficulty = rng.Uint64()
	header.Number = rng.Uint64()
	header.GasLimit = rng.Uint64()
	header.GasUsed = rng.Uint64()
	header.Time = rng.Uint64()
	header.Extra = make([]byte, rng.Intn(32)) // 0-32 bytes extra data
	rng.Read(header.Extra)
	rng.Read(header.MixDigest[:])
	header.Nonce = rng.Uint64()
	
	// RLP encode
	encoded, _ := rlp.EncodeToBytes(header)
	return encoded
}

func (w *PoSBlockWorkload) generateBlockBodyValue(rng *rand.Rand) []byte {
	// Simulate a block body with variable number of transactions
	txCount := rng.Intn(200) + 1 // 1-200 transactions per block
	
	// Simple transaction structure
	type Transaction struct {
		Nonce    uint64
		GasPrice uint64
		Gas      uint64
		To       [20]byte
		Value    uint64
		Data     []byte
		V        uint64
		R        [32]byte
		S        [32]byte
	}
	
	transactions := make([]Transaction, txCount)
	for i := range transactions {
		tx := &transactions[i]
		tx.Nonce = rng.Uint64()
		tx.GasPrice = rng.Uint64()
		tx.Gas = rng.Uint64()
		rng.Read(tx.To[:])
		tx.Value = rng.Uint64()
		tx.Data = make([]byte, rng.Intn(1024)) // 0-1KB transaction data
		rng.Read(tx.Data)
		tx.V = rng.Uint64()
		rng.Read(tx.R[:])
		rng.Read(tx.S[:])
	}
	
	encoded, _ := rlp.EncodeToBytes(transactions)
	return encoded
}

func (w *PoSBlockWorkload) generateReceiptsValue(rng *rand.Rand) []byte {
	// Simulate transaction receipts
	receiptCount := rng.Intn(200) + 1
	
	type Receipt struct {
		Status            uint64
		CumulativeGasUsed uint64
		Bloom             [256]byte
		Logs              [][]byte
	}
	
	receipts := make([]Receipt, receiptCount)
	for i := range receipts {
		receipt := &receipts[i]
		receipt.Status = uint64(rng.Intn(2)) // 0 or 1
		receipt.CumulativeGasUsed = rng.Uint64()
		rng.Read(receipt.Bloom[:])
		
		// Generate some log entries
		logCount := rng.Intn(5)
		receipt.Logs = make([][]byte, logCount)
		for j := range receipt.Logs {
			logData := make([]byte, rng.Intn(256))
			rng.Read(logData)
			receipt.Logs[j] = logData
		}
	}
	
	encoded, _ := rlp.EncodeToBytes(receipts)
	return encoded
}

func (w *PoSBlockWorkload) generateTxLookupValue(rng *rand.Rand) []byte {
	// Transaction lookup value: block number + transaction index
	lookup := struct {
		BlockNumber uint64
		TxIndex     uint64
	}{
		BlockNumber: rng.Uint64() % uint64(w.config.BlockRange),
		TxIndex:     rng.Uint64() % 200, // 0-199 transaction index
	}
	
	encoded, _ := rlp.EncodeToBytes(lookup)
	return encoded
}

func (w *PoSBlockWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	return rng.Float64() < w.config.ReadRatio
}

func (w *PoSBlockWorkload) SupportsRangeQueries() bool {
	return true
}

func (w *PoSBlockWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	// Generate range queries for sequential block access
	keyTypes := []string{"header", "body", "receipts"}
	keyType := keyTypes[rng.Intn(len(keyTypes))]
	
	var prefix []byte
	switch keyType {
	case "header":
		prefix = []byte("h")
	case "body":
		prefix = []byte("b")
	case "receipts":
		prefix = []byte("r")
	}
	
	// Select a random starting block
	startBlock := rng.Uint64() % uint64(w.config.BlockRange)
	rangeSize := uint64(rng.Intn(100) + 1) // 1-100 blocks
	
	// Create start key
	startBlockBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(startBlockBytes, startBlock)
	start = append(prefix, startBlockBytes...)
	
	// Create end key
	endBlock := startBlock + rangeSize
	endBlockBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(endBlockBytes, endBlock)
	end = append(prefix, endBlockBytes...)
	
	limit = int(rangeSize)
	return start, end, limit
}

// Helper function to select from weighted choices
func selectWeightedChoice(rng *rand.Rand, choices []string, weights []float64) string {
	if len(choices) != len(weights) {
		return choices[rng.Intn(len(choices))]
	}
	
	// Calculate cumulative weights
	total := 0.0
	for _, w := range weights {
		total += w
	}
	
	r := rng.Float64() * total
	cumulative := 0.0
	
	for i, weight := range weights {
		cumulative += weight
		if r <= cumulative {
			return choices[i]
		}
	}
	
	// Fallback
	return choices[len(choices)-1]
}