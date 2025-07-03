package benchmark

import (
	"encoding/binary"
	"iter"
	"math/rand"

	"github.com/ethereum/go-ethereum/crypto"
)

// GenericWorkload implements the original pebble-bench workload
type GenericWorkload struct {
	config       WorkloadConfig
	numPrefixes  int
}

// NewGenericWorkload creates a new generic workload (original pebble-bench behavior)
func NewGenericWorkload(cfg WorkloadConfig) *GenericWorkload {
	return &GenericWorkload{
		config:      cfg,
		numPrefixes: 32, // Original implementation used 32 prefix groups
	}
}

func (w *GenericWorkload) Name() string {
	return "Generic"
}

func (w *GenericWorkload) GetDescription() string {
	return "Generic hash-based workload with shared prefixes (original pebble-bench behavior)"
}

// GenerateKeys produces deterministic 32-byte hashed keys with some shared prefixes.
// This preserves the original pebble-bench key generation logic.
func (w *GenericWorkload) GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))

		// Simulate shared prefixes: randomly assign a "prefix group" to each key
		prefixes := make([][]byte, w.numPrefixes)
		for i := 0; i < w.numPrefixes; i++ {
			raw := make([]byte, 8)
			binary.LittleEndian.PutUint64(raw, rng.Uint64())
			prefixes[i] = raw // 8-byte prefix
		}

		for i := 0; i < count; i++ {
			prefix := prefixes[rng.Intn(w.numPrefixes)]
			suffix := make([]byte, 16) // random suffix
			rng.Read(suffix)
			rawKey := append(prefix, suffix...) // total 24 bytes pre-hash
			hash := crypto.Keccak256(rawKey)    // returns 32 bytes

			if !yield(hash) {
				return
			}
		}
	}
}

func (w *GenericWorkload) GenerateValue(rng *rand.Rand, key []byte) []byte {
	value := make([]byte, w.config.ValueSize)
	rng.Read(value)
	return value
}

func (w *GenericWorkload) ShouldRead(key []byte, rng *rand.Rand) bool {
	return rng.Float64() < w.config.ReadRatio
}

func (w *GenericWorkload) SupportsRangeQueries() bool {
	return false
}

func (w *GenericWorkload) GenerateRangeQuery(rng *rand.Rand) (start, end []byte, limit int) {
	return nil, nil, 0
}