package benchmark

import (
	"encoding/binary"
	"iter"
	"math/rand"

	"github.com/ethereum/go-ethereum/crypto"
)

// GenerateKeys produces deterministic 32-byte hashed keys with some shared prefixes.
func GenerateKeys(seed int64, count int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rng := rand.New(rand.NewSource(seed))

		// Simulate shared prefixes: randomly assign a "prefix group" to each key
		numPrefixes := 32 // we can tune this for more or less prefix reuse
		prefixes := make([][]byte, numPrefixes)
		for i := 0; i < numPrefixes; i++ {
			raw := make([]byte, 8)
			binary.LittleEndian.PutUint64(raw, rng.Uint64())
			prefixes[i] = raw // 8-byte prefix
		}

		for i := 0; i < count; i++ {
			prefix := prefixes[rng.Intn(numPrefixes)]
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
