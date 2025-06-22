package benchmark

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// loadKeysFromFile loads a binary file containing keys in the format:
// [uvarint length][key bytes] repeating.
func loadKeysFromFile(path string) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open keys file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat error: %w", err)
	}

	mm, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap error: %w", err)
	}
	defer syscall.Munmap(mm)

	var keys [][]byte
	p := mm
	for len(p) > 0 {
		n, nbytes := binary.Uvarint(p)
		if nbytes <= 0 {
			return nil, fmt.Errorf("invalid Uvarint header at offset %d", len(mm)-len(p))
		}
		start, end := nbytes, nbytes+int(n)
		if end > len(p) {
			return nil, fmt.Errorf("key bytes truncated: need %d, have %d", end, len(p))
		}
		key := make([]byte, n)
		copy(key, p[start:end])
		keys = append(keys, key)

		p = p[end:]
	}

	return keys, nil
}
