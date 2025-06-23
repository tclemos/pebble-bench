package benchmark

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"os"
)

// loadKeysFromFile loads a binary file containing keys in the format:
// [uvarint length][key bytes] repeating.
func loadKeysFromFile(path string) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		file, err := os.Open(path)
		if err != nil {
			panic(fmt.Errorf("failed to open keys file: %w", err))
		}
		defer file.Close()

		r := bufio.NewReader(file)
		for {
			n, err := binary.ReadUvarint(r)
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(fmt.Errorf("failed to read key length: %w", err))
			}

			key := make([]byte, n)
			_, err = io.ReadFull(r, key)
			if err != nil {
				panic(fmt.Errorf("failed to read key bytes: %w", err))
			}

			if !yield(key) {
				return
			}
		}
	}
}
