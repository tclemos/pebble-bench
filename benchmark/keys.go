package benchmark

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"os"
)

const readerBufferSize = 1024 * 1024

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
		for key := range loadKeysFromReader(r) {
			if !yield(key) {
				return
			}
		}
	}
}

// loadKeysFromStdin loads keys from standard input in the same binary format:
// [uvarint length][key bytes] repeating.
func loadKeysFromStdin() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		r := bufio.NewReader(os.Stdin)
		for key := range loadKeysFromReader(r) {
			if !yield(key) {
				return
			}
		}
	}
}

// loadKeysFromReader reads keys from an io.Reader in the binary format:
// [uvarint length][key bytes] repeating.
func loadKeysFromReader(r io.Reader) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		buf := bufio.NewReaderSize(r, readerBufferSize)
		for {
			n, err := binary.ReadUvarint(buf)
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(fmt.Errorf("failed to read key length from reader: %w", err))
			}

			key := make([]byte, n)
			_, err = io.ReadFull(buf, key)
			if err != nil {
				panic(fmt.Errorf("failed to read key bytes from reader: %w", err))
			}

			if !yield(key) {
				return
			}
		}
	}
}
