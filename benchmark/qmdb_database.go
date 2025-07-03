package benchmark

/*
#cgo LDFLAGS: -L../lib -lqmdb -ldl
#include "../lib/qmdb.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/rs/zerolog/log"
)

// QMDBDatabase implements the Database interface for QMDB
type QMDBDatabase struct {
	path     string
	readOnly bool
	closed   bool
	handle   *C.QMDBHandle // QMDB database handle
}

// NewQMDBDatabase creates a new QMDB database instance
func NewQMDBDatabase(cfg DatabaseConfig) (Database, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}

	// Convert Go string to C string
	cPath := C.CString(cfg.Path)
	defer C.free(unsafe.Pointer(cPath))

	// Open QMDB database
	handle := C.qmdb_open(cPath)
	if handle == nil {
		return nil, fmt.Errorf("failed to open QMDB database at %s", cfg.Path)
	}

	db := &QMDBDatabase{
		path:     cfg.Path,
		readOnly: cfg.ReadOnly,
		closed:   false,
		handle:   handle,
	}

	log.Info().
		Str("path", cfg.Path).
		Bool("readonly", cfg.ReadOnly).
		Msg("Created QMDB database")

	return db, nil
}

// Set implements Database.Set for QMDB
func (q *QMDBDatabase) Set(key, value []byte) error {
	if q.closed {
		return ErrDatabaseClosed
	}
	
	if q.readOnly {
		return fmt.Errorf("cannot write to read-only database")
	}

	// Call QMDB set function
	var keyPtr, valuePtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.qmdb_set(q.handle, keyPtr, C.size_t(len(key)), valuePtr, C.size_t(len(value)))
	if result != C.QMDB_OK {
		return fmt.Errorf("QMDB set failed with code %d", result)
	}

	return nil
}

// Get implements Database.Get for QMDB
func (q *QMDBDatabase) Get(key []byte) ([]byte, io.Closer, error) {
	if q.closed {
		return nil, nil, ErrDatabaseClosed
	}

	// Allocate buffer for value (start with reasonable size)
	maxValueLen := C.size_t(64 * 1024) // 64KB buffer
	valueBuf := make([]byte, maxValueLen)
	
	var keyPtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	valuePtr := (*C.uint8_t)(unsafe.Pointer(&valueBuf[0]))
	actualLen := maxValueLen

	result := C.qmdb_get(q.handle, keyPtr, C.size_t(len(key)), valuePtr, &actualLen)
	
	switch result {
	case C.QMDB_OK:
		// Success - return the value
		return valueBuf[:actualLen], nil, nil
	case C.QMDB_NOT_FOUND:
		// Key not found
		return nil, nil, ErrKeyNotFound
	case C.QMDB_ERROR:
		// Check if buffer was too small
		if actualLen > maxValueLen {
			// Retry with larger buffer
			valueBuf = make([]byte, actualLen)
			valuePtr = (*C.uint8_t)(unsafe.Pointer(&valueBuf[0]))
			
			result = C.qmdb_get(q.handle, keyPtr, C.size_t(len(key)), valuePtr, &actualLen)
			if result == C.QMDB_OK {
				return valueBuf[:actualLen], nil, nil
			}
		}
		return nil, nil, fmt.Errorf("QMDB get failed with code %d", result)
	default:
		return nil, nil, fmt.Errorf("QMDB get failed with code %d", result)
	}
}

// Flush implements Database.Flush for QMDB  
func (q *QMDBDatabase) Flush() error {
	if q.closed {
		return ErrDatabaseClosed
	}

	result := C.qmdb_flush(q.handle)
	if result != C.QMDB_OK {
		return fmt.Errorf("QMDB flush failed with code %d", result)
	}

	return nil
}

// Close implements Database.Close for QMDB
func (q *QMDBDatabase) Close() error {
	if q.closed {
		return nil
	}

	if q.handle != nil {
		result := C.qmdb_close(q.handle)
		q.handle = nil
		if result != C.QMDB_OK {
			return fmt.Errorf("QMDB close failed with code %d", result)
		}
	}

	q.closed = true
	return nil
}

// GetMetrics implements Database.GetMetrics for QMDB
func (q *QMDBDatabase) GetMetrics() DatabaseMetrics {
	metrics := DatabaseMetrics{
		BackendSpecific: make(map[string]interface{}),
	}

	if q.closed || q.handle == nil {
		return metrics
	}

	// Get QMDB metrics through FFI
	var cMetrics C.QMDBMetrics
	result := C.qmdb_get_metrics(q.handle, &cMetrics)
	
	if result == C.QMDB_OK {
		// Map C metrics to Go metrics
		metrics.CacheSize = int64(cMetrics.cache_size_bytes)
		metrics.CacheHits = int64(cMetrics.cache_hits)
		metrics.CacheMisses = int64(cMetrics.cache_misses)
		metrics.BytesWritten = int64(cMetrics.total_size_bytes)
		
		// Store QMDB-specific metrics
		metrics.BackendSpecific["qmdb"] = map[string]interface{}{
			"entries_count":      int64(cMetrics.entries_count),
			"total_size_bytes":   int64(cMetrics.total_size_bytes),
			"cache_size_bytes":   int64(cMetrics.cache_size_bytes),
			"cache_hits":         int64(cMetrics.cache_hits),
			"cache_misses":       int64(cMetrics.cache_misses),
		}
	}

	return metrics
}