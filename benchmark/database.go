package benchmark

import (
	"errors"
	"io"
	"time"
)

// Database defines the interface that all database backends must implement
// This allows pebble-bench to work with different storage engines while
// maintaining consistent benchmark semantics
type Database interface {
	// Set stores a key-value pair in the database
	// Returns error on failure
	Set(key, value []byte) error

	// Get retrieves a value for the given key
	// Returns the value, a closer (if needed), and error
	// Returns ErrKeyNotFound if key doesn't exist
	Get(key []byte) ([]byte, io.Closer, error)

	// Flush ensures all pending writes are persisted to storage
	// This is crucial for measuring write performance accurately
	Flush() error

	// Close properly shuts down the database and releases resources
	Close() error

	// GetMetrics returns database-specific performance metrics
	// This allows comparison of internal stats between backends
	GetMetrics() DatabaseMetrics
}

// DatabaseMetrics provides common metrics across different database backends
type DatabaseMetrics struct {
	// Memory usage
	CacheSize     int64 // bytes in cache (0 if no cache)
	MemTableSize  int64 // bytes in memory tables
	DataSize      uint64 // total data size
	KeyCount      uint64 // total number of keys
	
	// I/O statistics  
	BytesRead     int64 // total bytes read from storage
	BytesWritten  int64 // total bytes written to storage
	
	// Operation counts
	CacheHits     int64 // cache hit count
	CacheMisses   int64 // cache miss count
	CompactionOps int64 // compaction operations (LSM-specific)
	
	// Performance metrics
	ReadCount     uint64
	WriteCount    uint64
	FlushCount    uint64
	ReadErrors    uint64 
	WriteErrors   uint64
	FlushErrors   uint64
	ReadLatency   time.Duration
	WriteLatency  time.Duration
	FlushLatency  time.Duration
	
	// Database-specific metrics (optional)
	BackendSpecific map[string]interface{}
}

// Database backend types
type DatabaseType string

const (
	DatabaseTypePebble DatabaseType = "pebble"
	DatabaseTypeQMDB   DatabaseType = "qmdb"
	DatabaseTypeMDBX   DatabaseType = "mdbx"
)

// DatabaseConfig holds configuration for database creation
type DatabaseConfig struct {
	Type     DatabaseType
	Path     string
	ReadOnly bool
	
	// Pebble-specific options
	BlockCacheSize int64 // bytes, negative means disabled
	
	// QMDB-specific options
	QMDBConfig QMDBConfig
	
	// MDBX-specific options
	MDBXConfig MDBXConfig
}

// QMDBConfig holds QMDB-specific configuration options
type QMDBConfig struct {
	// Add QMDB-specific options as they become available
	// For now, using defaults from QMDB
	LibraryPath string // path to QMDB shared library
}

// MDBXConfig holds MDBX-specific configuration options
type MDBXConfig struct {
	// Database geometry settings
	MapSize     int64 // Maximum map size in bytes (-1 for default)
	MaxDbs      int   // Maximum number of databases (default: 2)
	MaxReaders  int   // Maximum number of readers (default: 128)
	
	// Performance settings
	NoSync      bool  // Don't fsync after commit
	NoMetaSync  bool  // Don't fsync metapage after commit
	WriteMap    bool  // Use writeable memory map
	NoReadahead bool  // Disable readahead
}

// Common database errors
var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrDatabaseClosed   = errors.New("database is closed")
	ErrInvalidOperation = errors.New("invalid operation")
	ErrBackendNotFound  = errors.New("database backend not found")
)

// NewDatabase creates a new database instance based on the configuration
func NewDatabase(cfg DatabaseConfig) (Database, error) {
	switch cfg.Type {
	case DatabaseTypePebble:
		return NewPebbleDatabase(cfg)
	case DatabaseTypeQMDB:
		return NewQMDBDatabase(cfg)
	case DatabaseTypeMDBX:
		return NewMDBXDatabase(cfg)
	default:
		return nil, ErrBackendNotFound
	}
}

// Helper function to check if an error is "key not found"
// This abstracts away backend-specific error types
func IsKeyNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}