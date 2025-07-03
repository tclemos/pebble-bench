package benchmark

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
)

// MDBXDatabase implements the Database interface using MDBX (libmdbx)
type MDBXDatabase struct {
	env     *mdbx.Env
	db      mdbx.DBI
	path    string
	mu      sync.RWMutex
	closed  bool
	metrics DatabaseMetrics
}

// NewMDBXDatabase creates a new MDBX database instance
func NewMDBXDatabase(cfg DatabaseConfig) (Database, error) {
	path := cfg.Path
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create MDBX environment
	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		return nil, fmt.Errorf("failed to create MDBX environment: %w", err)
	}

	// Set environment options
	if err := env.SetGeometry(
		-1,        // size lower bound: use default
		-1,        // size now: use default  
		-1,        // size upper bound: use default
		-1,        // growth step: use default
		-1,        // shrink threshold: use default
		-1,        // page size: use default
	); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to set geometry: %w", err)
	}

	// Set max databases (use config or default)
	maxDbs := cfg.MDBXConfig.MaxDbs
	if maxDbs == 0 {
		maxDbs = 2
	}
	if err := env.SetOption(mdbx.OptMaxDB, uint64(maxDbs)); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to set max databases: %w", err)
	}

	// Set max readers (use config or default)
	maxReaders := cfg.MDBXConfig.MaxReaders
	if maxReaders == 0 {
		maxReaders = 128
	}
	if err := env.SetOption(mdbx.OptMaxReaders, uint64(maxReaders)); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to set max readers: %w", err)
	}

	// Build flags based on configuration
	flags := uint(mdbx.EnvDefaults)
	if cfg.MDBXConfig.NoSync {
		flags |= mdbx.UtterlyNoSync
	}
	if cfg.MDBXConfig.NoMetaSync {
		flags |= mdbx.NoMetaSync
	}
	if cfg.MDBXConfig.WriteMap {
		flags |= mdbx.WriteMap
	}
	if cfg.MDBXConfig.NoReadahead {
		flags |= mdbx.NoReadahead
	}
	if cfg.ReadOnly {
		flags |= mdbx.Readonly
	}

	// Open environment
	if err := env.Open(path, flags, 0644); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to open MDBX environment: %w", err)
	}

	// Open main database
	var db mdbx.DBI
	err = env.Update(func(txn *mdbx.Txn) error {
		var err error
		db, err = txn.OpenRoot(mdbx.Create)
		return err
	})
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &MDBXDatabase{
		env:  env,
		db:   db,
		path: path,
	}, nil
}

// Set stores a key-value pair in the database
func (d *MDBXDatabase) Set(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	start := time.Now()
	defer func() {
		d.metrics.WriteLatency = time.Since(start)
		d.metrics.WriteCount++
	}()

	err := d.env.Update(func(txn *mdbx.Txn) error {
		return txn.Put(d.db, key, value, 0)
	})

	if err != nil {
		d.metrics.WriteErrors++
		return fmt.Errorf("failed to set key: %w", err)
	}

	return nil
}

// Get retrieves a value by key from the database
func (d *MDBXDatabase) Get(key []byte) ([]byte, io.Closer, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, nil, fmt.Errorf("database is closed")
	}

	start := time.Now()
	defer func() {
		d.metrics.ReadLatency = time.Since(start)
		d.metrics.ReadCount++
	}()

	var value []byte
	err := d.env.View(func(txn *mdbx.Txn) error {
		val, err := txn.Get(d.db, key)
		if err != nil {
			return err
		}
		// Copy the value since it's only valid during the transaction
		value = make([]byte, len(val))
		copy(value, val)
		return nil
	})

	if err != nil {
		d.metrics.ReadErrors++
		if mdbx.IsNotFound(err) {
			return nil, nil, fmt.Errorf("key not found")
		}
		return nil, nil, fmt.Errorf("failed to get key: %w", err)
	}

	// Return a no-op closer since we copied the data
	return value, &noopCloser{}, nil
}

// Flush ensures all data is written to disk
func (d *MDBXDatabase) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("database is closed")
	}

	start := time.Now()
	defer func() {
		d.metrics.FlushLatency = time.Since(start)
		d.metrics.FlushCount++
	}()

	// Force synchronous flush
	err := d.env.Sync(true, false)
	if err != nil {
		d.metrics.FlushErrors++
		return fmt.Errorf("failed to flush: %w", err)
	}

	return nil
}

// Close closes the database
func (d *MDBXDatabase) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	// Close the environment (this also closes the database)
	d.env.Close()

	return nil
}

// GetMetrics returns database performance metrics
func (d *MDBXDatabase) GetMetrics() DatabaseMetrics {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Create a copy of metrics to avoid race conditions
	metrics := d.metrics

	// Add MDBX-specific metrics if available
	if !d.closed {
		if info, err := d.env.Info(nil); err == nil {
			metrics.DataSize = uint64(info.MapSize)
		}
		if stat, err := d.env.Stat(); err == nil {
			metrics.KeyCount = stat.Entries
		}
	}

	return metrics
}

// noopCloser is a no-op implementation of io.Closer
type noopCloser struct{}

func (n *noopCloser) Close() error {
	return nil
}