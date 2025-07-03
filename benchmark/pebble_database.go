package benchmark

import (
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog/log"
)

// PebbleDatabase implements the Database interface for Pebble
type PebbleDatabase struct {
	db    *pebble.DB
	cache *pebble.Cache
}

// NewPebbleDatabase creates a new Pebble database instance
func NewPebbleDatabase(cfg DatabaseConfig) (Database, error) {
	opts := &pebble.Options{}
	
	if cfg.ReadOnly {
		opts.ReadOnly = true
	}

	var cache *pebble.Cache
	if cfg.BlockCacheSize >= 0 {
		cache = pebble.NewCache(cfg.BlockCacheSize)
		opts.Cache = cache
		
		log.Info().
			Int64("block_cache_size", cfg.BlockCacheSize).
			Msg("Created Pebble with block cache")
	} else {
		log.Info().Msg("Created Pebble with block cache disabled")
	}

	db, err := pebble.Open(cfg.Path, opts)
	if err != nil {
		if cache != nil {
			cache.Unref()
		}
		return nil, err
	}

	return &PebbleDatabase{
		db:    db,
		cache: cache,
	}, nil
}

// Set implements Database.Set for Pebble
func (p *PebbleDatabase) Set(key, value []byte) error {
	return p.db.Set(key, value, pebble.NoSync)
}

// Get implements Database.Get for Pebble  
func (p *PebbleDatabase) Get(key []byte) ([]byte, io.Closer, error) {
	value, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil, ErrKeyNotFound
		}
		return nil, nil, err
	}
	return value, closer, nil
}

// Flush implements Database.Flush for Pebble
func (p *PebbleDatabase) Flush() error {
	return p.db.Flush()
}

// Close implements Database.Close for Pebble
func (p *PebbleDatabase) Close() error {
	var err error
	if p.db != nil {
		err = p.db.Close()
		p.db = nil
	}
	
	if p.cache != nil {
		p.cache.Unref()
		p.cache = nil
	}
	
	return err
}

// GetMetrics implements Database.GetMetrics for Pebble
func (p *PebbleDatabase) GetMetrics() DatabaseMetrics {
	metrics := DatabaseMetrics{
		BackendSpecific: make(map[string]interface{}),
	}

	if p.db == nil {
		return metrics
	}

	// Get Pebble metrics
	pebbleMetrics := p.db.Metrics()
	
	// Map Pebble metrics to common metrics
	metrics.MemTableSize = int64(pebbleMetrics.MemTable.Size)
	metrics.BytesRead = 0  // Will need to calculate from available metrics
	metrics.BytesWritten = 0  // Will need to calculate from available metrics  
	metrics.CompactionOps = pebbleMetrics.Compact.Count
	
	// Cache metrics (if cache is enabled)
	if p.cache != nil {
		cacheMetrics := p.cache.Metrics()
		metrics.CacheSize = cacheMetrics.Size
		metrics.CacheHits = cacheMetrics.Hits
		metrics.CacheMisses = cacheMetrics.Misses
	}

	// Store full Pebble metrics for detailed analysis
	metrics.BackendSpecific["pebble"] = map[string]interface{}{
		"flush":       pebbleMetrics.Flush,
		"compaction":  pebbleMetrics.Compact,
		"memtable":    pebbleMetrics.MemTable,
		"levels":      pebbleMetrics.Levels,
		"wal":         pebbleMetrics.WAL,
		"filter":      pebbleMetrics.Filter,
	}

	return metrics
}