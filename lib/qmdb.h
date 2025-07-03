#ifndef QMDB_H
#define QMDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle for QMDB database instance
typedef struct QMDBHandle QMDBHandle;

// Error codes
#define QMDB_OK 0
#define QMDB_ERROR -1
#define QMDB_NOT_FOUND -2
#define QMDB_INVALID_PARAM -3

// Database metrics structure
typedef struct {
    uint64_t entries_count;
    uint64_t total_size_bytes;
    uint64_t cache_size_bytes;
    uint64_t cache_hits;
    uint64_t cache_misses;
} QMDBMetrics;

// Open a QMDB database instance
// path: Path to the database directory
// Returns: Handle to the database instance, or NULL on error
QMDBHandle* qmdb_open(const char* path);

// Set a key-value pair
// handle: Database handle
// key_ptr: Pointer to key data
// key_len: Length of key in bytes
// value_ptr: Pointer to value data  
// value_len: Length of value in bytes
// Returns: QMDB_OK on success, error code on failure
int qmdb_set(QMDBHandle* handle, const uint8_t* key_ptr, size_t key_len, 
             const uint8_t* value_ptr, size_t value_len);

// Get a value for a key
// handle: Database handle
// key_ptr: Pointer to key data
// key_len: Length of key in bytes
// value_ptr: Pointer to buffer for value data
// value_len: In/out parameter - input: buffer size, output: actual value size
// Returns: QMDB_OK on success, QMDB_NOT_FOUND if key not found, error code on failure
int qmdb_get(QMDBHandle* handle, const uint8_t* key_ptr, size_t key_len,
             uint8_t* value_ptr, size_t* value_len);

// Flush pending operations to storage
// handle: Database handle  
// Returns: QMDB_OK on success, error code on failure
int qmdb_flush(QMDBHandle* handle);

// Close the database and free resources
// handle: Database handle
// Returns: QMDB_OK on success, error code on failure
int qmdb_close(QMDBHandle* handle);

// Get database metrics
// handle: Database handle
// metrics: Pointer to metrics structure to fill
// Returns: QMDB_OK on success, error code on failure
int qmdb_get_metrics(QMDBHandle* handle, QMDBMetrics* metrics);

// Get library version
// Returns: Version string (static, do not free)
const char* qmdb_version(void);

#ifdef __cplusplus
}
#endif

#endif // QMDB_H