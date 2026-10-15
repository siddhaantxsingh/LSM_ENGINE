/* C API for lsm-engine.
 *
 * Conventions
 * - Every function that can fail returns an int status (LSM_OK, LSM_NOT_FOUND
 *   or a negative LSM_ERR_*). If `errmsg` is not NULL and the call fails, *errmsg
 *   receives a NUL-terminated message that the caller frees with
 *   lsm_free_string(). On success *errmsg is left untouched.
 * - Keys and values are (pointer, length) pairs. A NULL pointer is allowed
 *   only when the length is 0.
 * - Buffers returned by the library (values from lsm_get) are owned by the
 *   caller and must be released with lsm_free_buf(ptr, len).
 * - Handles (lsm_db, lsm_snapshot, lsm_batch, lsm_iter) are freed with their
 *   own *_free / lsm_close function. Passing NULL to any free function is a
 *   no-op. Snapshots and iterators stay valid after lsm_close (they keep the
 *   files they need alive) but should be freed before the process exits.
 * - A panic inside the library never unwinds into C: it is caught and
 *   reported as LSM_ERR_PANIC.
 * - All handles may be used from multiple threads, except that one lsm_iter
 *   or lsm_batch must not be used by two threads at the same time.
 */
#ifndef LSM_H
#define LSM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LSM_OK 0
#define LSM_NOT_FOUND 1
#define LSM_ERR_INVALID_ARGUMENT (-1)
#define LSM_ERR_IO (-2)
#define LSM_ERR_CORRUPTION (-3)
#define LSM_ERR_PANIC (-4)

typedef struct lsm_db lsm_db;
typedef struct lsm_snapshot lsm_snapshot;
typedef struct lsm_batch lsm_batch;
typedef struct lsm_iter lsm_iter;

/* Opens (creating if needed) the database in directory `path`.
 * sync != 0: every write is fsynced before it returns. */
int lsm_open(const char *path, int sync, lsm_db **out, char **errmsg);
/* Opens a database held entirely in memory (for tests). */
int lsm_open_in_memory(int sync, lsm_db **out, char **errmsg);
void lsm_close(lsm_db *db);

int lsm_put(lsm_db *db, const uint8_t *key, size_t key_len,
            const uint8_t *val, size_t val_len, char **errmsg);
int lsm_delete(lsm_db *db, const uint8_t *key, size_t key_len, char **errmsg);
/* LSM_OK: *val and *val_len hold a new buffer (free with lsm_free_buf).
 * LSM_NOT_FOUND: *val = NULL, *val_len = 0.
 * snap may be NULL (read the latest data). */
int lsm_get(lsm_db *db, const lsm_snapshot *snap, const uint8_t *key,
            size_t key_len, uint8_t **val, size_t *val_len, char **errmsg);
int lsm_flush(lsm_db *db, char **errmsg);
int lsm_compact(lsm_db *db, char **errmsg);

lsm_batch *lsm_batch_new(void);
int lsm_batch_put(lsm_batch *b, const uint8_t *key, size_t key_len,
                  const uint8_t *val, size_t val_len);
int lsm_batch_delete(lsm_batch *b, const uint8_t *key, size_t key_len);
/* Applies the batch atomically. The batch is not consumed; it may be
 * reused or freed afterwards. */
int lsm_write(lsm_db *db, const lsm_batch *b, char **errmsg);
void lsm_batch_free(lsm_batch *b);

lsm_snapshot *lsm_snapshot_new(lsm_db *db);
void lsm_snapshot_free(lsm_snapshot *s);

/* Iterates keys in [start, end). A NULL start or end means unbounded (use
 * a non-NULL pointer with length 0 for the empty key). */
int lsm_iter_new(lsm_db *db, const lsm_snapshot *snap,
                 const uint8_t *start, size_t start_len,
                 const uint8_t *end, size_t end_len,
                 lsm_iter **out, char **errmsg);
/* LSM_OK: *key and *val point into the iterator and stay valid until the next
 * call on this iterator. LSM_NOT_FOUND: end of range. Negative: a read
 * error stopped the scan (it is incomplete). */
int lsm_iter_next(lsm_iter *it, const uint8_t **key, size_t *key_len,
                  const uint8_t **val, size_t *val_len, char **errmsg);
void lsm_iter_free(lsm_iter *it);

void lsm_free_buf(uint8_t *buf, size_t len);
void lsm_free_string(char *s);

#ifdef __cplusplus
}
#endif
#endif
