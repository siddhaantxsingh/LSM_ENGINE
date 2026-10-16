/* End-to-end test of the C API: open on disk, writes, batches, snapshots,
 * iteration, error paths, reopen. Built and run by capi/run_c_tests.sh,
 * also under AddressSanitizer and valgrind. Exits non-zero on failure. */
#include "lsm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(cond)                                                        \
  do {                                                                     \
    if (!(cond)) {                                                         \
      fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
      failures++;                                                          \
    }                                                                      \
  } while (0)

static const uint8_t *B(const char *s) { return (const uint8_t *)s; }

static int get_str(lsm_db *db, const lsm_snapshot *snap, const char *k, char *out, size_t cap) {
  uint8_t *v = NULL;
  size_t n = 0;
  char *err = NULL;
  int rc = lsm_get(db, snap, B(k), strlen(k), &v, &n, &err);
  if (rc == LSM_OK) {
    size_t c = n < cap - 1 ? n : cap - 1;
    memcpy(out, v, c);
    out[c] = 0;
    lsm_free_buf(v, n);
  } else if (err) {
    lsm_free_string(err);
  }
  return rc;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <dir>\n", argv[0]);
    return 2;
  }
  const char *dir = argv[1];
  lsm_db *db = NULL;
  char *err = NULL;
  char buf[256];

  CHECK(lsm_open(dir, 1, &db, &err) == LSM_OK);
  CHECK(db != NULL);

  /* Basic put/get/delete. */
  CHECK(lsm_put(db, B("hello"), 5, B("world"), 5, &err) == LSM_OK);
  CHECK(get_str(db, NULL, "hello", buf, sizeof buf) == LSM_OK && strcmp(buf, "world") == 0);
  CHECK(lsm_delete(db, B("hello"), 5, &err) == LSM_OK);
  CHECK(get_str(db, NULL, "hello", buf, sizeof buf) == LSM_NOT_FOUND);

  /* Many keys, flush, compaction. */
  for (int i = 0; i < 5000; i++) {
    char k[32], v[64];
    snprintf(k, sizeof k, "key%06d", i);
    snprintf(v, sizeof v, "value-%d", i);
    CHECK(lsm_put(db, B(k), strlen(k), B(v), strlen(v), NULL) == LSM_OK);
  }
  CHECK(lsm_flush(db, &err) == LSM_OK);

  /* Snapshot isolation. */
  lsm_snapshot *snap = lsm_snapshot_new(db);
  CHECK(snap != NULL);
  lsm_batch *b = lsm_batch_new();
  CHECK(lsm_batch_put(b, B("key000001"), 9, B("changed"), 7) == LSM_OK);
  CHECK(lsm_batch_delete(b, B("key000002"), 9) == LSM_OK);
  CHECK(lsm_write(db, b, &err) == LSM_OK);
  lsm_batch_free(b);
  CHECK(lsm_compact(db, &err) == LSM_OK);
  CHECK(get_str(db, NULL, "key000001", buf, sizeof buf) == LSM_OK && strcmp(buf, "changed") == 0);
  CHECK(get_str(db, NULL, "key000002", buf, sizeof buf) == LSM_NOT_FOUND);
  CHECK(get_str(db, snap, "key000001", buf, sizeof buf) == LSM_OK && strcmp(buf, "value-1") == 0);
  CHECK(get_str(db, snap, "key000002", buf, sizeof buf) == LSM_OK && strcmp(buf, "value-2") == 0);

  /* Range iteration [key000010, key000020) at the snapshot. */
  lsm_iter *it = NULL;
  CHECK(lsm_iter_new(db, snap, B("key000010"), 9, B("key000020"), 9, &it, &err) == LSM_OK);
  int count = 0, rc;
  const uint8_t *k, *v;
  size_t kl, vl;
  while ((rc = lsm_iter_next(it, &k, &kl, &v, &vl, &err)) == LSM_OK) {
    char want[32];
    snprintf(want, sizeof want, "key%06d", 10 + count);
    CHECK(kl == strlen(want) && memcmp(k, want, kl) == 0);
    count++;
  }
  CHECK(rc == LSM_NOT_FOUND);
  CHECK(count == 10);
  lsm_iter_free(it);
  lsm_snapshot_free(snap);

  /* Error paths: NULL handles and NULL pointers with non-zero length. */
  err = NULL;
  CHECK(lsm_put(NULL, B("a"), 1, B("b"), 1, &err) == LSM_ERR_INVALID_ARGUMENT);
  CHECK(err != NULL);
  lsm_free_string(err);
  err = NULL;
  CHECK(lsm_put(db, NULL, 3, B("b"), 1, &err) == LSM_ERR_INVALID_ARGUMENT);
  lsm_free_string(err);
  uint8_t *val = (uint8_t *)1;
  size_t vlen = 99;
  CHECK(lsm_get(db, NULL, B("missing"), 7, &val, &vlen, NULL) == LSM_NOT_FOUND);
  CHECK(val == NULL && vlen == 0);
  CHECK(lsm_get(db, NULL, B("x"), 1, NULL, &vlen, NULL) == LSM_ERR_INVALID_ARGUMENT);
  CHECK(lsm_iter_new(db, NULL, NULL, 4, NULL, 0, &it, NULL) == LSM_ERR_INVALID_ARGUMENT);
  lsm_close(NULL);
  lsm_iter_free(NULL);
  lsm_batch_free(NULL);
  lsm_snapshot_free(NULL);
  lsm_free_buf(NULL, 0);
  lsm_free_string(NULL);

  /* Reopen: data is durable. */
  lsm_close(db);
  db = NULL;
  CHECK(lsm_open(dir, 0, &db, &err) == LSM_OK);
  CHECK(get_str(db, NULL, "key004999", buf, sizeof buf) == LSM_OK && strcmp(buf, "value-4999") == 0);
  CHECK(get_str(db, NULL, "key000001", buf, sizeof buf) == LSM_OK && strcmp(buf, "changed") == 0);
  lsm_close(db);

  if (failures) {
    fprintf(stderr, "%d check(s) failed\n", failures);
    return 1;
  }
  printf("C API test: all checks passed\n");
  return 0;
}
