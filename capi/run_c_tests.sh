#!/usr/bin/env bash
# Builds the C ABI library and runs the C test program three ways:
# plain, with AddressSanitizer + UBSan (on the C side, linked against the
# static library), and under valgrind (against the shared library).
# Usage: capi/run_c_tests.sh   (needs cc/clang; valgrind optional)
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release
LIB=target/release
CC=${CC:-cc}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "== plain (static lib)"
$CC -std=c11 -Wall -Wextra -Werror -O1 -Iinclude tests/c/test_lsm.c $LIB/liblsm.a -lpthread -ldl -lm -o "$tmp/test_plain"
"$tmp/test_plain" "$tmp/db_plain"

echo "== AddressSanitizer + UndefinedBehaviorSanitizer (C side)"
clang -std=c11 -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer -Iinclude \
  tests/c/test_lsm.c $LIB/liblsm.a -lpthread -ldl -lm -o "$tmp/test_asan"
ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 "$tmp/test_asan" "$tmp/db_asan"

if command -v valgrind >/dev/null; then
  echo "== valgrind (shared lib)"
  $CC -std=c11 -g -Iinclude tests/c/test_lsm.c -L$LIB -llsm -o "$tmp/test_dyn"
  LD_LIBRARY_PATH=$LIB valgrind --error-exitcode=1 --leak-check=full \
    --errors-for-leak-kinds=definite "$tmp/test_dyn" "$tmp/db_valgrind"
else
  echo "== valgrind not installed: skipped"
fi
