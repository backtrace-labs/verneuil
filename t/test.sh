#!/bin/bash

set -e

# Run this script in a sqlite build directory to run tests with the
# Verneuil VFS.

CURRENT="$PWD"

HERE=$(dirname $(readlink -f "$0"))

(cd "$HERE/..";
 cargo build  --release --target-dir "$CURRENT" --no-default-features --features 'verneuil_test_vfs')

OPTS="-DSQLITE_EXTRA_INIT=sqlite3_verneuil_test_only_register -DSQLITE_MAX_MMAP_SIZE=0 -DSQLITE_OMIT_WAL"

make clean

# Other interesting targets:
#  mptest: multi-process locks
#  fulltestonly, fulltest, soaktest: more extensive tests
#  valgrindtest
make "OPTS=$OPTS" "LIBS=release/libverneuil.a -lpthread -lm -ldl" test "$@"
