#!/bin/bash

set -e

# Run this script in a sqlite build directory to run tests with the
# Verneuil VFS.

CURRENT="$PWD"

HERE=$(dirname $(readlink -f "$0"))

# Default test features: validate after read and write transactions,
# and publish data to a local minio server.  That gives us a lot of
# coverage, but can be slow.
FEATURES="test_validate_all,test_random_chunk_action"

# Other options:
#
# Maximise test speed, no assertion.  Useful for mptest.
# FEATURES="test_vfs"
#
# Slightly lower test speed, some assertions.  Also useful for mptest,
# especially with minio disabled in VERNEUIL_CONFIG below.
# FEATURES="test_validate_writes"
#
# Restore some assertions.  Useful for soaktest.
# FEATURES="test_validate_writes"
#
# Run full assertions, without xattr support.
# FEATURES="test_validate_all,no_xattr"
#
# Make sure that we behave usefully with the default chunk action.
# FEATURES="test_validate_all"

(cd "$HERE/..";
 RUSTFLAGS=-g cargo build  --release --target-dir "$CURRENT" --no-default-features --features "$FEATURES")

# We want sqlite to call our test-only registration function, which
# will mark the verneuil VFS as the one, and pretend it's the Unix
# VFS.
#
# We also want to clearly disable mmap and the WAL: the VFS does not
# support them.
OPTS="-DSQLITE_EXTRA_INIT=sqlite3_verneuil_test_only_register -DSQLITE_MAX_MMAP_SIZE=0 -DSQLITE_OMIT_WAL"

# Finally, we replace calls to `malloc` with `calloc`: we want
# zero-filling because sqlite will read pages partially, then rewrite
# the whole thing to disk (e.g., for its rollback journal).  That not
# only persists garbage from the application's heap, but, more
# importantly for us, means that the bitwise representation of a DB
# may change in semantically irrelevant ways after a rollback.
# We avoid all that with calloc.
#
# We also set `-fcommon` because, in test mode, vfs.c redundantly
# defines counters like `sqlite3_sync_count`, and we need them to be
# merged instead of flagged as collisions.
CFLAGS="-g -O2 -fcommon -DSQLITE_OS_UNIX=1 -include '$HERE/../c/replace_malloc.h'"

if ! env | grep -iq eatmydata ;
then
    echo "Tests are much faster under eatmydata (https://github.com/stewartsmith/libeatmydata), or with tmpfs mounts for ./minio/, ./testdir/, and /tmp/verneuil-cache/";
fi

make clean

function cleanup() {
    # Don't fail the script if there's no container to remove.
    docker rm -f verneuil_test_minio 2>&1 | cat /dev/null
}

cleanup

trap cleanup EXIT

mkdir -p minio
rm -rf minio/*

# We use minio to spin up a local s3-compatible service on localhost:7777
#
# The data files in the data volume aren't as simple as they once were; the
# easiest way to see what's stored in the local buckets is probably
#
# $ docker run --security-opt label=disable -v ./escape-hatch:/out --net=host -it --entrypoint=/bin/sh docker.io/minio/mc
# sh-5.1# mc alias set verneuil http://localhost:7777 VERNEUIL_TEST_ACCOUNT VERNEUIL_TEST_KEY
# sh-5.1# mc ls verneuil  # etc.
EXTRA_DOCKER_ARGS=${EXTRA_DOCKER_ARGS:-}
if docker info | grep -q 'rootless: true' ; then
    # rootless, no need for fancy user mapping, and also can't assume
    # we can tweak our cpuset... but then we have to calm down selinux,
    # to let the container write to bind mounted directories.
    EXTRA_DOCKER_ARGS="$EXTRA_DOCKER_ARGS --replace --security-opt label=disable"
else
    EXTRA_DOCKER_ARG="$EXTRA_DOCKER_ARG --user $(id -u):$(id -g)"
    # If there are at least 4 CPUs on this machine, restrict minio to 2
    # cores: goodput scales negatively at some point around 256 cores.
    if grep -q '^processor\s*:\s*3$' /proc/cpuinfo ;
    then
        EXTRA_DOCKER_ARGS="$EXTRA_DOCKER_ARGS --cpuset-cpus=1-2"
    fi
fi

docker run --net=host $EXTRA_DOCKER_ARGS \
  --name verneuil_test_minio \
  -v $CURRENT/minio:/data \
  -e "MINIO_ROOT_USER=VERNEUIL_TEST_ACCOUNT" \
  -e "MINIO_ROOT_PASSWORD=VERNEUIL_TEST_KEY" \
  docker.io/minio/minio server --address 127.0.0.1:7777  /data &

# Leftover state shouldn't result in incorrect replication, but does trigger extra assertions
# that assume a clean initial state.
echo "About to run integration tests. Remember to clear leftover state under '/tmp/verneuil-*' if running with extra (read) validation."
sleep 5;

OFFLINE_CONFIG=$(cat <<'EOF'
{
    "make_default": true,
    "replication_spooling_dir": "/tmp",
    "replication_targets": []
}
EOF
)

# Other interesting targets:
#  mptest: multi-process locks
#  fulltestonly, fulltest, soaktest: more extensive tests
#  valgrindtest
export AWS_ACCESS_KEY_ID=VERNEUIL_TEST_ACCOUNT
export AWS_SECRET_ACCESS_KEY=VERNEUIL_TEST_KEY
export RUST_LOG=off  # Disable logging by default: some sqlite tests take over stdin/stdout.
export RUST_BACKTRACE=full # Dump a full backtrace on panic

export VERNEUIL_CONFIG="@${HERE}/test_with_minio.json"
# export VERNEUIL_CONFIG="$OFFLINE_CONFIG"  # Disable the replication target here.
make "OPTS=$OPTS" "CFLAGS=$CFLAGS" "LIBS=release/libverneuil.a -lpthread -lm -ldl" test "$@"
