#!/bin/bash
# Build the toolchain image once, then run the analytics node + load inside a
# memory-limited Linux container. When native memory blows past --memory, the
# cgroup OOM-killer kills the node — the faithful EC2 crash repro.
#
# Usage:
#   ./scripts/docker-crash/run.sh [MEM] [CONCURRENCY]
#   MEM         container memory limit (default 6g). Lower it (3-4g) on a 2nd run
#               once the gradle/cargo caches are warm to crash faster.
#   CONCURRENCY simultaneous queries (default 16)
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
MEM="${1:-6g}"
CONCURRENCY="${2:-16}"
IMAGE=analytics-crash:latest

echo "[run] building toolchain image…"
docker build -t "$IMAGE" -f "$REPO/scripts/docker-crash/Dockerfile" "$REPO/scripts/docker-crash"

echo "[run] starting node + load: --memory=$MEM CONCURRENCY=$CONCURRENCY"
echo "      (first run compiles everything — slow; caches persist in .docker-gradle-home/.docker-cargo-target)"
set +e
docker run --rm --name analytics-crash \
  --memory="$MEM" --memory-swap="$MEM" \
  -e CONCURRENCY="$CONCURRENCY" \
  -v "$REPO":/workspace \
  "$IMAGE"
code=$?
set -e

echo
echo "[run] container exited (code=$code)."
echo "[run] OOM-killed? (this is the EC2-equivalent crash signal):"
# --rm removes the container, so capture state before exit isn't possible post-hoc;
# watch `docker events`/dmesg, or re-run without --rm and inspect:
echo "      re-run without --rm and check: docker inspect --format '{{.State.OOMKilled}}' analytics-crash"
echo "      or on the host VM: dmesg | grep -i 'killed process'"
