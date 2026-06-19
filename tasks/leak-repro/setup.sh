#!/usr/bin/env bash
#
# One-time setup before reproducing the leak:
#   1. confirm clickbench is green
#   2. set an aggressive cancel-after timeout so heavy queries get cancelled
#   3. (optional) start native heap profiling   [RELEASE dylib only]
#
# Usage: ./setup.sh [HOST] [CANCEL_AFTER]
set -uo pipefail
HOST="${1:-localhost:9200}"
CANCEL_AFTER="${2:-200ms}"

echo "=== indices (expect clickbench green) ==="
curl -s "$HOST/_cat/indices?v"

echo
echo "=== set search.cancel_after_time_interval=$CANCEL_AFTER (forces TaskCancelledException) ==="
curl -s -X PUT "$HOST/_cluster/settings" -H 'Content-Type: application/json' -d "{
  \"transient\": { \"search.cancel_after_time_interval\": \"$CANCEL_AFTER\" }
}"
echo

echo "=== confirm REDUCE pool is present (the whole point) ==="
curl -s "$HOST/_plugins/arrow_base/stats" | python3 -m json.tool 2>/dev/null | grep -iE "reduce|query|flight|ingest" | head

echo
echo "Native heap profiling (RELEASE dylib only — NOT -PrustDebug). CLI auto-detects the PID:"
echo "  DISTRO=build/testclusters/runTask-0/distro/*/bin"
echo "  \$DISTRO/opensearch-heap-prof status     # confirm profiling available/active"
echo "  \$DISTRO/opensearch-heap-prof start       # activate"
echo "  \$DISTRO/opensearch-heap-prof dump /tmp/heap_baseline.prof"
echo "  # ...run repro_leak.sh, let queries drain..."
echo "  \$DISTRO/opensearch-heap-prof dump /tmp/heap_after_drain.prof"
echo "  \$DISTRO/opensearch-heap-prof stop"
echo "Thread profiling is always available via cpu_runtime stat (num_alive_tasks)."
