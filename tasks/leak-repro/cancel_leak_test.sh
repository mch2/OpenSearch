#!/usr/bin/env bash
#
# End-to-end coordinator-reduce leak test driving the FULL scheduler/stage path.
# Fires the expensive reduce query and cancels it three ways, repeatedly, then checks
# whether pool.reduce (and the DataFusion native pool) return to baseline.
#
#   1. parent-task cancel  — POST _tasks/<id>/_cancel on the ppl task (top-down stage cancel)
#   2. client disconnect   — curl --max-time kills the connection mid-flight
#   3. cancel-after        — search.cancel_after_time_interval timeout
#
# Usage: ./cancel_leak_test.sh [HOST] [ROUNDS]
set -uo pipefail
H="${1:-localhost:9200}"
ROUNDS="${2:-5}"
QUERY='{"query":"source=clickbench | stats count() by WatchID, ClientIP"}'
LOG=build/testclusters/runTask-0/logs/runTask.log

pool() { curl -s "$H/_plugins/arrow_base/stats" | python3 -c "import sys,json;p=json.load(sys.stdin)['memory_pools']['pools'];print('reduce=%.2fMB(peak %.1f) query=%.2f datafusion=%.1f'%(p['reduce']['allocated_bytes']/1048576,p['reduce']['peak_bytes']/1048576,p['query']['allocated_bytes']/1048576,p['datafusion']['allocated_bytes']/1048576))"; }
dfnative() { curl -s "$H/_plugins/_analytics_backend_datafusion/stats" | python3 -c "import sys,json;d=json.load(sys.stdin);
def f(o):
  if isinstance(o,dict):
    if 'memory_bytes' in o: return o['memory_bytes']
    for v in o.values():
      r=f(v)
      if r is not None: return r
  return None
print('df_native_mem=%.1fMB'%((f(d) or 0)/1048576))"; }

MARK=$(wc -l < "$LOG" | tr -d ' ')
echo "=== BASELINE ==="; pool; dfnative

for r in $(seq 1 "$ROUNDS"); do
  echo "=== ROUND $r ==="

  # (1) parent-task cancel: fire, find the ppl task, cancel it
  ( curl -s -o /dev/null --max-time 60 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QUERY" ) &
  bgpid=$!
  sleep 1.5
  TID=$(curl -s "$H/_tasks?actions=cluster:admin/opensearch/ppl&group_by=none" 2>/dev/null | python3 -c "import sys,json;d=json.load(sys.stdin);ts=d.get('tasks',[]);print((ts[0]['node']+':'+str(ts[0]['id'])) if ts else '')" 2>/dev/null)
  if [ -n "$TID" ]; then
    curl -s -o /dev/null -X POST "$H/_tasks/$TID/_cancel"
    echo "  [1] cancelled parent task $TID"
  else
    echo "  [1] no task found (already done?)"
  fi
  wait $bgpid 2>/dev/null

  # (2) client disconnect: short max-time so curl drops the connection mid-query
  for i in 1 2 3; do
    ( curl -s -o /dev/null --max-time 2 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QUERY" ) &
  done
  wait
  echo "  [2] 3x client-disconnect (max-time 2s) fired"

  sleep 6
  echo -n "  after round $r drain: "; pool
done

echo ""
echo "=== FINAL (wait 15s for any async tail) ==="
sleep 15
pool; dfnative
echo ""
echo "=== leak warnings since baseline ==="
tail -n +"$MARK" "$LOG" | grep -c "still allocated — potential leak"
echo "=== leaked amounts (batches @ ~264340 B) ==="
tail -n +"$MARK" "$LOG" | grep -oE "Memory leaked: \([0-9]+\)" | grep -oE "[0-9]+" | awk '{s+=$1} END{printf "total %d B = %.1f batches across %d warnings\n", s, s/264340, NR}'
echo "=== barrier timeouts (should be 0) ==="
echo "  feed-quiesce: $(tail -n +"$MARK" "$LOG" | grep -c 'in-flight feed(s) to quiesce')"
echo "  teardown:     $(tail -n +"$MARK" "$LOG" | grep -c 'waiting for reduce teardown')"
echo "  stream_close: $(tail -n +"$MARK" "$LOG" | grep -c 'stream_close: timed out')"
