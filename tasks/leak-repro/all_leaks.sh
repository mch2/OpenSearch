#!/usr/bin/env bash
#
# Unified leak harness — covers all 4 known scenarios in one run:
#   #1 reduce-drain (Java Arrow)      — watch pools.query + "Memory was leaked" log
#   #2 shard-feed   (Java Arrow)      — same signals, client-disconnect path
#   #3 RepartitionExec aborted tasks  (Rust jemalloc) — cpu_runtime.num_alive_tasks
#                                       stuck >0 while idle + RSS climb run-over-run
#   #4 Flight send-buffer (Java Netty)— pools.flight residual after RST_STREAM
#
# Method: heavy multi-shard GROUP BY (forces RepartitionExec), cancelled 3 ways,
# repeated; after each round we settle then read every signal. The authoritative
# leak signals are: log "Memory was leaked" (Java), and num_alive_tasks>0-while-idle
# + monotonic RSS (Rust). Pool snapshots mid-run are in-flight borrows, not leaks —
# only the post-settle FINAL matters for Java pools.
set -uo pipefail
H="${1:-localhost:9200}"
ROUNDS="${2:-6}"
LOG=build/testclusters/runTask-0/logs/runTask.log

QBIG='{"query":"source=clickbench | stats count() by WatchID, ClientIP"}'           # huge result, multi-shard GROUP BY -> RepartitionExec
QDC='{"query":"source=clickbench | stats dc(WatchID) as u, count() as c by ClientIP | sort - c | head 50"}'

PID=$(curl -s -m5 "$H/_nodes/process" | python3 -c "import sys,json;d=json.load(sys.stdin);print(list(d['nodes'].values())[0]['process']['id'])" 2>/dev/null)
rss() { [ -n "$PID" ] && ps -o rss= -p "$PID" 2>/dev/null | awk '{printf "%.2f",$1/1048576}' || echo "?"; }

# Rust runtime signals (#3): alive tasks while idle is the leak tell; gate shows permits>workers.
native() {
  curl -s "$H/_plugins/_analytics_backend_datafusion/stats" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
n=list(d.get('nodes',{}).values())
if not n: print('  native: (no node)'); sys.exit()
n=n[0]; cpu=n.get('cpu_runtime',{}); g=n.get('fragment_executor_gate',{})
print('  cpu_runtime: workers=%s alive_tasks=%s spawned=%s | gate: max_permits=%s active=%s pending_batches=%s'%(
  cpu.get('workers_count'),cpu.get('num_alive_tasks'),cpu.get('spawned_tasks_count'),
  g.get('max_permits'),g.get('active_permits'),g.get('pending_acquire_batches')))
" 2>/dev/null || echo "  native: (unavailable)"
}
# Java Arrow pools (#1/#2/#4): only FINAL post-settle reading is authoritative.
pools() {
  curl -s "$H/_plugins/arrow_base/stats" 2>/dev/null | python3 -c "
import sys,json
p=json.load(sys.stdin)['memory_pools']['pools']
print('  pools(MB): reduce=%.1f query=%.1f flight=%.1f datafusion=%.0f'%(
  p['reduce']['allocated_bytes']/1048576,p['query']['allocated_bytes']/1048576,
  p['flight']['allocated_bytes']/1048576,p['datafusion']['allocated_bytes']/1048576))
" 2>/dev/null || echo "  pools: (unavailable)"
}

MARK=$(wc -l < "$LOG" | tr -d ' ')
echo "PID=$PID  ROUNDS=$ROUNDS  log_mark=$MARK"
echo "=== BASELINE (engine idle) ==="; echo "  RSS=$(rss)GB"; native; pools

for r in $(seq 1 "$ROUNDS"); do
  echo "=== ROUND $r ==="
  # (a) parent-task cancel mid-reduce
  ( curl -s -o /dev/null --max-time 60 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QBIG" ) & bg=$!
  sleep 1.5
  curl -s -o /dev/null -X POST "$H/_tasks/_cancel?actions=cluster:admin/opensearch/ppl"
  wait $bg 2>/dev/null
  # (b) client disconnect: short max-time drops connection mid-stream (RST_STREAM -> #2/#4)
  for i in 1 2 3 4; do ( curl -s -o /dev/null --max-time 2 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QBIG" ) & done
  wait
  # (c) heavier dc() mix, client disconnect
  for i in 1 2 3; do ( curl -s -o /dev/null --max-time 3 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QDC" ) & done
  wait
  sleep 7   # settle: let aborted tasks (if a worker is free) reach Drop, borrowers release
  echo "  [after settle] RSS=$(rss)GB"; native; pools
done

echo ""
echo "=== FINAL (idle 20s — this is the authoritative read) ==="
sleep 20
echo "  RSS=$(rss)GB"; native; pools
echo ""
echo "=== LOG TALLY since baseline ==="
echo "  Java 'Memory was leaked'   : $(tail -n +"$MARK" "$LOG" | grep -c 'Memory was leaked')"
echo "  Java leaked bytes total    : $(tail -n +"$MARK" "$LOG" | grep -oE 'Memory leaked: \([0-9]+\)' | grep -oE '[0-9]+' | awk '{s+=$1} END{print s+0}')"
echo "  'still allocated' (premature check, non-authoritative): $(tail -n +"$MARK" "$LOG" | grep -c 'still allocated')"
echo "  CircuitBreaking            : $(tail -n +"$MARK" "$LOG" | grep -c CircuitBreak)"
echo "  TaskCancelled              : $(tail -n +"$MARK" "$LOG" | grep -c TaskCancelled)"
echo ""
echo "INTERPRET: #1/#2/#4 leak iff FINAL pools stay elevated OR 'Memory was leaked'>0."
echo "           #3 leaks iff FINAL cpu_runtime.alive_tasks>0 while idle AND RSS climbed run-over-run."
