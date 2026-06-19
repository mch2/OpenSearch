#!/usr/bin/env bash
#
# Verify the RepartitionExec/gate-permit cancel-leak fix on the running node.
#
# Pre-fix signal (sustained no-settle cancel load): fragment_executor_gate.active_permits
# stays pinned at max while the engine is idle, and the datafusion pool never drains —
# the cancelled queries' gate permits were stranded on the QueryStreamHandle until a
# stream_close that the cancel path skipped.
#
# Post-fix: cancel_query releases the permit synchronously, so after the idle hold
# active_permits → 0 and the datafusion pool → 0.
set -uo pipefail
H="${1:-localhost:9200}"
QBIG='{"query":"source=clickbench | stats count() by WatchID, ClientIP"}'
PID=$(curl -s -m5 "$H/_nodes/process" | python3 -c "import sys,json;print(list(json.load(sys.stdin)['nodes'].values())[0]['process']['id'])" 2>/dev/null)

alive(){ curl -s "$H/_plugins/_analytics_backend_datafusion/stats"|python3 -c "import sys,json;n=list(json.load(sys.stdin)['nodes'].values())[0];c=n['cpu_runtime'];g=n['fragment_executor_gate'];print('alive=%s spawned=%s active_permits=%s/%s pending=%s'%(c['num_alive_tasks'],c['spawned_tasks_count'],g['active_permits'],g['max_permits'],g['pending_acquire_batches']))";}
df(){ curl -s "$H/_plugins/arrow_base/stats"|python3 -c "import sys,json;p=json.load(sys.stdin)['memory_pools']['pools'];print('df=%.0fMB query=%.1f'%(p['datafusion']['allocated_bytes']/1048576,p['query']['allocated_bytes']/1048576))";}
rss(){ [ -n "$PID" ] && ps -o rss= -p "$PID" 2>/dev/null|awk '{printf "%.2fGB",$1/1048576}' || echo "?";}

echo "PID=$PID"
echo "BASELINE: $(alive) $(df) RSS=$(rss)"
echo "=== SUSTAINED: 5 waves of 8 concurrent QBIG, cancel-all after 1.2s, NO settle ==="
for w in 1 2 3 4 5; do
  for i in $(seq 1 8); do ( curl -s -o /dev/null --max-time 1.2 "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$QBIG" ) & done
  sleep 0.4
  curl -s -o /dev/null -X POST "$H/_tasks/_cancel?actions=cluster:admin/opensearch/ppl"
  wait
  echo "  wave $w: $(alive) $(df) RSS=$(rss)"
done
echo "=== hold idle, watch drain (THE TEST): +5s, +15s, +30s ==="
sleep 5;  echo "  +5s:  $(alive) $(df) RSS=$(rss)"
sleep 10; echo "  +15s: $(alive) $(df) RSS=$(rss)"
sleep 15; echo "  +30s: $(alive) $(df) RSS=$(rss)"
echo ""
echo "PASS iff +30s shows active_permits=0/N and df=0MB (was active_permits pinned + df multi-GB)."
