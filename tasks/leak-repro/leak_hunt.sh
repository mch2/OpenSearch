#!/usr/bin/env bash
# Aggressive multi-vector leak hunt against the per-query allocator pools.
# Vectors: oversampling on/off, expensive queries (big result + multi-agg), client cancel
# (curl --max-time), OOM via shrunk DF pool, server-side timeouts (cancel_after).
set -uo pipefail
H="${1:-localhost:9200}"
LOG=build/testclusters/runTask-0/logs/runTask.log
MARK=$(wc -l < "$LOG" | tr -d ' ')

# query shapes
QBIG='{"query":"source=clickbench | stats count() by WatchID, ClientIP"}'                                   # huge result, no head
QAGG='{"query":"source=clickbench | stats bucket_nullable=false count() as c, sum(IsRefresh), avg(ResolutionWidth) by WatchID, ClientIP | sort - c | head 10"}'
QURL='{"query":"source=clickbench | stats count() as c by URL | sort - c | head 20"}'
QDC='{"query":"source=clickbench | stats dc(WatchID) as u, count() as c by ClientIP | sort - c | head 50"}'

pools() { curl -s "$H/_plugins/arrow_base/stats" | python3 -c "import sys,json;p=json.load(sys.stdin)['memory_pools']['pools'];print('  reduce=%.1f(pk%.0f) query=%.1f(pk%.0f) df=%.0f'%(p['reduce']['allocated_bytes']/1048576,p['reduce']['peak_bytes']/1048576,p['query']['allocated_bytes']/1048576,p['query']['peak_bytes']/1048576,p['datafusion']['allocated_bytes']/1048576))"; }
setq() { curl -s -X PUT "$H/_cluster/settings" -H 'Content-Type: application/json' -d "$1" >/dev/null; }
fire() { curl -s -o /dev/null --max-time "${2:-40}" "$H/_plugins/_ppl" -H 'Content-Type: application/json' -d "$1"; }

echo "baseline:"; pools

# ---- Phase 1: oversampling ON + expensive aggregates, server timeout fires ----
echo "=== P1: oversampling=2.0, cancel_after=300ms, 8x QAGG ==="
setq '{"transient":{"analytics.shard_bucket_oversampling_factor":"2.0","search.cancel_after_time_interval":"300ms"}}'
for i in $(seq 1 8); do fire "$QAGG" & done; wait; sleep 4; pools

# ---- Phase 2: oversampling OFF + huge-result query, server timeout ----
echo "=== P2: oversampling=0, cancel_after=400ms, 8x QBIG (huge result) ==="
setq '{"transient":{"analytics.shard_bucket_oversampling_factor":"0.0","search.cancel_after_time_interval":"400ms"}}'
for i in $(seq 1 8); do fire "$QBIG" & done; wait; sleep 4; pools

# ---- Phase 3: client cancel (curl --max-time kills connection mid-query) ----
echo "=== P3: no server timeout; client disconnect at 1-3s, 12x mixed ==="
setq '{"transient":{"search.cancel_after_time_interval":null}}'
for i in $(seq 1 12); do
  q=$QAGG; [ $((i%2)) -eq 0 ] && q=$QBIG
  t=$(( (i%3) + 1 ))
  fire "$q" "$t" &
done; wait; sleep 5; pools

# ---- Phase 4: parent-task cancel mid-reduce ----
echo "=== P4: fire 6 QBIG, parent-cancel all after 3s ==="
for i in $(seq 1 6); do fire "$QBIG" 60 & done
sleep 3
curl -s -o /dev/null -X POST "$H/_tasks/_cancel?actions=cluster:admin/opensearch/ppl"
wait; sleep 5; pools

# ---- Phase 5: OOM — shrink DF pool so reduces exhaust mid-flight ----
echo "=== P5: DF pool -> 700MB, 10x QAGG+QDC concurrent (force CircuitBreaking) ==="
setq '{"transient":{"datafusion.memory_pool_limit_bytes": 734003200}}'
for i in $(seq 1 10); do q=$QAGG; [ $((i%2)) -eq 0 ] && q=$QDC; fire "$q" 40 & done; wait; sleep 5; pools

# ---- Phase 6: OOM + timeout together, oversampling on ----
echo "=== P6: DF 700MB + cancel_after 250ms + oversampling 4.0, 12x QDC ==="
setq '{"transient":{"analytics.shard_bucket_oversampling_factor":"4.0","search.cancel_after_time_interval":"250ms"}}'
for i in $(seq 1 12); do fire "$QDC" 40 & done; wait; sleep 5; pools

# ---- restore + settle ----
echo "=== restore pool + clear timeout, settle 25s ==="
setq '{"transient":{"datafusion.memory_pool_limit_bytes":null,"search.cancel_after_time_interval":null,"analytics.shard_bucket_oversampling_factor":"0.0"}}'
sleep 25
echo "FINAL:"; pools

echo ""
echo "=== TALLY since baseline ==="
echo "  cancellable fired : $(tail -n +"$MARK" "$LOG" | grep -c 'firing cancellable')"
echo "  CircuitBreaking   : $(tail -n +"$MARK" "$LOG" | grep -c CircuitBreak)"
echo "  TaskCancelled     : $(tail -n +"$MARK" "$LOG" | grep -c TaskCancelled)"
echo "  LEAK warnings     : $(tail -n +"$MARK" "$LOG" | grep -c 'Memory was leaked')"
echo "  late-releases     : $(tail -n +"$MARK" "$LOG" | grep -c 'allocator when allocator is closed')"
echo "  drain-timeouts    : $(tail -n +"$MARK" "$LOG" | grep -c 'waiting for allocator to drain')"
echo "  leaked bytes total: $(tail -n +"$MARK" "$LOG" | grep -oE 'Memory leaked: \([0-9]+\)' | grep -oE '[0-9]+' | awk '{s+=$1} END{print s+0}')"