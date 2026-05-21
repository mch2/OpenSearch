#!/bin/bash
# Runs CoordinatorSingleNodeTopologyIT in a loop; when the reduce drain is detected
# stuck in native streamNext (the hang), captures full thread dumps of the cluster JVM
# and extracts the producer chain + all SEARCH threads. Goal: see WHY the shard-fragment
# producer never delivers input to the blocked reduce.
set -uo pipefail

OUT=/tmp/reduce-hang
rm -rf "$OUT"; mkdir -p "$OUT"

CARGO_TARGET_DIR=$HOME/.cargo-target-shared ./gradlew \
  :sandbox:qa:analytics-engine-coordinator:internalClusterTest \
  --tests 'org.opensearch.analytics.resilience.CoordinatorSingleNodeTopologyIT.testSingleNodeMultiShardSum' \
  -Dtests.iters=20 -PrustDebug > "$OUT/run.out" 2>&1 &
BG=$!

clusterPid() {
  for pid in $(jps -q 2>/dev/null); do
    jstack "$pid" 2>/dev/null | grep -q "opensearch\[node" && { echo "$pid"; return; }
  done
}

captured=0
while kill -0 "$BG" 2>/dev/null; do
  pid=$(clusterPid)
  if [ -n "${pid:-}" ]; then
    dump=$(jstack "$pid" 2>/dev/null)
    # Hang signature: reduce drain parked in native streamNext.
    if echo "$dump" | grep -q "DatafusionReduceSink.reduce"; then
      ts=$(date +%s)
      echo "$dump" > "$OUT/dump-$ts.txt"
      captured=$((captured+1))
      echo "[catch] stall dump #$captured captured (pid=$pid) at $(date +%H:%M:%S)"
      # Capture 3 dumps ~4s apart to prove it's STUCK (not transient), then we have enough.
      [ "$captured" -ge 3 ] && { echo "[catch] got 3 stall dumps — killing run"; kill "$BG" 2>/dev/null; break; }
      sleep 4
    else
      sleep 2
    fi
  else
    sleep 2
  fi
done
wait "$BG" 2>/dev/null

echo
echo "================ ANALYSIS ================"
D=$(ls -t "$OUT"/dump-*.txt 2>/dev/null | head -1)
if [ -z "${D:-}" ]; then echo "No stall dump captured (run may have passed all iters)."; exit 0; fi
echo "Analyzing: $D"
echo
echo "--- reduce drain (the blocked CONSUMER) ---"
grep -A20 "DatafusionReduceSink.reduce" "$D" | grep -E "streamNext|drainOutput|reduce\(|\[search\]|park|RUNNABLE|WAITING" | head -8
echo
echo "--- PRODUCER chain: is the shard fragment running/queued/blocked anywhere? ---"
grep -B2 -A12 -E "executeFragmentStreamingAsync|ShardTaskRunner|dispatchFragmentStreaming|registerStreamingFragmentHandler|PendingExecutions|send_blocking|df_sender_send" "$D" | head -60
echo
echo "--- ALL [search] thread states (how many blocked vs idle of the pool) ---"
awk '/^"opensearch\[node.*\[search\]/{name=$1} /java.lang.Thread.State:/{if(name){print name, $0; name=""}}' "$D" | sed -E 's/@[0-9]+//' | sort | uniq -c | sort -rn | head
echo
echo "--- threads mentioning the shard scan / fragment execution ---"
grep -E "^\"" "$D" | grep -iE "search|flight|transport|generic" | wc -l | xargs echo "search/flight/transport/generic thread count:"
echo
echo "--- any thread in send_blocking / partition sender (producer feeding the mpsc)? ---"
grep -c "send_blocking\|df_sender_send\|PartitionStreamSender" "$D" | xargs echo "send-side frames:"
echo "Full dumps in: $OUT/"
