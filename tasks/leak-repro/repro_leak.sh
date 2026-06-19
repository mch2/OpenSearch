#!/usr/bin/env bash
#
# Reproduce the cancel/timeout native-memory leak and isolate coordinator vs data-node.
#
# Heavy reduce query (high-cardinality 2-key group-by -> group_aggregate_batch path):
#   source=clickbench | stats count() by WatchID, ClientIP
#
# Fires N concurrent copies with a short server-side timeout so most get cancelled,
# then snapshots memory before / during / after drain. The key question: after traffic
# stops, does memory return to baseline, and WHICH pool holds?
#   - pool.reduce elevated  -> coordinator reduce path
#   - pool.query  elevated  -> data-node Arrow scan path
#   - analytics_backend (DataFusion Rust pool) elevated -> data-node native pool
#
# Usage: ./repro_leak.sh [HOST] [CONCURRENCY] [TIMEOUT_MS]
set -uo pipefail

HOST="${1:-localhost:9200}"
CONC="${2:-8}"
TIMEOUT_MS="${3:-200}"          # short -> forces TaskCancelledException
QUERY='{"query":"source=clickbench | stats count() by WatchID, ClientIP"}'
OUT="$(dirname "$0")/run-$(date +%H%M%S)"
mkdir -p "$OUT"

echo "host=$HOST  concurrency=$CONC  timeout=${TIMEOUT_MS}ms  out=$OUT"

# --- snapshot helpers -------------------------------------------------------
snap_arrow()  { curl -s "$HOST/_plugins/arrow_base/stats" ; }
snap_df()     { curl -s "$HOST/_plugins/_analytics_backend_datafusion/stats" ; }
# cpu_runtime carries num_alive_tasks (thread profiling)
snap_cpu()    { curl -s "$HOST/_plugins/_analytics_backend_datafusion/stats/cpu_runtime" ; }

snapshot() {  # $1 = label
  local label="$1"
  echo "----- snapshot: $label ($(date +%T)) -----" | tee -a "$OUT/timeline.txt"
  snap_arrow > "$OUT/${label}.arrow.json"
  snap_df    > "$OUT/${label}.df.json"
  snap_cpu   > "$OUT/${label}.cpu.json"
  # compact one-liner for the timeline: pool maxes/used + datafusion used + alive tasks
  python3 - "$OUT/${label}.arrow.json" "$OUT/${label}.df.json" "$OUT/${label}.cpu.json" >> "$OUT/timeline.txt" 2>/dev/null <<'PY'
import json,sys
def load(p):
    try: return json.load(open(p))
    except: return {}
arrow,df,cpu = load(sys.argv[1]),load(sys.argv[2]),load(sys.argv[3])
print("  arrow:", json.dumps(arrow)[:600])
print("  df   :", json.dumps(df)[:600])
print("  cpu  :", json.dumps(cpu)[:600])
PY
}

# --- fire one query, record HTTP status + error type ------------------------
fire() {  # $1 = index
  local i="$1"
  local body
  # Cancellation is driven by the cluster setting search.cancel_after_time_interval
  # (see setup.sh). The ?timeout= param is also sent as a belt-and-suspenders signal.
  body=$(curl -s -w '\nHTTP %{http_code} t=%{time_total}s' \
    -H 'Content-Type: application/json' \
    "$HOST/_plugins/_ppl?timeout=${TIMEOUT_MS}ms" \
    -d "$QUERY")
  printf '%s\n%s\n\n' "[q$i]" "$body" >> "$OUT/responses.txt"
}

# --- run --------------------------------------------------------------------
snapshot baseline

echo "firing $CONC concurrent queries..."
for i in $(seq 1 "$CONC"); do fire "$i" & done

# snapshot mid-flight (during), then wait for all to finish
sleep 0.3
snapshot during
wait
echo "all queries returned."

snapshot t_plus_0
sleep 30;  snapshot t_plus_30
sleep 60;  snapshot t_plus_90
sleep 160; snapshot t_plus_250

echo
echo "=== cancellation / error summary ==="
grep -hoE 'HTTP [0-9]+|TaskCancelled|cancelled|timed? ?out' "$OUT/responses.txt" | sort | uniq -c
echo
echo "=== drain check: did pools return to baseline? (eyeball timeline.txt) ==="
echo "report dir: $OUT"
