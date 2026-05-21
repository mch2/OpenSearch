#!/bin/bash
# Drives a running analytics node toward an OOM death by hammering it with
# concurrent high-cardinality GROUP BY queries (clickbench-q19 shape). Works
# against a local `gradle run` node (watch RSS grow) or a memory-limited
# Docker container (cgroup OOM-kill, faithful to EC2).
#
# Start the node first (one terminal):
#   ./gradlew run    # local, or run the tarball inside `docker run --memory=2g ...`
# Then (another terminal):
#   ./scripts/crash-node.sh
#
# Knobs (env vars):
#   HOST           node http endpoint            (default localhost:9200)
#   INDEX          index name                    (default crashidx)
#   DOCS           docs to seed                  (default 2000000)
#   CARDINALITY    distinct group keys           (default 2000000  → 1 group/doc)
#   CONCURRENCY    simultaneous queries/round    (default 16)
#   ROUNDS         rounds (0 = until node dies)  (default 0)
#   SKIP_SEED      reuse an existing index       (default unset)
set -uo pipefail
HOST=${HOST:-localhost:9200}
INDEX=${INDEX:-crashidx}
DOCS=${DOCS:-2000000}
CARDINALITY=${CARDINALITY:-2000000}
CONCURRENCY=${CONCURRENCY:-16}
ROUNDS=${ROUNDS:-0}

alive() { curl -s -o /dev/null -m 5 "http://$HOST/" ; }

seed() {
  echo "[seed] creating composite/parquet index $INDEX"
  curl -s -X DELETE "http://$HOST/$INDEX" >/dev/null 2>&1
  curl -s -X PUT "http://$HOST/$INDEX" -H 'Content-Type: application/json' -d '{
    "settings": {
      "number_of_shards": 2, "number_of_replicas": 0,
      "index.pluggable.dataformat.enabled": true,
      "index.pluggable.dataformat": "composite",
      "index.composite.primary_data_format": "parquet"
    },
    "mappings": { "properties": {
      "user_id": {"type":"long"}, "phrase": {"type":"keyword"}, "value": {"type":"integer"}
    }}
  }' >/dev/null
  echo "[seed] bulk-indexing $DOCS docs (cardinality=$CARDINALITY) ..."
  local b=0
  while [ "$b" -lt "$DOCS" ]; do
    {
      end=$((b+10000)); [ "$end" -gt "$DOCS" ] && end=$DOCS
      for ((i=b;i<end;i++)); do
        printf '{"index":{}}\n{"user_id":%d,"phrase":"p%d","value":7}\n' $((RANDOM*RANDOM%CARDINALITY)) $((RANDOM%50000))
      done
    } | curl -s -X POST "http://$HOST/$INDEX/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @- >/dev/null
    b=$end; printf '\r[seed] %d/%d' "$b" "$DOCS"
  done
  echo; curl -s -X POST "http://$HOST/$INDEX/_refresh" >/dev/null; curl -s -X POST "http://$HOST/$INDEX/_flush" >/dev/null
  echo "[seed] done"
}

# q19 shape: high-cardinality GROUP BY → huge coordinator-reduce hash table + result.
fire_one() {
  curl -s -o /dev/null -m 60 -X POST "http://$HOST/_analytics/ppl" -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=$INDEX | stats count() as c, sum(value) as s by user_id, phrase\"}"
}

rss_of_node() {
  # best-effort local RSS sampler (gradle-run JVM); for containers use `docker stats`.
  local pid; pid=$(jps -l 2>/dev/null | grep -i "Elasticsearch\|OpenSearch\|opensearch" | awk '{print $1}' | head -1)
  [ -n "$pid" ] && ps -o rss= -p "$pid" 2>/dev/null | awk '{printf "%.0fMB", $1/1024}'
}

[ -z "${SKIP_SEED:-}" ] && seed

echo "[load] hammering $INDEX: concurrency=$CONCURRENCY rounds=${ROUNDS:-∞}"
round=0
while :; do
  round=$((round+1))
  t0=$(date +%s%3N 2>/dev/null || date +%s000)
  pids=()
  for ((c=0;c<CONCURRENCY;c++)); do fire_one & pids+=($!); done
  fails=0; for p in "${pids[@]}"; do wait "$p" || fails=$((fails+1)); done
  t1=$(date +%s%3N 2>/dev/null || date +%s000)
  echo "[load] round=$round wall=$((t1-t0))ms query_failures=$fails node_rss=$(rss_of_node)"
  if ! alive; then
    echo "[load] *** NODE IS DEAD (http unreachable) after round $round *** — this is the crash."
    echo "       container? check: docker inspect --format '{{.State.OOMKilled}}' <id>"
    exit 1
  fi
  [ "$ROUNDS" -ne 0 ] && [ "$round" -ge "$ROUNDS" ] && { echo "[load] completed $ROUNDS rounds, node still alive"; exit 0; }
done
