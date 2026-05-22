#!/bin/bash
# Runs inside the container: extracts the pre-built tar, installs plugins,
# starts the node, then drives crash-node.sh until OOM-kill.
set -uo pipefail

DIST_DIR=/opt/opensearch
OS_HOME="$DIST_DIR/opensearch"
NATIVE_LIB_DIR=/workspace/sandbox/libs/dataformat-native/rust/target/release

echo "[prebuilt] extracting distribution..."
tar -xzf /workspace/distribution/archives/linux-tar/build/distributions/opensearch-min-*-linux-x64.tar.gz \
  -C "$DIST_DIR" --strip-components=1
mv "$DIST_DIR/opensearch-"* "$OS_HOME" 2>/dev/null || OS_HOME="$DIST_DIR"
# If the tar extracts to a subdir, find it
if [ ! -f "$OS_HOME/bin/opensearch" ]; then
  OS_HOME=$(find "$DIST_DIR" -maxdepth 1 -type d -name "opensearch-*" | head -1)
fi
echo "[prebuilt] OS_HOME=$OS_HOME"

echo "[prebuilt] installing plugins..."
PLUGIN_CMD="$OS_HOME/bin/opensearch-plugin"

# Local plugins (sandbox + arrow)
for zip in \
  /workspace/plugins/arrow-base/build/distributions/arrow-base-*.zip \
  /workspace/plugins/arrow-flight-rpc/build/distributions/arrow-flight-rpc-*.zip \
  /workspace/sandbox/plugins/composite-engine/build/distributions/composite-engine-*.zip \
  /workspace/sandbox/plugins/parquet-data-format/build/distributions/parquet-data-format-*.zip \
  /workspace/sandbox/plugins/analytics-engine/build/distributions/analytics-engine-*.zip \
  /workspace/sandbox/plugins/analytics-backend-datafusion/build/distributions/analytics-backend-datafusion-*.zip \
  /workspace/sandbox/plugins/analytics-backend-lucene/build/distributions/analytics-backend-lucene-*.zip
do
  if ls $zip 1>/dev/null 2>&1; then
    echo "  -> $(basename $zip)"
    "$PLUGIN_CMD" install --batch "file://$zip" || true
  fi
done

# External plugins (job-scheduler, sql)
for zip in /workspace/scripts/docker-crash/plugins/*.zip; do
  if [ -f "$zip" ]; then
    echo "  -> $(basename $zip)"
    "$PLUGIN_CMD" install --batch "file://$zip" || true
  fi
done

echo "[prebuilt] configuring node..."
cat >> "$OS_HOME/config/opensearch.yml" <<'EOF'
cluster.name: crash-test
node.name: crash-node
discovery.type: single-node
network.host: 0.0.0.0
opensearch.experimental.feature.pluggable.dataformat.enabled: true
opensearch.experimental.feature.transport.stream.enabled: true
EOF

cat >> "$OS_HOME/config/jvm.options" <<EOF
-Xms512m
-Xmx512m
-Djava.library.path=$NATIVE_LIB_DIR
--add-opens=java.base/java.nio=ALL-UNNAMED
--enable-native-access=ALL-UNNAMED
-Dio.netty.allocator.numDirectArenas=1
-Dio.netty.noUnsafe=false
-Dio.netty.tryUnsafe=true
-Dio.netty.tryReflectionSetAccessible=true
-Dnative.heap_prof.allowed_paths=/workspace/scripts/docker-crash/profiles
EOF

echo "[prebuilt] starting OpenSearch node..."
"$OS_HOME/bin/opensearch" &
NODE_PID=$!

echo "[prebuilt] waiting for node on localhost:9200..."
for i in $(seq 1 120); do
  curl -s -o /dev/null "http://localhost:9200/" && break
  kill -0 "$NODE_PID" 2>/dev/null || { echo "[prebuilt] node exited before coming up"; exit 1; }
  sleep 5
done
curl -s "http://localhost:9200/" || { echo "[prebuilt] node never came up"; exit 1; }
echo "[prebuilt] node is UP"

# Activate jemalloc heap profiling via the CLI tool
PROF_DIR=/workspace/scripts/docker-crash/profiles
mkdir -p "$PROF_DIR"
echo "[prebuilt] activating jemalloc heap profiling..."
"$OS_HOME/bin/opensearch-heap-prof" start 2>&1 || echo "[prebuilt] WARNING: heap-prof start failed (profiling may not be compiled in)"

# If max_result_rows override is set, apply it dynamically
if [ -n "${MAX_RESULT_ROWS:-}" ]; then
  echo "[prebuilt] setting analytics.coordinator.max_result_rows=$MAX_RESULT_ROWS"
  curl -s -X PUT "http://localhost:9200/_cluster/settings" -H 'Content-Type: application/json' \
    -d "{\"persistent\":{\"analytics.coordinator.max_result_rows\":$MAX_RESULT_ROWS}}"
  echo
fi

# Background diagnostics: dump heap profile + thread dump + memory stats every 10s
# Find actual JVM PID (the opensearch launcher forks it)
sleep 5
JAVA_PID=$(pgrep -f "org.opensearch.bootstrap.OpenSearch" | head -1)
echo "[prebuilt] JVM PID=$JAVA_PID (launcher PID=$NODE_PID)"

(
  n=0
  while kill -0 "$JAVA_PID" 2>/dev/null; do
    sleep 10
    n=$((n+1))
    ts=$(date +%H:%M:%S)

    # Heap profile (jemalloc native allocations)
    "$OS_HOME/bin/opensearch-heap-prof" dump "$PROF_DIR/heap_${n}.prof" 2>/dev/null \
      && echo "[diag:$ts] heap_${n}.prof" || true

    # Thread dump (jstack)
    jstack "$JAVA_PID" > "$PROF_DIR/threads_${n}.txt" 2>/dev/null \
      && echo "[diag:$ts] threads_${n}.txt" || true

    # Memory stats snapshot
    {
      echo "=== snapshot $n at $ts ==="
      echo "--- /proc/$JAVA_PID/status ---"
      grep -E "VmRSS|VmSize|VmPeak|RssAnon|Threads" /proc/"$JAVA_PID"/status 2>/dev/null
      echo "--- cgroup memory ---"
      cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || true
      cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true
      echo "--- JVM heap ---"
      curl -s "http://localhost:9200/_nodes/stats/jvm?filter_path=nodes.*.jvm.mem" 2>/dev/null | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin); n=list(d.get('nodes',{}).values())[0]; mem=n['jvm']['mem']
  print(f'heap_used={mem[\"heap_used_in_bytes\"]//1048576}MB heap_max={mem[\"heap_max_in_bytes\"]//1048576}MB')
except: pass" 2>/dev/null || true
      echo
    } >> "$PROF_DIR/memstats.log"

  done
  echo "[diag] JVM exited — diagnostics loop done"
) &

echo "[prebuilt] starting load..."
INDEX=${INDEX:-crashidx} CONCURRENCY=${CONCURRENCY:-16} DOCS=${DOCS:-2000000} \
  CARDINALITY=${CARDINALITY:-2000000} ROUNDS=${ROUNDS:-0} \
  /workspace/scripts/crash-node.sh

# Final diagnostics
"$OS_HOME/bin/opensearch-heap-prof" dump "$PROF_DIR/heap_final.prof" 2>/dev/null || true
jstack "$NODE_PID" > "$PROF_DIR/threads_final.txt" 2>/dev/null || true
echo "[prebuilt] load script returned (node likely dead)"
echo "[prebuilt] diagnostics saved to $PROF_DIR/"
ls -lh "$PROF_DIR/" 2>/dev/null
