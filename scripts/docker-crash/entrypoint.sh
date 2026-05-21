#!/bin/bash
# Runs inside the container: builds the linux native lib, starts the analytics
# node via `gradle run` (background), waits for it, then drives crash-node.sh
# until the cgroup OOM-killer kills the node (faithful to the EC2 crash).
set -uo pipefail
cd /workspace

# Container-local gradle home so we never corrupt the host's ~/.gradle. The rust
# build uses the DEFAULT in-tree target dir (rust/target/release) because
# run.gradle hardcodes java.library.path to exactly that path; the linux .so
# lands there alongside the macOS .dylib (different filenames, they coexist).
export GRADLE_USER_HOME=/workspace/.docker-gradle-home
NATIVE_LIB_DIR=/workspace/sandbox/libs/dataformat-native/rust/target/release

echo "[docker-crash] building linux native lib (release is the default)…"
./gradlew :sandbox:libs:dataformat-native:buildRustLibrary -Dsandbox.enabled=true --no-daemon

echo "[docker-crash] starting analytics node (gradle run)…"
./gradlew run -Dsandbox.enabled=true --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx512m \
  -PinstalledPlugins="['analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', 'analytics-backend-lucene', 'dsl-query-executor', 'composite-engine', 'test-ppl-frontend']" \
  -Dtests.jvm.argline="-Djava.library.path=$NATIVE_LIB_DIR -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true -Dopensearch.experimental.feature.transport.stream.enabled=true" \
  -x javadoc -x test -x missingJavadoc &
GRADLE_PID=$!

echo "[docker-crash] waiting for node on localhost:9200…"
for i in $(seq 1 120); do
  curl -s -o /dev/null "http://localhost:9200/" && break
  kill -0 "$GRADLE_PID" 2>/dev/null || { echo "[docker-crash] gradle run exited before node came up"; exit 1; }
  sleep 5
done
curl -s "http://localhost:9200/" >/dev/null || { echo "[docker-crash] node never came up"; exit 1; }
echo "[docker-crash] node is UP — starting load (this should grow native RSS until OOM-kill)"

# Hammer it. CONCURRENCY / DOCS / CARDINALITY overridable via -e on docker run.
INDEX=${INDEX:-crashidx} CONCURRENCY=${CONCURRENCY:-16} DOCS=${DOCS:-2000000} \
  CARDINALITY=${CARDINALITY:-2000000} ROUNDS=${ROUNDS:-0} \
  /workspace/scripts/crash-node.sh
echo "[docker-crash] load script returned (node likely dead — check OOMKilled on the host)"
