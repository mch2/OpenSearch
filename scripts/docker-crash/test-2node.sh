#!/bin/bash
# Test: 2-node cluster under concurrent load.
# Both nodes are full (coordinator + data). Index is sharded across both.
# Queries hit node1 which coordinates and fans out to both nodes.
#
# Usage: ./scripts/docker-crash/test-2node.sh [MEMORY_PER_NODE] [CONCURRENCY] [SHARDS] [ROUNDS]
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
MEM="${1:-3g}"
CONCURRENCY="${2:-32}"
SHARDS="${3:-4}"
ROUNDS="${4:-30}"
NETWORK="analytics-test-net"
IMAGE="analytics-crash-prebuilt:latest"

echo "[2node] memory=$MEM/node concurrency=$CONCURRENCY shards=$SHARDS rounds=$ROUNDS"

# Cleanup
docker rm -f analytics-node1 analytics-node2 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true
docker network create "$NETWORK" 2>/dev/null

# Shared opensearch.yml config for cluster formation
CLUSTER_CONFIG="
cluster.name: crash-cluster
discovery.seed_hosts: [analytics-node1,analytics-node2]
cluster.initial_cluster_manager_nodes: [node1]
network.host: 0.0.0.0
opensearch.experimental.feature.pluggable.dataformat.enabled: true
opensearch.experimental.feature.transport.stream.enabled: true
"

# Start node1
echo "[2node] starting node1..."
docker run -d --name analytics-node1 --network "$NETWORK" \
  --memory="$MEM" --memory-swap="$MEM" \
  -e CONCURRENCY=1 -e ROUNDS=0 -e DOCS=1 -e SHARDS="$SHARDS" \
  -e NODE_NAME=node1 \
  -e CLUSTER_CONFIG="$CLUSTER_CONFIG" \
  -v "$REPO":/workspace \
  "$IMAGE"

# Start node2
echo "[2node] starting node2..."
docker run -d --name analytics-node2 --network "$NETWORK" \
  --memory="$MEM" --memory-swap="$MEM" \
  -e CONCURRENCY=1 -e ROUNDS=0 -e DOCS=1 -e SHARDS="$SHARDS" \
  -e NODE_NAME=node2 \
  -e CLUSTER_CONFIG="$CLUSTER_CONFIG" \
  -v "$REPO":/workspace \
  "$IMAGE"

# Wait for both nodes to come up
echo "[2node] waiting for cluster formation..."
for i in $(seq 1 120); do
  NODES=$(docker exec analytics-node1 curl -s "http://localhost:9200/_cat/nodes" 2>/dev/null | wc -l)
  [ "$NODES" -ge 2 ] && break
  sleep 3
done

NODES=$(docker exec analytics-node1 curl -s "http://localhost:9200/_cat/nodes" 2>/dev/null | wc -l)
if [ "$NODES" -lt 2 ]; then
  echo "[2node] FAILED: cluster did not form (only $NODES nodes)"
  docker logs analytics-node1 2>&1 | tail -20
  echo "---"
  docker logs analytics-node2 2>&1 | tail -20
  docker rm -f analytics-node1 analytics-node2 2>/dev/null
  docker network rm "$NETWORK" 2>/dev/null
  exit 1
fi
echo "[2node] cluster formed: $NODES nodes"
docker exec analytics-node1 curl -s "http://localhost:9200/_cat/nodes?v"

# Seed data (will distribute across both nodes)
echo "[2node] seeding $((SHARDS * 250000)) docs across $SHARDS shards..."
docker exec analytics-node1 bash -c "
  HOST=localhost:9200 INDEX=crashidx DOCS=500000 CARDINALITY=500000 SHARDS=$SHARDS CONCURRENCY=1 ROUNDS=0 \
    /workspace/scripts/crash-node.sh 2>&1 | grep -E 'seed|done'
" || true

# Verify shard distribution
echo "[2node] shard allocation:"
docker exec analytics-node1 curl -s "http://localhost:9200/_cat/shards/crashidx?v"

# Run load against node1 (coordinator)
echo "[2node] starting load: concurrency=$CONCURRENCY rounds=$ROUNDS"
docker exec analytics-node1 bash -c "
  HOST=localhost:9200 INDEX=crashidx CONCURRENCY=$CONCURRENCY ROUNDS=$ROUNDS SKIP_SEED=1 \
    /workspace/scripts/crash-node.sh
" 2>&1 | grep -E "round=|DEAD|Killed|query_failures"

# Check both nodes survived
echo
echo "[2node] === POST-TEST STATUS ==="
echo "node1:"
docker exec analytics-node1 curl -s "http://localhost:9200/" 2>/dev/null && echo " ALIVE" || echo " DEAD"
echo "node2:"
docker exec analytics-node2 curl -s "http://localhost:9200/" 2>/dev/null && echo " ALIVE" || echo " DEAD"

echo
echo "node1 RSS:"
docker exec analytics-node1 bash -c 'grep VmRSS /proc/$(pgrep -f "org.opensearch.bootstrap" | head -1)/status 2>/dev/null' || echo "N/A"
echo "node2 RSS:"
docker exec analytics-node2 bash -c 'grep VmRSS /proc/$(pgrep -f "org.opensearch.bootstrap" | head -1)/status 2>/dev/null' || echo "N/A"

# Cleanup
docker rm -f analytics-node1 analytics-node2 2>/dev/null
docker network rm "$NETWORK" 2>/dev/null
echo "[2node] done"
