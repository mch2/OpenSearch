/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link DfShardRouting#resolve} — the replica-failover core: picking the first LIVE
 * candidate node per shard (routing around a node whose Worker is down/absent), compacting used URLs,
 * and failing when a shard has no live copy. Exercised directly (no cluster) by constructing a
 * {@link DfShardRouting.Routing} with fake nodes + candidate lists.
 */
public class DfShardRoutingTests extends OpenSearchTestCase {

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Map.of(), java.util.Set.of(), org.opensearch.Version.CURRENT);
    }

    /** node idx 0,1 both live → each shard routes to its primary; shard map indexes the compacted URL list. */
    public void testAllLivePrimaries() {
        DfShardRouting.Routing routing = new DfShardRouting.Routing(
            List.of(node("n0"), node("n1")),
            List.of(new DfShardRouting.TableRouting("t", "uuid-t", new int[] { 0, 1 }, new int[][] { { 0 }, { 1 } }))
        );
        DfShardRouting.ResolvedRouting r = DfShardRouting.resolve(routing, Map.of(0, "http://n0:9400", 1, "http://n1:9400"));

        assertEquals(List.of("http://n0:9400", "http://n1:9400"), r.workerUrls());
        assertEquals("t:0:0\nt:1:1", r.shardMapCsv());
        assertEquals("t=uuid-t", r.indexUuidCsv());
    }

    /** Primary node (idx 0) is dead; each shard falls back to its replica (idx 1). Only n1's URL is used. */
    public void testFailoverToReplicaWhenPrimaryDead() {
        DfShardRouting.Routing routing = new DfShardRouting.Routing(
            List.of(node("n0"), node("n1")),
            // shard 0 candidates [primary n0, replica n1]; shard 1 candidates [primary n0, replica n1]
            List.of(new DfShardRouting.TableRouting("t", "uuid-t", new int[] { 0, 1 }, new int[][] { { 0, 1 }, { 0, 1 } }))
        );
        // Only n1 has a live Worker (n0 down / worker absent).
        DfShardRouting.ResolvedRouting r = DfShardRouting.resolve(routing, Map.of(1, "http://n1:9400"));

        assertEquals("only the live replica URL is used", List.of("http://n1:9400"), r.workerUrls());
        assertEquals("both shards route to worker idx 0 (the single used URL = n1)", "t:0:0\nt:1:0", r.shardMapCsv());
    }

    /** A shard whose every candidate node is dead fails the query clearly. */
    public void testNoLiveCopyFails() {
        DfShardRouting.Routing routing = new DfShardRouting.Routing(
            List.of(node("n0"), node("n1")),
            List.of(new DfShardRouting.TableRouting("t", "uuid-t", new int[] { 0 }, new int[][] { { 0, 1 } }))
        );
        // Neither candidate is live.
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> DfShardRouting.resolve(routing, Map.of()));
        assertTrue(e.getMessage(), e.getMessage().contains("no live data-node Worker"));
    }

    /** Cross-index join: two tables, differing shard layouts, each routes to its own live nodes. */
    public void testMultiTableRoutesPerTable() {
        DfShardRouting.Routing routing = new DfShardRouting.Routing(
            List.of(node("n0"), node("n1"), node("n2")),
            List.of(
                new DfShardRouting.TableRouting("l", "uuid-l", new int[] { 0, 1 }, new int[][] { { 0 }, { 1 } }),
                new DfShardRouting.TableRouting("r", "uuid-r", new int[] { 0, 1, 2 }, new int[][] { { 0 }, { 1 }, { 2 } })
            )
        );
        DfShardRouting.ResolvedRouting r = DfShardRouting.resolve(
            routing,
            Map.of(0, "http://n0:9400", 1, "http://n1:9400", 2, "http://n2:9400")
        );

        assertEquals(List.of("http://n0:9400", "http://n1:9400", "http://n2:9400"), r.workerUrls());
        assertEquals("l:0:0\nl:1:1\nr:0:0\nr:1:1\nr:2:2", r.shardMapCsv());
        assertEquals("l=uuid-l\nr=uuid-r", r.indexUuidCsv());
    }

    /** A replica preferred over a dead primary shares the same used-URL slot across shards on that node. */
    public void testUrlCompactionDedupesSharedNode() {
        DfShardRouting.Routing routing = new DfShardRouting.Routing(
            List.of(node("n0"), node("n1")),
            // 3 shards, all with primary n0 (dead) + replica n1 (live) → all land on n1, one URL.
            List.of(new DfShardRouting.TableRouting("t", "uuid-t", new int[] { 0, 1, 2 }, new int[][] { { 0, 1 }, { 0, 1 }, { 0, 1 } }))
        );
        DfShardRouting.ResolvedRouting r = DfShardRouting.resolve(routing, Map.of(1, "http://n1:9400"));
        assertEquals(1, r.workerUrls().size());
        assertEquals("t:0:0\nt:1:0\nt:2:0", r.shardMapCsv());
    }
}
