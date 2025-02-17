/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.allocation;

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaFilteringAllocationIT extends RemoteStoreBaseIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public void testSearchReplicaDedicatedIncludes() {
        List<String> nodesIds = internalCluster().startNodes(3);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        final String node_2 = nodesIds.get(2);
        assertEquals(3, cluster().size());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node_1 + "," + node_0)
            )
            .execute()
            .actionGet();

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen("test");
        // ensure primary is not on node 0 or 1,
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(node_2, getNodeName(routingTable.primaryShard().currentNodeId()));

        String existingSearchReplicaNode = getNodeName(routingTable.searchOnlyReplicas().get(0).currentNodeId());
        String emptyAllowedNode = existingSearchReplicaNode.equals(node_0) ? node_1 : node_0;

        // set the included nodes to the other open node, search replica should relocate to that node.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", emptyAllowedNode))
            .execute()
            .actionGet();
        ensureGreen("test");

        routingTable = getRoutingTable();
        assertEquals(node_2, getNodeName(routingTable.primaryShard().currentNodeId()));
        assertEquals(emptyAllowedNode, getNodeName(routingTable.searchOnlyReplicas().get(0).currentNodeId()));
    }

    public void testSearchReplicaDedicatedIncludes_DoNotAssignToOtherNodes() {
        List<String> nodesIds = internalCluster().startNodes(3);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        final String node_2 = nodesIds.get(2);
        assertEquals(3, cluster().size());

        // set filter on 1 node and set search replica count to 2 - should leave 1 unassigned
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", node_1))
            .execute()
            .actionGet();

        logger.info("--> creating an index with no replicas");
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards("test");
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(2, routingTable.searchOnlyReplicas().size());
        List<ShardRouting> assignedSearchShards = routingTable.searchOnlyReplicas()
            .stream()
            .filter(ShardRouting::assignedToNode)
            .collect(Collectors.toList());
        assertEquals(1, assignedSearchShards.size());
        assertEquals(node_1, getNodeName(assignedSearchShards.get(0).currentNodeId()));
        assertEquals(1, routingTable.searchOnlyReplicas().stream().filter(ShardRouting::unassigned).count());
    }

    private ClusterAllocationExplanation runExplain(boolean primary, boolean includeYesDecisions, boolean includeDiskInfo)
        throws Exception {

        return runExplain(primary, null, includeYesDecisions, includeDiskInfo);
    }

    private ClusterAllocationExplanation runExplain(boolean primary, String nodeId, boolean includeYesDecisions, boolean includeDiskInfo)
        throws Exception {

        ClusterAllocationExplanation explanation = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("test")
            .setShard(0)
            .setPrimary(false)
            .setIncludeYesDecisions(includeYesDecisions)
            .setIncludeDiskInfo(includeDiskInfo)
            .setCurrentNode(nodeId)
            .get()
            .getExplanation();
//        if (logger.isDebugEnabled()) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.prettyPrint();
            builder.humanReadable(true);
            logger.info("--> explain json output: \n{}", explanation.toXContent(builder, ToXContent.EMPTY_PARAMS).toString());
//        }
        return explanation;
    }

    public void testSearchReplicaWithDedicatedNodesAndAwarenessAttributes() throws Exception {
        Settings commonSettings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build();
        Settings dedicatedSRNodeSettings = Settings.builder().put(commonSettings).put("node.attr.shard_type", "search").build();

        logger.info("--> starting 2 nodes on the same rack");
        String node_0 = internalCluster().startNode(Settings.builder().put(dedicatedSRNodeSettings).put("node.attr.zone", "zone_1").put("node.name", "node_0").build());
        String node_1 = internalCluster().startNode(Settings.builder().put(dedicatedSRNodeSettings).put("node.attr.zone", "zone_2").put("node.name", "node_1").build());
        String node_2 = internalCluster().startNode(Settings.builder().put(dedicatedSRNodeSettings).put("node.attr.zone", "zone_3").put("node.name", "node_2").build());


        String node_3 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "zone_1").put("node.name", "node_3").build());
        String node_4 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "zone_2").put("node.name", "node_4").build());
        String node_5 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "zone_3").put("node.name", "node_5").build());

        Set<String> searchOnlyNodes = Set.of(node_0, node_1, node_2);
        Set<String> writerNodes = Set.of(node_3, node_4, node_5);

        Set<String> searchOnlyNode_ids = searchOnlyNodes.stream().map(this::getNodeId).collect(Collectors.toSet());
        Set<String> writerNode_ids = writerNodes.stream().map(this::getNodeId).collect(Collectors.toSet());

        System.out.println("SEARCH NODE IDS: " + searchOnlyNode_ids);
        System.out.println("WRITE NODE IDS: " + writerNode_ids);

        assertEquals(6, cluster().size());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "shard_type", "search")
            )
            .execute()
            .actionGet();

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1) // one shard per zone
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 3)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards("test");

//        ClusterAllocationExplanation explanation = runExplain(true, false, false);

//        System.out.println(explanation);
        ensureGreen("test");
        IndexShardRoutingTable routingTable = getRoutingTable();

        Set<String> allocatedSearchNodes = routingTable.searchOnlyReplicas().stream()
            .filter(r -> r.currentNodeId() != null)
            .map(r -> getNodeName(r.currentNodeId()))
            .collect(Collectors.toSet());

        System.out.println(routingTable);
        // all search nodes are allocated
        assertEquals(allocatedSearchNodes, searchOnlyNodes);

        Set<String> allocatedWriters = routingTable.writerReplicas().stream()
            .filter(r -> r.currentNodeId() != null)
            .map(r -> getNodeName(r.currentNodeId()))
            .collect(Collectors.toSet());

        System.out.println(allocatedWriters);

        // only one of two writer replicas assigned
        assertEquals(2, allocatedWriters.size());

        System.out.println("SEARCH REPLICAS");
        for (ShardRouting shardRouting : routingTable.searchOnlyReplicas()) {
            System.out.println(shardRouting);
        }

        System.out.println("WRITE REPLICAS");
        for (ShardRouting shardRouting : routingTable.writerReplicas()) {
            System.out.println(shardRouting);
        }
        System.out.println(allocatedWriters);
        assertEquals(1, allocatedWriters.size());
    }

    private IndexShardRoutingTable getRoutingTable() {
        IndexShardRoutingTable routingTable = getClusterState().routingTable().index("test").getShards().get(0);
        return routingTable;
    }

    private String getNodeName(String id) {
        return getClusterState().nodes().get(id).getName();
    }

    private String getNodeId(String name) {
        return getClusterState().nodes().resolveNode(name).getId();
    }
}
