/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service responsible for applying backpressure for lagging behind replicas when Segment Replication is enabled.
 *
 * @opensearch.internal
 */
public class SegmentReplicationPressureService {

    private volatile boolean isSegmentReplicationBackpressureEnabled;
    private volatile int maxCheckpointsBehind;
    private volatile double maxAllowedStaleReplicas;
    private volatile TimeValue maxReplicationTime;

    private static final Logger logger = LogManager.getLogger(SegmentReplicationPressureService.class);

    public static final Setting<Boolean> SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED = Setting.boolSetting(
        "index.segrep.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_INDEXING_CHECKPOINTS = Setting.intSetting(
        "index.segrep.pressure.checkpoint.limit",
        4,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MAX_REPLICATION_TIME_SETTING = Setting.positiveTimeSetting(
        "index.segrep.pressure.time.limit",
        TimeValue.timeValueMinutes(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> MAX_ALLOWED_STALE_SHARDS = Setting.doubleSetting(
        "index.segrep.replica.stale.limit",
        .5,
        0,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final IndicesService indicesService;
    private final ShardStateAction shardStateAction;
    private final SegmentReplicationStatsTracker tracker;

    @Inject
    public SegmentReplicationPressureService(Settings settings, ClusterService clusterService, IndicesService indicesService, ShardStateAction shardStateAction) {
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.tracker = new SegmentReplicationStatsTracker(this.indicesService);

        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.isSegmentReplicationBackpressureEnabled = SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED,
            this::setSegmentReplicationBackpressureEnabled
        );

        this.maxCheckpointsBehind = MAX_INDEXING_CHECKPOINTS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEXING_CHECKPOINTS, this::setMaxCheckpointsBehind);

        this.maxReplicationTime = MAX_REPLICATION_TIME_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_REPLICATION_TIME_SETTING, this::setMaxReplicationTime);

        this.maxAllowedStaleReplicas = MAX_ALLOWED_STALE_SHARDS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_ALLOWED_STALE_SHARDS, this::setMaxAllowedStaleReplicas);
    }

    public void isSegrepLimitBreached(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        if (isSegmentReplicationBackpressureEnabled && shard.indexSettings().isSegRepEnabled() && shard.routingEntry().primary()) {
            validateReplicationGroup(shard);
        }
    }

    private void validateReplicationGroup(IndexShard shard) {
        final Set<SegmentReplicationShardStats> replicaStatus = shard.getReplicationStats();
        final List<SegmentReplicationShardStats> staleReplicas = getStaleReplicas(replicaStatus);
        if (staleReplicas.isEmpty() == false) {
            // inSyncIds always considers the primary id, so filter it out.
            final float percentStale = staleReplicas.size() * 100f / (shard.getReplicationGroup().getInSyncAllocationIds().size() - 1);
            final double maxStaleLimit = maxAllowedStaleReplicas * 100f;
            if (percentStale >= maxStaleLimit) {
                tracker.incrementRejectionCount(shard.shardId());
                logger.warn("Rejecting write requests for shard, stale shards [{}%] shards: {}", percentStale, staleReplicas);
                throw new OpenSearchRejectedExecutionException(
                    "rejected execution on primary shard: " + shard.shardId() + " Stale Replicas: " + staleReplicas + "]",
                    false
                );
            }
        }
    }

    private List<SegmentReplicationShardStats> getStaleReplicas(final Set<SegmentReplicationShardStats> replicas) {
        return replicas.stream()
            .filter(entry -> entry.getCheckpointsBehindCount() > maxCheckpointsBehind)
            .filter(entry -> entry.getCurrentReplicationTimeMillis() > maxReplicationTime.millis())
            .collect(Collectors.toList());
    }

    public SegmentReplicationStats nodeStats() {
        return tracker.getStats();
    }

    public boolean isSegmentReplicationBackpressureEnabled() {
        return isSegmentReplicationBackpressureEnabled;
    }

    public void setSegmentReplicationBackpressureEnabled(boolean segmentReplicationBackpressureEnabled) {
        isSegmentReplicationBackpressureEnabled = segmentReplicationBackpressureEnabled;
    }

    public void setMaxCheckpointsBehind(int maxCheckpointsBehind) {
        this.maxCheckpointsBehind = maxCheckpointsBehind;
    }

    public void setMaxAllowedStaleReplicas(double maxAllowedStaleReplicas) {
        this.maxAllowedStaleReplicas = maxAllowedStaleReplicas;
    }

    public void setMaxReplicationTime(TimeValue maxReplicationTime) {
        this.maxReplicationTime = maxReplicationTime;
    }

    final class AsyncFailStaleReplicaTask extends IndexService.BaseAsyncTask {

        AsyncFailStaleReplicaTask(final IndexService indexService) {
            super(indexService, TimeValue.timeValueSeconds(30));
        }

        @Override
        protected void runInternal() {
            final SegmentReplicationStats stats = tracker.getStats();
            for (Map.Entry<ShardId, SegmentReplicationPerGroupStats> entry : stats.getShardStats().entrySet()) {
                final List<SegmentReplicationShardStats> staleReplicas = getStaleReplicas(entry.getValue().getReplicaStats());
                final ShardId shardId = entry.getKey();
                final IndexService indexService = indicesService.indexService(shardId.getIndex());
                final IndexShard primaryShard = indexService.getShard(shardId.getId());
                for (SegmentReplicationShardStats staleReplica : staleReplicas) {
                    if (staleReplica.getCurrentReplicationTimeMillis() > 2 * maxReplicationTime.millis()) {
                        shardStateAction.remoteShardFailed(shardId, staleReplica.getAllocationId(), primaryShard.getOperationPrimaryTerm(), true, "replica too far behind primary, marking as stale", null, new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                logger.trace("Successfully failed remote shardId [{}] allocation id [{}]", shardId, staleReplica.getAllocationId());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.error("Failed to send remote shard failure", e);
                            }
                        });
                    }
                }
            }
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        public String toString() {
            return "retention_lease_sync";
        }

    }

}
