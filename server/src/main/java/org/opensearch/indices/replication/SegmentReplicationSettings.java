/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.transport.TransportRequestOptions;

public class SegmentReplicationSettings implements ReplicationSettings {

    private volatile TimeValue internalActionRetryTimeout;
    private volatile ByteSizeValue maxBytesPerSec;
    private volatile int maxConcurrentFileChunks;
    private volatile ByteSizeValue chunkSize;
    private volatile RateLimiter rateLimiter;

    public static final Setting<ByteSizeValue> INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting(
        "indices.replication.max_bytes_per_sec",
        new ByteSizeValue(0, ByteSizeUnit.MB),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** timeout value to use for the retrying of requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_REPLICATION_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "indices.replication.internal_action_retry_timeout",
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_REPLICATION_MAX_CONCURRENT_FILE_CHUNKS_SETTING = Setting.intSetting(
        "indices.replication.max_concurrent_file_chunks",
        5,
        1,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<ByteSizeValue> INDICES_REPLICATION_MAX_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.replication.max_chunk_size",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SegmentReplicationSettings(Settings settings, ClusterSettings clusterSettings) {

        maxConcurrentFileChunks = INDICES_REPLICATION_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        maxBytesPerSec = INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING.get(settings);
        setChunkSize(INDICES_REPLICATION_MAX_CHUNK_SIZE_SETTING.get(settings));
        internalActionRetryTimeout = INDICES_REPLICATION_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(INDICES_REPLICATION_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING, this::setInternalActionRetryTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING, this::setMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(INDICES_REPLICATION_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_REPLICATION_MAX_CHUNK_SIZE_SETTING, this::setChunkSize);
    }

    private void setInternalActionRetryTimeout(TimeValue timeValue) {
        this.internalActionRetryTimeout = timeValue;
    }

    @Override
    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    @Override
    public TimeValue internalActionTimeout() {
        return internalActionRetryTimeout;
    }

    @Override
    public TransportRequestOptions.Type getTransportRequestType() {
        return TransportRequestOptions.Type.RECOVERY;
    }

    public int getChunkSize() {
        return Math.toIntExact(chunkSize.getBytes());
    }

    public void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        this.maxBytesPerSec = maxBytesPerSec;
        if (maxBytesPerSec.getBytes() <= 0) {
            rateLimiter = null;
        } else if (rateLimiter != null) {
            rateLimiter.setMBPerSec(maxBytesPerSec.getMbFrac());
        } else {
            rateLimiter = new RateLimiter.SimpleRateLimiter(maxBytesPerSec.getMbFrac());
        }
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }
}
