/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.Engine;
import org.opensearch.indices.segmentcopy.checkpoint.CheckpointPublisher;

import java.io.IOException;

// TODO: this could be internal to a PrimaryShard class.
public class CheckpointRefreshListener implements ReferenceManager.RefreshListener {

    protected static Logger logger = LogManager.getLogger(CheckpointRefreshListener.class);

    private final IndexShard shard;
    private final CheckpointPublisher publisher;

    public CheckpointRefreshListener(IndexShard shard, CheckpointPublisher publisher) {
        this.shard = shard;
        this.publisher = publisher;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do nothing
        if (shard.routingEntry().primary()) {
            Engine engine = shard.getEngine();
            logger.info("Primary - Before refresh: " +
                " Last processed: {}" +
                " Last persisted: {}" +
                " Last synced global: {}" +
                "", engine.getProcessedLocalCheckpoint(), engine.getPersistedLocalCheckpoint(), engine.getLastSyncedGlobalCheckpoint());
        }
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (shard.routingEntry().primary()) {
            Engine engine = shard.getEngine();
            logger.info("Primary - Before publishing: " +
                " Last processed: {}" +
                " Last persisted: {}" +
                " Last synced global: {}" +
                "", engine.getProcessedLocalCheckpoint(), engine.getPersistedLocalCheckpoint(), engine.getLastSyncedGlobalCheckpoint());
            publisher.publish(shard.getOperationPrimaryTerm(), shard.getLatestSegmentInfos().getVersion(), engine.getProcessedLocalCheckpoint());
        }
    }
}
