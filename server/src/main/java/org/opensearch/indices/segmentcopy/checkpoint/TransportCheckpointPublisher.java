/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.client.Client;

public class TransportCheckpointPublisher implements CheckpointPublisher {

    protected static Logger logger = LogManager.getLogger(TransportCheckpointPublisher.class);

    private final Client client;

    public TransportCheckpointPublisher(
        Client client) {
        this.client = client;
    }

    @Override
    public void publish(long primaryTerm, long segmentInfosVersion, long seqNo) {
        client.admin()
            .indices()
            .publishCheckpoint(new PublishCheckpointRequest()
                    .primaryGen(primaryTerm)
                    .segmentInfoVersion(segmentInfosVersion)
                    .seqNo(seqNo),
                new ActionListener<RefreshResponse>() {
                    @Override
                    public void onResponse(RefreshResponse response) {
                        logger.info("Successfully published checkpoints");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Publishing Checkpoints from primary to replicas failed", e);
                    }
                });
    }
}
