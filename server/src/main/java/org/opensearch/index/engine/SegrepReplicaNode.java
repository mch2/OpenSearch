/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.copy.PrimaryShardReplicationSource;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;
import org.opensearch.indices.replication.copy.SegmentReplicationPrimaryService;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SegrepReplicaNode extends ReplicaNode {
    private static final Logger logger = LogManager.getLogger(SegrepReplicaNode.class);

    PrimaryShardReplicationSource replicationSource;
    Jobs jobs;
    Store store;
    ShardId shardId;

    public SegrepReplicaNode(ShardId shardId,
                             Store store,
                             PrimaryShardReplicationSource replicationSource,
                             int id,
                             SearcherFactory searcherFactory,
                             PrintStream printStream) throws IOException {
        super(id, store.directory(), searcherFactory, printStream);
        this.shardId = shardId;
        this.replicationSource = replicationSource;
        jobs = new Jobs(this);
        jobs.setName("R" + id + ".copyJobs");
        jobs.setDaemon(true);
        jobs.start();
        start(1);
    }

    @Override
    protected CopyJob newCopyJob(String reason, Map<String, FileMetaData> files, Map<String, FileMetaData> prevFiles, boolean highPriority, CopyJob.OnceDone onceDone) throws IOException {
        final CountDownLatch latch = new CountDownLatch(1);
        StepListener<CopyState> copyStateStepListener = new StepListener<>();
        ActionListener<CopyState> listener = new LatchedActionListener<>(copyStateStepListener, latch);
        replicationSource.getCheckpointInfo(0, new ReplicationCheckpoint(shardId, lastPrimaryGen, 0, 0, 0), listener);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final CopyState result = copyStateStepListener.result();
        logger.info("Got copy state with primary gen {}", result.primaryGen);
        logger.info("Got copy state with files {}", result.files.keySet());
        return new TransportCopyJob(reason, shardId, store, replicationSource, result, this, highPriority, onceDone);
    }

    @Override
    protected void launch(CopyJob job) {
        jobs.launch(job);
    }

    @Override
    protected void sendNewReplica() throws IOException {
        // Already done.
    }

    @Override
    public void close() throws IOException {
        // Can't be sync'd when calling jobs since it can lead to deadlock:
        jobs.close();
        message("top: jobs closed");
        synchronized (mergeCopyJobs) {
            for (CopyJob job : mergeCopyJobs) {
                message("top: cancel merge copy job " + job);
                job.cancel("jobs closing", null);
            }
        }
        super.close();
    }
}
