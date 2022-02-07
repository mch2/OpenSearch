/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.Timer;
import org.opensearch.indices.segmentcopy.copy.ReplicationCheckpoint;
import org.opensearch.indices.segmentcopy.copy.ReplicationCollection;
import org.opensearch.indices.segmentcopy.copy.ReplicationFailedException;
import org.opensearch.indices.segmentcopy.copy.ReplicationSource;
import org.opensearch.indices.segmentcopy.copy.ReplicationState;
import org.opensearch.indices.segmentcopy.copy.ReplicationTarget;
import org.opensearch.threadpool.ThreadPool;

/**
 * Orchestrator of replication events.
 */
public class SegmentReplicationReplicaService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationReplicaService.class);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    public ReplicationCollection getOnGoingReplications() {
        return onGoingReplications;
    }

    private final ReplicationCollection onGoingReplications;

    public SegmentReplicationReplicaService(final ThreadPool threadPool,
                                            final RecoverySettings recoverySettings) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.onGoingReplications = new ReplicationCollection(logger, threadPool);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingReplications.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    public void startReplication(final ReplicationCheckpoint checkpoint, final IndexShard indexShard, ReplicationSource source, final ReplicationListener listener) {
        final long replicationId = onGoingReplications.startReplication(checkpoint, indexShard, source, listener, recoverySettings.activityTimeout());
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    private void doReplication(final long replicationId) {
        final Timer timer;
        try (ReplicationCollection.ReplicationRef replicationRef = onGoingReplications.getReplication(replicationId)) {
            if (replicationRef == null) {
                logger.trace("not running replication with id [{}] - can not find it (probably finished)", replicationId);
                return;
            }
            final ReplicationTarget replicationTarget = replicationRef.target();
            timer = replicationTarget.state().getTimer();
            final IndexShard indexShard = replicationTarget.getIndexShard();
            try {
                assert replicationTarget.getSource() != null : "can not do a replication without a source";
                logger.trace("{} preparing shard for replication", indexShard.shardId());
            } catch (final Exception e) {
                // this will be logged as warning later on...
                logger.error("unexpected error while preparing shard for peer replication, failing replication", e);
                onGoingReplications.failReplication(
                    replicationId,
                    new ReplicationFailedException(indexShard, "failed to prepare shard for replication", e),
                    true
                );
                return;
            }
            logger.trace("{} starting replication from {}", indexShard.shardId(), replicationTarget.getSource());
            ReplicationResponseHandler listener = new ReplicationResponseHandler(replicationId, indexShard, timer);
            replicationTarget.startReplication(listener);
        }
    }

    class ReplicationRunner extends AbstractRunnable {

        final long replicationId;

        ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void onFailure(Exception e) {
            try (ReplicationCollection.ReplicationRef replicationRef = onGoingReplications.getReplication(replicationId)) {
                if (replicationRef != null) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during replication [{}], failing shard", replicationId), e);
                    onGoingReplications.failReplication(
                        replicationId,
                        new ReplicationFailedException(replicationRef.target().getIndexShard(), "unexpected error", e),
                        true // be safe
                    );
                } else {
                    logger.debug(
                        () -> new ParameterizedMessage("unexpected error during replication, but replication id [{}] is finished", replicationId),
                        e
                    );
                }
            }
        }

        @Override
        public void doRun() {
            doReplication(replicationId);
        }
    }

    private class ReplicationResponseHandler implements ActionListener<ReplicationResponse> {

        private final long replicationId;
        private final IndexShard shard;
        private final Timer timer;

        private ReplicationResponseHandler(final long id, final IndexShard shard, final Timer timer) {
            this.replicationId = id;
            this.timer = timer;
            this.shard = shard;
        }

        @Override
        public void onResponse(ReplicationResponse replicationResponse) {
//            final TimeValue replicationTime = new TimeValue(timer.time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingReplications.markReplicationAsDone(replicationId);
        }

        @Override
        public void onFailure(Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "[{}][{}] Got exception on replication",
                        shard.shardId().getIndex().getName(),
                        shard.shardId().id()
                    ),
                    e
                );
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingReplications.failReplication(
                    replicationId,
                    new ReplicationFailedException(shard, "source has canceled the replication", cause),
                    false
                );
                return;
            }
//            if (cause instanceof ReplicationEngineException) {
//                // unwrap an exception that was thrown as part of the replication
//                cause = cause.getCause();
//            }
//            // do it twice, in case we have double transport exception
//            cause = ExceptionsHelper.unwrapCause(cause);
//            if (cause instanceof ReplicationEngineException) {
//                // unwrap an exception that was thrown as part of the replication
//                cause = cause.getCause();
//            }

//            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)
//
//            if (cause instanceof IllegalIndexShardStateException
//                || cause instanceof IndexNotFoundException
//                || cause instanceof ShardNotFoundException) {
//                // if the target is not ready yet, retry
////                retryReplication(
////                    replicationId,
////                    "remote shard not ready",
////                    recoverySettings.retryDelayStateSync(),
////                    recoverySettings.activityTimeout()
////                );
//                return;
//            }
//
//            // PeerReplicationNotFound is returned when the source node cannot find the replication requested by
//            // the REESTABLISH_RECOVERY request. In this case, we delay and then attempt to restart.
//            if (cause instanceof DelayReplicationException || cause instanceof PeerReplicationNotFound) {
////                retryReplication(replicationId, cause, recoverySettings.retryDelayStateSync(), recoverySettings.activityTimeout());
//                return;
//            }
//
//            if (cause instanceof ConnectTransportException) {
//                logger.info(
//                    "replication of {} from [{}] interrupted by network disconnect, will retry in [{}]; cause: [{}]",
//                    request.shardId(),
//                    request.sourceNode(),
//                    recoverySettings.retryDelayNetwork(),
//                    cause.getMessage()
//                );
//                if (request.sourceNode().getVersion().onOrAfter(LegacyESVersion.V_7_9_0)) {
//                    reestablishReplication(request, cause.getMessage(), recoverySettings.retryDelayNetwork());
//                } else {
////                    retryReplication(replicationId, cause.getMessage(), recoverySettings.retryDelayNetwork(), recoverySettings.activityTimeout());
//                }
//                return;
//            }
//
//            if (cause instanceof AlreadyClosedException) {
//                onGoingReplications.failReplication(replicationId, new ReplicationFailedException(request, "source shard is closed", cause), false);
//                return;
//            }

            onGoingReplications.failReplication(replicationId, new ReplicationFailedException(shard, e), true);
        }
    }

    public interface ReplicationListener {
        void onReplicationDone(ReplicationState state);

        void onReplicationFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure);
    }
}

