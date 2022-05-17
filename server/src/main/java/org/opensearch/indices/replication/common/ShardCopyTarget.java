/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ShardCopyTarget<T> extends AbstractRefCounted {

    // TODO will this cause issues because its shared between subclasses?
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    // last time the target/status was accessed
    private volatile long lastAccessTime = System.nanoTime();
    private final long id;

    protected final AtomicBoolean finished = new AtomicBoolean();
    protected final IndexShard indexShard;
    protected final EventStateListener<T> listener;
    protected final Logger logger;
    protected final CancellableThreads cancellableThreads;

    protected abstract void onDone();

    public abstract T state();

    public abstract ShardCopyTarget<T> retryCopy();

    public abstract String source();

    public EventStateListener<T> getListener() {
        return listener;
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    public abstract void notifyListener(Exception e, boolean sendShardFailure);

    public ShardCopyTarget(String name, IndexShard indexShard, EventStateListener<T> listener) {
        super(name);
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.listener = listener;
        this.id = ID_GENERATOR.incrementAndGet();
        this.indexShard = indexShard;
        this.cancellableThreads = new CancellableThreads();
    }

    public long getId() {
        return id;
    }

    public abstract boolean resetRecovery(CancellableThreads newTargetCancellableThreads) throws IOException;

    /**
     * return the last time this RecoveryStatus was used (based on System.nanoTime()
     */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /**
     * sets the lasAccessTime flag to now
     */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    /**
     * mark the current recovery as done
     */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            try {
                onDone();
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onDone(state());
        }
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(OpenSearchException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed event[" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    protected void ensureRefCount() {
        if (refCount() <= 0) {
            throw new OpenSearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef calls");
        }
    }
}
