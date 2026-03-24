/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.delegation.filter.FilterDelegationTarget;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Registry mapping delegation context IDs to {@link DelegationTarget} instances.
 * Each target gets its own ID. A single query may register multiple targets.
 *
 * <p>Rust JNI callbacks resolve targets via the static {@link #delegateFilter} entry point.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class DelegationBroker {

    private static final Logger logger = LogManager.getLogger(DelegationBroker.class);
    private static final DelegationBroker INSTANCE = new DelegationBroker();

    private final AtomicLong nextId = new AtomicLong(1);
    private final ConcurrentHashMap<Long, DelegationTarget> targets = new ConcurrentHashMap<>();

    public static DelegationBroker getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a delegation target and returns its context ID.
     */
    public long register(DelegationTarget target) {
        long id = nextId.getAndIncrement();
        targets.put(id, target);
        logger.info("[DelegationBroker] register: id={}, type={}", id, target.type());
        return id;
    }

    /**
     * Releases a delegation context.
     */
    public void release(long delegationContextId) {
        targets.remove(delegationContextId);
    }

    /**
     * Resolves a {@link FilterDelegationTarget} by context ID.
     */
    FilterDelegationTarget resolveFilterTarget(long delegationContextId) {
        DelegationTarget target = targets.get(delegationContextId);
        return target instanceof FilterDelegationTarget ? (FilterDelegationTarget) target : null;
    }

    /**
     * Called from Rust via JNI to delegate a filter predicate.
     *
     * @param delegationContextId the context ID
     * @param targetBackend       the backend name (for logging/routing)
     * @param segmentOrd          0-based segment ordinal
     * @param minDocId            inclusive min doc ID
     * @param maxDocId            exclusive max doc ID
     * @return matching doc IDs as BitSet.toLongArray(), or empty on error
     */
    public static long[] delegateFilter(
        long delegationContextId, String targetBackend,
        int segmentOrd, int minDocId, int maxDocId) {
        logger.info("[DelegationBroker] delegateFilter: ctxId={}, backend={}, segment={}, docs=[{}, {})",
            delegationContextId, targetBackend, segmentOrd, minDocId, maxDocId);

        FilterDelegationTarget target = INSTANCE.resolveFilterTarget(delegationContextId);
        if (target == null) {
            logger.warn("[DelegationBroker] No FilterDelegationTarget for ctxId={}", delegationContextId);
            return new long[0];
        }

        long[] result = target.delegateFilter(targetBackend, segmentOrd, minDocId, maxDocId);
        logger.info("[DelegationBroker] delegateFilter result: segment={}, bitsetWords={}", segmentOrd, result.length);
        return result;
    }
}
