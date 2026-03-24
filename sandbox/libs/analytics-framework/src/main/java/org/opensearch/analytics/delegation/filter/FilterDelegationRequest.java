/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation.filter;

import java.util.Objects;

/**
 * Immutable request to delegate a filter predicate to a target backend.
 * Built Java-side from JNI primitive arguments, then passed through
 * the {@link org.opensearch.analytics.delegation.DelegationBroker delegation broker}.
 *
 * @opensearch.internal
 */
public final class FilterDelegationRequest {

    private final String targetBackend;
    private final byte[] predicatePayload;
    private final SegmentContext segmentContext;

    /**
     * @param targetBackend    backend name to delegate to (e.g. "lucene")
     * @param predicatePayload serialized predicate (e.g. QueryBuilder bytes)
     * @param segmentContext   segment alignment for the delegation
     */
    public FilterDelegationRequest(String targetBackend, byte[] predicatePayload, SegmentContext segmentContext) {
        this.targetBackend = Objects.requireNonNull(targetBackend, "targetBackend");
        Objects.requireNonNull(predicatePayload, "predicatePayload");
        this.predicatePayload = predicatePayload.clone();
        this.segmentContext = Objects.requireNonNull(segmentContext, "segmentContext");
    }

    public String getTargetBackend() {
        return targetBackend;
    }

    public byte[] getPredicatePayload() {
        return predicatePayload.clone();
    }

    public SegmentContext getSegmentContext() {
        return segmentContext;
    }

    @Override
    public String toString() {
        return "FilterDelegationRequest[target=" + targetBackend
            + ", payload=" + predicatePayload.length + " bytes"
            + ", segment=" + segmentContext + "]";
    }
}
