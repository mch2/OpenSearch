/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Describes a backend's join support: which join kinds (INNER, LEFT, etc.) the backend can
 * execute, and which storage formats those joins apply to.
 *
 * <p>Replaces an older enum-only model so a single backend can declare support for
 * multiple kinds without per-algorithm enum entries. The planner inspects
 * {@link BackendCapabilityProvider#joinCapabilities()} when matching a query's required
 * {@link JoinKind} against the backend.
 *
 * @opensearch.internal
 */
public record JoinCapability(Set<JoinKind> kinds, Set<String> formats) {

    /** Standard SQL join kinds. */
    public enum JoinKind {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        SEMI,
        ANTI,
        CROSS
    }
}
