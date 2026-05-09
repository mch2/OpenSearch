/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Algorithm a backend uses to execute a join. Backends declare which algorithms
 * they support via {@link BackendCapabilityProvider#supportedJoinAlgorithms()};
 * the planner selects an algorithm and matches it against backend support.
 *
 * @opensearch.internal
 */
public enum JoinAlgorithm {
    /** Both inputs gathered SINGLETON to the coordinator; build hash table on right, probe with left. */
    COORDINATOR_HASH
}
