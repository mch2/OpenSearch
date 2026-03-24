/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation;

import java.util.List;

/**
 * Carries delegation state for a query. Holds the broker-assigned context IDs
 * for all registered delegation targets.
 *
 * @opensearch.internal
 */
public class DelegationContext {

    public static final DelegationContext NONE = new DelegationContext(List.of());

    private final List<Long> ids;

    public DelegationContext(List<Long> ids) {
        this.ids = List.copyOf(ids);
    }

    /** All delegation context IDs for this query. */
    public List<Long> getIds() {
        return ids;
    }

    /** Returns true if this context carries active delegations. */
    public boolean hasDelegation() {
        return !ids.isEmpty();
    }

    /** Releases all delegation targets from the broker. */
    public void release() {
        DelegationBroker broker = DelegationBroker.getInstance();
        for (long id : ids) {
            broker.release(id);
        }
    }
}
