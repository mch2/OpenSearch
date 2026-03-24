/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.analytics.delegation.DelegationContext;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.plugins.ReaderManagerProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Execution context carrying plan, reader, and delegation state through
 * the query execution lifecycle.
 *
 * @opensearch.internal
 */
public class ExecutionContext {

    private final ResolvedPlan plan;
    private final String tableName;
    private DelegationContext delegationContext;
    private ReaderProvider readerProvider;

    public ExecutionContext(ResolvedPlan plan, String tableName) {
        this.plan = plan;
        this.tableName = tableName;
    }

    public ResolvedPlan plan() {
        return plan;
    }

    public String getTableName() {
        return tableName;
    }

    public void setDelegationContext(DelegationContext delegationContext) {
        this.delegationContext = delegationContext;
    }

    public boolean hasDelegation() {
        return delegationContext != null && delegationContext.hasDelegation();
    }

    public DelegationContext getDelegationContext() {
        return delegationContext;
    }
}
