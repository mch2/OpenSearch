/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Map;

/**
 * Request carrying a logical query plan for analytics engine execution.
 * Implements {@link IndicesRequest.Replaceable} so the security plugin's
 * {@code IndexResolverReplacer} can resolve and optionally narrow the
 * target indices for permission evaluation.
 *
 * <p>This request is local-dispatch only (same JVM via {@code NodeClient.execute()}).
 * The {@link RelNode} is transient and not wire-serializable.
 */
public class AnalyticsQueryRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private final transient RelNode plan;
    private final transient QueryRequestContext queryCtx;
    private final boolean profile;
    private String[] indices;

    public AnalyticsQueryRequest(RelNode plan, QueryRequestContext queryCtx, String[] indices) {
        this(plan, queryCtx, indices, false);
    }

    public AnalyticsQueryRequest(RelNode plan, QueryRequestContext queryCtx, String[] indices, boolean profile) {
        this.plan = plan;
        this.queryCtx = queryCtx;
        this.indices = indices;
        this.profile = profile;
    }

    public AnalyticsQueryRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("AnalyticsQueryRequest is local-dispatch only");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("AnalyticsQueryRequest is local-dispatch only");
    }

    public RelNode getPlan() {
        return plan;
    }

    public QueryRequestContext getQueryCtx() {
        return queryCtx;
    }

    public boolean isProfile() {
        return profile;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpandOpen();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new AnalyticsQueryTask(id, type, action, "pending", parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
