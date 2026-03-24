/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.delegation.DelegationContext;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Delegates Substrait conversion to {@link SubstraitConverter} and execution
 * to the native DataFusion runtime via {@link DatafusionSearcher}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine {

    private final DatafusionContext context;

    public DatafusionSearchExecEngine(DatafusionContext context) {
        this.context = context;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        RelNode prepared = SubstraitConverter.rewriteHybridFilters(requestContext.plan().getRoot());
        byte[] substraitBytes = SubstraitConverter.convert(prepared);

        if (requestContext.hasDelegation()) {
            DelegationContext delegation = requestContext.getDelegationContext();
            substraitBytes = SubstraitConverter.embedDelegation(
                substraitBytes, delegation.getId(), null, "lucene-analytics-backend");
        }
        context.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes));
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        DatafusionSearcher searcher = context.getEngineSearcher();
        searcher.search(context);
        return new DatafusionResultStream(context.getStreamHandle());
    }

    @Override
    public void close() throws IOException {
        context.close();
    }
}
