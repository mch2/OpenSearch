/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;

/**
 * Default {@link StageResultHandler} that feeds each batch to the root
 * {@link ExchangeSink}. Used for scan, filter, aggregate, and all
 * non-shuffle stages.
 *
 * @opensearch.internal
 */
public class SinkFeedingHandler implements StageResultHandler {

    private final ExchangeSink rootSink;

    public SinkFeedingHandler(ExchangeSink rootSink) {
        this.rootSink = rootSink;
    }

    @Override
    public void onBatch(FragmentExecutionResponse response, ShardTarget target) {
        rootSink.feed(response);
    }
}
