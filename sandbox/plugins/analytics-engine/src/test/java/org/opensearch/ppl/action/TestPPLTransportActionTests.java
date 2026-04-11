/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link TestPPLTransportAction}.
 *
 * <p>The transport action now directly parses PPL and delegates to the analytics engine.
 * Integration testing via {@code AnalyticsShardDispatchIT} covers the full pipeline.
 * Unit tests for the transport action's listener contract are deferred until the
 * PPL parsing can be mocked without a full Calcite schema context.
 */
public class TestPPLTransportActionTests extends OpenSearchTestCase {

    public void testPlaceholder() {
        // TODO: add unit tests once PPL parsing can be mocked independently
        // Full pipeline is tested via AnalyticsShardDispatchIT
    }
}
