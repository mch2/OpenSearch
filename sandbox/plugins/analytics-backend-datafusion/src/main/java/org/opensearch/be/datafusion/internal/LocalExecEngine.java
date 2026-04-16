/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.internal;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.EngineResultStream;

/**
 * Backend-internal execution engine interface. Runs a plan fragment
 * in-process with streaming inputs fed from child stages.
 * <p>
 * Moved from {@code org.opensearch.analytics.backend} (analytics-framework)
 * to the DataFusion backend module since it is now backend-internal.
 *
 * @opensearch.internal
 */
public interface LocalExecEngine extends AutoCloseable {

    InputHandle registerInput(String stageInputId, Schema schema);

    EngineResultStream execute();

    @Override
    void close();
}
