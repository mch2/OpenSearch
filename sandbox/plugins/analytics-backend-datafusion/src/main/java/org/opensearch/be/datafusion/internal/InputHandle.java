/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.internal;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Handle for pushing Arrow record batches into a local engine's
 * partition stream input. Each handle corresponds to one child stage's output.
 * <p>
 * Moved from {@code org.opensearch.analytics.backend} (analytics-framework)
 * to the DataFusion backend module since it is now backend-internal.
 *
 * @opensearch.internal
 */
public interface InputHandle extends AutoCloseable {

    void pushBatch(VectorSchemaRoot batch);

    void closeInput();

    @Override
    default void close() {
        closeInput();
    }
}
