/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Transport response carrying a native Arrow {@link VectorSchemaRoot} from a
 * shard fragment execution. Extends {@link ArrowBatchResponse} so the Arrow
 * Flight transport does a zero-copy transfer of the producer root into the
 * wire-side shared root — no byte serialization.
 *
 * <p>On the receive side, the constructor from {@link StreamInput} extracts
 * the root from {@code VectorStreamInput.getRoot()}, giving the handler
 * direct access to the Arrow data without deserialization.
 *
 * @opensearch.internal
 */
public class FragmentExecutionResponse extends ArrowBatchResponse {

    /**
     * Send-side constructor. The producer populates {@code root} and hands
     * it off; the framework transfers it zero-copy into the Flight stream.
     */
    public FragmentExecutionResponse(VectorSchemaRoot root) {
        super(root);
    }

    /**
     * Receive-side constructor. Extracts the Arrow root from the Flight
     * stream via {@code VectorStreamInput.getRoot()}.
     */
    public FragmentExecutionResponse(StreamInput in) throws IOException {
        super(in);
    }

    /** Convenience accessor for the underlying Arrow root. */
    public VectorSchemaRoot getArrowRoot() {
        return getRoot();
    }
}
