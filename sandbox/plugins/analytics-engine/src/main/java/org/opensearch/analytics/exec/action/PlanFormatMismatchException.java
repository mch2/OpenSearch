/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Thrown by a data node when a DF_PROTO {@link FragmentExecutionRequest}'s
 * {@code planFormatVersion} / {@code dataFusionVersion} do not match the node's
 * own (df-proto migration D8).
 *
 * <p>Coordinator handling: while the legacy format still exists, the coordinator
 * catches this and re-plans the query on the legacy path. After Phase 4 deletes
 * legacy, the coordinator instead fails the query with the version pair in the
 * message — there is no other negotiation mechanism.
 */
public class PlanFormatMismatchException extends OpenSearchException {

    public PlanFormatMismatchException(
        int requestPlanFormatVersion,
        String requestDataFusionVersion,
        int nodePlanFormatVersion,
        String nodeDataFusionVersion
    ) {
        super(
            "plan format mismatch: request planFormatVersion="
                + requestPlanFormatVersion
                + " dataFusionVersion="
                + requestDataFusionVersion
                + ", node planFormatVersion="
                + nodePlanFormatVersion
                + " dataFusionVersion="
                + nodeDataFusionVersion
        );
    }

    public PlanFormatMismatchException(String message) {
        super(message);
    }

    public PlanFormatMismatchException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
