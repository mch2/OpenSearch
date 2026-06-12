/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

/**
 * D8 version handshake for DF_PROTO {@link FragmentExecutionRequest}s.
 *
 * <p>A data node accepts a proto shard request only when (a) it can execute proto
 * shard plans at all, and (b) the request's {@code planFormatVersion} and
 * {@code dataFusionVersion} match the node's own. Any mismatch yields a
 * {@link PlanFormatMismatchException} carrying the version pair, which the
 * coordinator catches to re-plan on the legacy path while legacy still exists
 * (after Phase 4 it fails the query with the version pair in the message).
 *
 * <p>This isolates the accept/reject decision in one tested place so the wire
 * contract and the node's capability gate evolve together.
 *
 * @opensearch.internal
 */
public final class PlanFormatCompatibility {

    private PlanFormatCompatibility() {}

    /**
     * Whether this data node can execute DF_PROTO shard plans. Flips to {@code true}
     * when the {@code execute_stage_task} shard route + {@code OpenSearchShardScanExec}
     * session build land (Phase 2b). Until then a proto shard request is rejected with
     * a typed mismatch so the coordinator falls back to legacy — never a hard failure.
     */
    public static final boolean SHARD_PROTO_EXECUTION_SUPPORTED = false;

    /**
     * Validate a DF_PROTO shard request against this node. Returns normally when the
     * request can be executed; throws {@link PlanFormatMismatchException} otherwise.
     *
     * @param request the incoming request (must be {@link FragmentExecutionRequest#isProtoFormat()})
     */
    public static void checkShardRequest(FragmentExecutionRequest request) {
        if (!SHARD_PROTO_EXECUTION_SUPPORTED) {
            throw new PlanFormatMismatchException(
                request.getPlanFormatVersion(),
                request.getDataFusionVersion(),
                FragmentExecutionRequest.PLAN_FORMAT_VERSION_LEGACY,
                "shard-proto-execution-not-supported-on-this-node"
            );
        }
        if (request.getPlanFormatVersion() != FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT) {
            throw new PlanFormatMismatchException(
                request.getPlanFormatVersion(),
                request.getDataFusionVersion(),
                FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT,
                FragmentExecutionRequest.DATAFUSION_VERSION
            );
        }
        if (!FragmentExecutionRequest.DATAFUSION_VERSION.equals(request.getDataFusionVersion())) {
            throw new PlanFormatMismatchException(
                request.getPlanFormatVersion(),
                request.getDataFusionVersion(),
                FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT,
                FragmentExecutionRequest.DATAFUSION_VERSION
            );
        }
    }
}
