/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for the D8 version handshake in {@link PlanFormatCompatibility}.
 */
public class PlanFormatCompatibilityTests extends OpenSearchTestCase {

    private static FragmentExecutionRequest protoRequest(int planFormatVersion, String dfVersion) {
        return new FragmentExecutionRequest(
            "q1",
            1,
            new ShardId("idx", "uuid", 0),
            planFormatVersion,
            dfVersion,
            new byte[] { 1, 2, 3 }
        );
    }

    public void testLegacyRequestIsNotProtoFormat() {
        FragmentExecutionRequest legacy = new FragmentExecutionRequest("q1", 1, new ShardId("idx", "uuid", 0), List.of());
        assertFalse("legacy request must not be proto format", legacy.isProtoFormat());
    }

    public void testProtoRequestIsProtoFormat() {
        FragmentExecutionRequest req = protoRequest(
            FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT,
            FragmentExecutionRequest.DATAFUSION_VERSION
        );
        assertTrue("version>0 request must be proto format", req.isProtoFormat());
    }

    public void testCheckRejectsWhenShardProtoUnsupported() {
        // While SHARD_PROTO_EXECUTION_SUPPORTED is false, any proto shard request — even a
        // version-matched one — is rejected so the coordinator falls back to legacy (D8).
        FragmentExecutionRequest req = protoRequest(
            FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT,
            FragmentExecutionRequest.DATAFUSION_VERSION
        );
        PlanFormatMismatchException e = expectThrows(
            PlanFormatMismatchException.class,
            () -> PlanFormatCompatibility.checkShardRequest(req)
        );
        assertNotNull(e.getMessage());
    }

    public void testWireRoundTripPreservesProtoFields() throws Exception {
        FragmentExecutionRequest original = protoRequest(7, "54.0.0");
        FragmentExecutionRequest restored = copyRequest(original);
        assertTrue(restored.isProtoFormat());
        assertEquals(7, restored.getPlanFormatVersion());
        assertEquals("54.0.0", restored.getDataFusionVersion());
        assertArrayEquals(new byte[] { 1, 2, 3 }, restored.getPlanBytes());
    }

    public void testWireRoundTripLegacyHasNoProtoTrailer() throws Exception {
        FragmentExecutionRequest legacy = new FragmentExecutionRequest("q1", 1, new ShardId("idx", "uuid", 0), List.of());
        FragmentExecutionRequest restored = copyRequest(legacy);
        assertFalse(restored.isProtoFormat());
        assertEquals(FragmentExecutionRequest.PLAN_FORMAT_VERSION_LEGACY, restored.getPlanFormatVersion());
        assertNull(restored.getPlanBytes());
    }

    private static FragmentExecutionRequest copyRequest(FragmentExecutionRequest original) throws Exception {
        try (var out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            original.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                return new FragmentExecutionRequest(in);
            }
        }
    }
}
