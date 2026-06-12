/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Unit tests for {@link QueryPlanJson} — the Java side of the whole-plan FFM JSON contract
 * (whole-plan-lowering-spec.md D12). Asserts the encoded {@code QueryPlanInput} carries exactly
 * the field names the Rust {@code whole_plan::QueryPlanInput} serde struct expects, and that a
 * Rust-shaped {@code QueryPlanOutput} round-trips through {@link QueryPlanJson#decodeOutput}.
 */
public class QueryPlanJsonTests extends OpenSearchTestCase {

    public void testEncodeInputFieldNamesMatchRustSerde() {
        byte[] substrait = new byte[] { 1, 2, 3 };
        byte[] json = QueryPlanJson.encodeInput(
            "q1",
            substrait,
            List.of(new QueryPlanJson.ScanInput("http_logs", 2, false))
        );
        String s = new String(json, StandardCharsets.UTF_8);

        // Field names are the Rust serde contract — must be present verbatim.
        assertTrue(s, s.contains("\"query_id\":\"q1\""));
        assertTrue(s, s.contains("\"substrait_b64\":\"" + java.util.Base64.getEncoder().encodeToString(substrait) + "\""));
        assertTrue(s, s.contains("\"scans\""));
        assertTrue(s, s.contains("\"table\":\"http_logs\""));
        assertTrue(s, s.contains("\"tree_shape\":2"));
        assertTrue(s, s.contains("\"requests_row_ids\":false"));
        assertTrue(s, s.contains("\"delegated\":[]"));
    }

    public void testDecodeRustShapedOutput() {
        // Exactly the shape Rust's QueryPlanOutput serializes (field-order-independent).
        String plan0 = java.util.Base64.getEncoder().encodeToString(new byte[] { 10, 20 });
        String schema0 = java.util.Base64.getEncoder().encodeToString(new byte[] { 30 });
        String rust = "{\"stages\":["
            + "{\"boundary_id\":-1,\"child_boundary_ids\":[0],\"plan_bytes_b64\":\"" + plan0
            + "\",\"output_schema_ipc_b64\":\"" + schema0 + "\"},"
            + "{\"boundary_id\":0,\"child_boundary_ids\":[],\"plan_bytes_b64\":\"\",\"output_schema_ipc_b64\":\"\"}"
            + "]}";

        List<QueryPlanJson.StageOutput> stages = QueryPlanJson.decodeOutput(rust.getBytes(StandardCharsets.UTF_8));
        assertEquals(2, stages.size());

        QueryPlanJson.StageOutput root = stages.stream().filter(s -> s.boundaryId() == -1).findFirst().orElseThrow();
        assertArrayEquals(new int[] { 0 }, root.childBoundaryIds());
        assertArrayEquals(new byte[] { 10, 20 }, root.planBytes());
        assertArrayEquals(new byte[] { 30 }, root.outputSchemaIpc());

        QueryPlanJson.StageOutput leaf = stages.stream().filter(s -> s.boundaryId() == 0).findFirst().orElseThrow();
        assertEquals(0, leaf.childBoundaryIds().length);
        assertEquals(0, leaf.planBytes().length);
    }
}
