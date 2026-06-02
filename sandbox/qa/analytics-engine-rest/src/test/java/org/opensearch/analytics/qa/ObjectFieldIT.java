/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Diagnostic integration tests for PPL access to OpenSearch {@code object} fields
 * via dotted-path notation ({@code city.name}, {@code city.location.latitude}) on the
 * analytics-engine route. Mirrors the shape of the sql repo's
 * {@code ObjectFieldOperateIT}. Every test here is expected to fail initially —
 * the purpose is to surface exact failure modes for follow-up debugging, not to
 * exercise a working implementation.
 */
public class ObjectFieldIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("object_fields", "object_fields");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testSelectSingleObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name | head 3",
            row("Seattle"),
            row("Portland"),
            row("Austin")
        );
    }

    public void testSelectMultipleObjectFields() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, account.owner | head 3",
            row("Seattle", "alice"),
            row("Portland", "bob"),
            row("Austin", "carol")
        );
    }

    public void testSelectDeeplyNestedObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, city.location.latitude | head 3",
            row("Seattle", 47.6062),
            row("Portland", 45.5152),
            row("Austin", 30.2672)
        );
    }

    public void testMinOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats min(account.balance)",
            row(300.25)
        );
    }

    public void testMaxOnDeeplyNestedObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats max(city.location.latitude)",
            row(47.6062)
        );
    }

    public void testSumOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats sum(city.population)",
            row(2380000)
        );
    }

    public void testFilterOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.name='Seattle' | fields account.owner",
            row("alice")
        );
    }

    public void testFilterOnDeeplyNestedObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.location.latitude > 40 | fields city.name",
            row("Seattle"),
            row("Portland")
        );
    }

    // ── Object-parent projection (analytics-engine ObjectFieldStitch) ─────────
    //
    // Projecting an object parent (top-level "city" or intermediate "city.location")
    // returns a nested JSON value built from the underlying flat leaves. The schema
    // surfaces each parent as a synthetic ObjectType column; analytics-engine's
    // ObjectFieldStitch rewrites those references into leaf projections at planning
    // time and re-assembles them into a Map<String,Object> on the coordinator side
    // before the response goes back to the SQL plugin.

    public void testSelectIntermediateObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.location | head 1",
            row(Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    public void testSelectTopLevelObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    public void testSelectTopLevelObjectFieldWithSiblings() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city, account | head 1",
            row(
                Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)),
                Map.of("owner", "alice", "balance", 1000.50)
            )
        );
    }

    public void testSelectParentAndLeafMixed() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, city.location | head 1",
            row("Seattle", Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    // ── Negative tests: only fetch/projection of object parents is supported ──
    //
    // Filtering, aggregating, evaluating, sorting on an object-parent column has no defined
    // semantics — the schema only exposes the parent as an opaque MAP marker, and the engine
    // strips it before execution. These tests pin the failure surface so a future change that
    // accidentally allows the operation surfaces here.

    /** {@code | where city = ...} — predicate on an object parent must be rejected. */
    public void testFilterOnObjectParentFails() throws IOException {
        expectFailure(
            "source=" + DATASET.indexName + " | where city = 'Seattle'",
            "filter on object parent should fail"
        );
    }

    /** {@code | stats min(city)} — aggregate over an object parent must be rejected. */
    public void testAggregateOnObjectParentFails() throws IOException {
        expectFailure(
            "source=" + DATASET.indexName + " | stats min(city)",
            "aggregate on object parent should fail"
        );
    }

    /**
     * {@code | eval x = city | fields x} — assigning the parent to a new alias and projecting
     * it works as if the user had written {@code | fields city} directly. The SQL plugin
     * flattens the eval+fields pair, so the topmost Project we see still references the
     * underlying ObjectType column. We exercise it here as positive coverage rather than a
     * negative case.
     */
    public void testEvalAssignObjectParentPasses() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval x = city | fields x | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    /** {@code | sort city} — sorting on an object parent must be rejected. */
    public void testSortOnObjectParentFails() throws IOException {
        expectFailure(
            "source=" + DATASET.indexName + " | sort city",
            "sort on object parent should fail"
        );
    }

    // ── helpers (mirrored from FieldsCommandIT) ────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Runs a PPL query and asserts that it fails — the SQL plugin or analytics-engine must
     * reject the query rather than silently producing wrong results. We deliberately don't
     * pin a specific error code or message because the failure can surface from the SQL
     * plugin's validator (column-not-found semantics on the synthetic ObjectType column),
     * from analytics-engine's rewriter (RexInputRef-to-stripped-column thrown), or from
     * DataFusion (unable to convert MAP type) — all valid forms of "reject this".
     */
    private void expectFailure(String ppl, String why) throws IOException {
        try {
            executePpl(ppl);
            fail("Query should have failed (" + why + "): " + ppl);
        } catch (ResponseException e) {
            // Expected: the server returned a 4xx / 5xx error. We don't assert on the exact
            // status code or message — any non-success return is acceptable here.
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }


}
