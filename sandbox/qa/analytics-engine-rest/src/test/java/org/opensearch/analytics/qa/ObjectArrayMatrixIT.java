/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The full surface a user actually types against an OTel-shaped index: a nested object, an array of
 * objects, and scalar arrays — read, filtered, expanded and aggregated.
 *
 * <p>Every assertion here came from a hand-run comparison against the same data on a Lucene index,
 * so each one records the answer Lucene gives. The suite exists because that comparison found cases
 * that returned a <em>wrong answer</em> rather than an error, which no other test caught.
 *
 * <p><b>Each document is flushed separately</b>, so the shard holds one Parquet file per document and
 * every child is first seen in a different file. That is the shape that turns a schema-reconciliation
 * bug into a wrong answer instead of an error, and it is what a real ingest looks like.
 *
 * <pre>
 * id  city.name  city.geo.lat  tags              codes       events
 * s1  seattle    47.6          [prod, us-east]   [200, 201]  [{start,1}, {validate,2}, {commit,3}]
 * s2  portland   45.5          [prod, us-west]   [500, 503]  [{start,1}, {timeout,9}]
 * s3  austin     30.2          [staging]         [200]       [{start,1}]
 * s4  seattle    47.6          [prod]            [500]       [{start,1}, {retry,2}, {retry,3}, {fail,4}]
 * s5  denver     39.7          []                [200]       (absent)
 * s6  boston     42.3          (absent)          (absent)    (absent)
 * </pre>
 */
public class ObjectArrayMatrixIT extends AnalyticsRestTestCase {

    private static final String INDEX = "object_array_matrix_it";

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);

        // One file per document: array-ness and every object child are first seen in different files.
        index("{\"id\":\"s1\",\"city\":{\"name\":\"seattle\",\"geo\":{\"lat\":47.6,\"lon\":-122.3}},"
            + "\"tags\":[\"prod\",\"us-east\"],\"codes\":[200,201],"
            + "\"events\":[{\"name\":\"start\",\"time\":1},{\"name\":\"validate\",\"time\":2},{\"name\":\"commit\",\"time\":3}]}");
        index("{\"id\":\"s2\",\"city\":{\"name\":\"portland\",\"geo\":{\"lat\":45.5,\"lon\":-122.6}},"
            + "\"tags\":[\"prod\",\"us-west\"],\"codes\":[500,503],"
            + "\"events\":[{\"name\":\"start\",\"time\":1},{\"name\":\"timeout\",\"time\":9}]}");
        index("{\"id\":\"s3\",\"city\":{\"name\":\"austin\",\"geo\":{\"lat\":30.2,\"lon\":-97.7}},"
            + "\"tags\":[\"staging\"],\"codes\":[200],\"events\":[{\"name\":\"start\",\"time\":1}]}");
        index("{\"id\":\"s4\",\"city\":{\"name\":\"seattle\",\"geo\":{\"lat\":47.6,\"lon\":-122.3}},"
            + "\"tags\":[\"prod\"],\"codes\":[500],"
            + "\"events\":[{\"name\":\"start\",\"time\":1},{\"name\":\"retry\",\"time\":2},"
            + "{\"name\":\"retry\",\"time\":3},{\"name\":\"fail\",\"time\":4}]}");
        index("{\"id\":\"s5\",\"city\":{\"name\":\"denver\",\"geo\":{\"lat\":39.7,\"lon\":-105.0}},\"tags\":[],\"codes\":[200]}");
        index("{\"id\":\"s6\",\"city\":{\"name\":\"boston\",\"geo\":{\"lat\":42.3,\"lon\":-71.0}}}");
        provisioned = true;
    }

    private void index(String doc) throws IOException {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n" + doc + "\n");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    // ── a nested object: the struct, its leaf, and a leaf one level deeper ──────────────────────

    /** Depth 1 and 2. The deeper one is the case a single dotted ITEM key cannot express. */
    public void testNestedObjectLeavesAtEveryDepth() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where id='s1' | fields city.name", row("seattle"));
        assertRowsEqualUnordered("source=" + INDEX + " | where id='s1' | fields city.geo.lat", row(47.6));
        assertRowsEqualUnordered("source=" + INDEX + " | where city.geo.lat > 46 | fields id", row("s1"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where city.name='seattle' | fields id", row("s1"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | stats count() by city.name | where city.name='seattle'", row(2, "seattle"));
    }

    // ── an array of objects: element-scoped filters ────────────────────────────────────────────

    /** Every document whose events contain a matching element, not just the first file's. */
    public void testArrayOfObjectsLeafEquality() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='retry' | fields id", row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='start' | fields id", row("s1"), row("s2"), row("s3"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='timeout' and id='s2' | fields id", row("s2"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.time=3 | fields id", row("s1"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.time>2 | fields id", row("s1"), row("s2"), row("s4"));
    }

    /** A disjunction over one leaf: Calcite folds it to a Sarg, which is expanded before matching. */
    public void testArrayOfObjectsLeafDisjunction() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='retry' or events.name='timeout' | fields id", row("s2"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name in ('retry','timeout') | fields id", row("s2"), row("s4"));
    }

    /** Element scoping: no single event is both, so the answer is 0 rather than 1. */
    public void testArrayOfObjectsMultiLeafIsElementScoped() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='timeout' and events.time=9 | fields id", row("s2"));
        assertRowsEqualUnordered("source=" + INDEX + " | where events.name='timeout' and events.time=1 | stats count()", row(0));
    }

    /** Null predicates partition the documents: 4 with events, 2 without. */
    public void testNullPredicatesOnArraysPartition() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | where isnotnull(events) | fields id", row("s1"), row("s2"), row("s3"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where isnull(events) | fields id", row("s5"), row("s6"));
        assertRowsEqualUnordered("source=" + INDEX + " | where isnotnull(events.name) | fields id", row("s1"), row("s2"), row("s3"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where isnull(events.name) | fields id", row("s5"), row("s6"));
        // s5's [] is an absent field, so it answers isnull alongside s6.
        assertRowsEqualUnordered("source=" + INDEX + " | where isnull(tags) | fields id", row("s5"), row("s6"));
        assertRowsEqualUnordered("source=" + INDEX + " | where isnotnull(tags) | stats count()", row(4));
    }

    // ── projection and expansion ───────────────────────────────────────────────────────────────

    /** A leaf of an array of objects reads element-wise: one array per row, in element order. */
    public void testArrayOfObjectsLeafProjection() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields id, events.name",
            row("s1", List.of("start", "validate", "commit")),
            row("s2", List.of("start", "timeout")),
            row("s3", List.of("start")),
            row("s4", List.of("start", "retry", "retry", "fail")),
            row("s5", null),
            row("s6", null)
        );
    }

    /** Expanding the array column gives one row per element; the leaf is then a scalar. */
    public void testExpandArrayOfObjects() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | mvexpand events | stats count()", row(12));
        assertRowsEqualUnordered("source=" + INDEX + " | mvexpand events | where events.name='retry' | stats count()", row(2));
    }

    // ── aggregates ─────────────────────────────────────────────────────────────────────────────

    /**
     * Group keys and aggregate arguments both expand per element, but only the argument keeps a
     * document's repeated values: a group key counts documents per distinct value, which is what a
     * Lucene terms aggregation reports. So s4's two {@code retry} events are one {@code retry} bucket
     * entry, while {@code count(events.name)} still counts all ten values.
     */
    public void testAggregatesOverArrays() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | stats max(codes)", row(503));
        assertRowsEqualUnordered("source=" + INDEX + " | stats min(codes)", row(200));
        assertRowsEqualUnordered("source=" + INDEX + " | stats dc(tags)", row(4));
        assertRowsEqualUnordered("source=" + INDEX + " | stats count(events.name)", row(10));
        assertRowsEqualUnordered("source=" + INDEX + " | stats dc(events.name)", row(6));
        assertRowsEqualUnordered(
            "source=" + INDEX + " | stats count() by events.name",
            row(4, "start"),
            row(1, "validate"),
            row(1, "commit"),
            row(1, "timeout"),
            row(1, "retry"),
            row(1, "fail"),
            rowWithNull(2)
        );
    }

    // ── still unsupported: recorded so a change of behaviour is visible ────────────────────────

    /**
     * A scalar array compared against a scalar is rejected by the frontend's type checkers. Lucene
     * answers it — an inverted index matches if any value matches — so this is a known parity gap,
     * recorded rather than endorsed. {@code mvfind(tags,'prod') >= 0} is the working equivalent.
     */
    public void testScalarArrayComparisonIsRejected() throws IOException {
        assertRejected("source=" + INDEX + " | where tags = 'prod' | fields id", "ARRAY");
        assertRejected("source=" + INDEX + " | where codes = 500 | fields id", "ARRAY");
        assertRejected("source=" + INDEX + " | stats avg(codes)", "ARRAY");
        // The equivalents that do work today.
        assertRowsEqualUnordered("source=" + INDEX + " | where mvfind(tags,'prod') >= 0 | fields id", row("s1"), row("s2"), row("s4"));
        assertRowsEqualUnordered("source=" + INDEX + " | where match(tags,'prod') | fields id", row("s1"), row("s2"), row("s4"));
    }

    private void assertRejected(String ppl, String expectedInMessage) throws IOException {
        try {
            executePpl(ppl);
            fail("expected a 400 for: " + ppl);
        } catch (ResponseException e) {
            assertEquals("expected a 400 for: " + ppl, 400, e.getResponse().getStatusLine().getStatusCode());
            String body = e.getMessage() == null ? "" : e.getMessage();
            assertTrue("expected the error to mention " + expectedInMessage + ", got: " + body, body.contains(expectedInMessage));
        }
    }

    /** {@link java.util.List#of} rejects nulls, so a null-key bucket needs Arrays.asList. */
    private static List<Object> rowWithNull(Object first) {
        return Arrays.asList(first, null);
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Compares rows as a multiset: element order within a document is not stable across runs, and
     * {@code stats} output order is unspecified, so a sequence comparison would be flaky.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private void assertRowsEqualUnordered(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actual = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actual);
        List<String> want = new ArrayList<>();
        for (List<Object> r : Arrays.asList(expected)) {
            want.add(normalize(r));
        }
        List<String> got = new ArrayList<>();
        for (List<Object> r : actual) {
            got.add(normalize(r));
        }
        want.sort(null);
        got.sort(null);
        assertEquals("Rows mismatch for query: " + ppl, want, got);
    }

    /** Renders a row so numeric widths compare equal (a bigint may arrive as Integer or Long). */
    private static String normalize(List<Object> row) {
        StringBuilder sb = new StringBuilder();
        for (Object v : row) {
            sb.append('|');
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number n) {
                double d = n.doubleValue();
                sb.append(d == Math.rint(d) ? Long.toString((long) d) : Double.toString(d));
            } else {
                sb.append(v);
            }
        }
        return sb.toString();
    }
}
