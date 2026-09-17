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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Filter delegation on a field inside an {@code object}, now that an object is stored as a Parquet
 * struct rather than as flat dotted columns.
 *
 * <p>A predicate on {@code city.name} has two possible answers: Lucene's inverted index at the
 * dotted name, or DataFusion reading the struct's leaf. The storage change moved the DataFusion side
 * — the leaf is no longer a column of its own — so this pins that both drivers still answer, and
 * agree. {@code prefer_metadata_driver} selects which one runs, and the SHARD_FRAGMENT profile
 * reports it, so the same oracle is asserted against each.
 */
public class ObjectFilterDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX = "object_filter_delegation_it";

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
            "{\"settings\":{"
                + "\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\"},"
                + "\"mappings\":{\"properties\":{"
                + "\"id\":{\"type\":\"keyword\"},"
                + "\"city\":{\"properties\":{"
                + "  \"name\":{\"type\":\"keyword\"},"
                + "  \"population\":{\"type\":\"long\"}}}"
                + "}}}"
        );
        client().performRequest(create);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 7; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"id\":\"s").append(i).append("\",\"city\":{\"name\":\"seattle\",\"population\":750000}}\n");
        }
        for (int i = 0; i < 3; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"id\":\"p").append(i).append("\",\"city\":{\"name\":\"portland\",\"population\":650000}}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
        provisioned = true;
    }

    /** An equality predicate on an object's keyword leaf: same answer from either driver. */
    public void testObjectLeafEqualityDelegatesAndAgrees() throws Exception {
        String ppl = "source=" + INDEX + " | where city.name='seattle' | stats count()";
        assertCountUnderBothDrivers(ppl, 7L);
    }

    /** A numeric comparison on an object's leaf, which only DataFusion can evaluate from the struct. */
    public void testObjectLeafNumericComparisonAgrees() throws Exception {
        String ppl = "source=" + INDEX + " | where city.population > 700000 | stats count()";
        assertCountUnderBothDrivers(ppl, 7L);
    }

    /** Object leaf and a parent scalar together — the mixed shape delegation has to split. */
    public void testObjectLeafWithParentPredicateAgrees() throws Exception {
        String ppl = "source=" + INDEX + " | where city.name='seattle' and id='s3' | stats count()";
        assertCountUnderBothDrivers(ppl, 1L);
    }

    /** The null predicates read the struct's own validity rather than a leaf. */
    public void testObjectPresenceAgrees() throws Exception {
        assertCountUnderBothDrivers("source=" + INDEX + " | where isnotnull(city) | stats count()", 10L);
        assertCountUnderBothDrivers("source=" + INDEX + " | where isnull(city) | stats count()", 0L);
    }

    /**
     * Runs the query with the metadata driver preferred and not, asserting the same count each time
     * and reporting which backend the shard actually chose.
     */
    private void assertCountUnderBothDrivers(String ppl, long expected) throws Exception {
        for (boolean prefer : new boolean[] { true, false }) {
            setPreferMetadataDriver(prefer);
            String backend = shardBackend(ppl);
            assertEquals("prefer_metadata_driver=" + prefer + " (chosen_backend=" + backend + ") — " + ppl, expected, count(ppl));
        }
        setPreferMetadataDriver(false);
    }

    private long count(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\":\"" + ppl.replace("\"", "\\\"") + "\"}");
        Response response = client().performRequest(request);
        String body = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"datarows\"\\s*:\\s*\\[\\s*\\[\\s*(\\d+)").matcher(body);
        assertTrue("no count in response: " + body, m.find());
        return Long.parseLong(m.group(1));
    }

    /**
     * The backend the shard fragment chose, for the failure message only — best effort.
     *
     * <p>{@code _explain} lives in the SQL plugin and its relational-type converter has no {@code ROW}
     * case, so it answers 400 for any plan carrying an {@code ARRAY<ROW>} column ("Unsupported
     * conversion for Relational Data type: ROW"). That is a reporting gap, not an execution one — the
     * query itself runs — so a failure here must not mask the assertion that follows.
     */
    private String shardBackend(String ppl) {
        try {
            Request request = new Request("POST", "/_analytics/ppl/_explain");
            request.setJsonEntity("{\"query\":\"" + ppl.replace("\"", "\\\"") + "\"}");
            Response response = client().performRequest(request);
            String body = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"chosen_backend\"\\s*:\\s*\"([^\"]+)\"").matcher(body);
            return m.find() ? m.group(1) : null;
        } catch (Exception e) {
            return "<explain unavailable: " + e.getMessage() + ">";
        }
    }

    private void setPreferMetadataDriver(boolean value) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": " + value + "}}");
        client().performRequest(req);
    }

    /**
     * Clears the driver preference after every test, pass or fail.
     *
     * <p>It is a persistent cluster setting, so leaving it set leaks into every other test class
     * sharing the cluster — a query that expects the DataFusion driver then gets pushed at Lucene and
     * fails with "Unexpected RexNode in Lucene-driver filter condition". Resetting at the end of the
     * happy path is not enough: a failing assertion skips it.
     */
    @org.junit.After
    public void clearDriverPreference() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": null}}");
        client().performRequest(req);
    }

    /**
     * The discriminating case for element scoping: a conjunction over two leaves of an array of
     * objects.
     *
     * <p>One element is {@code exception} at time 1, another is {@code ok} at time 2. Asked for
     * {@code name='exception' and time=2}, an element-scoped evaluation answers 0 — no single event
     * is both — while an evaluation over the flattened values answers 1, because *some* element is
     * exception and *some* element has time 2. Lucene's inverted index holds the flattened values
     * (the element ordinal is dropped for formats that flatten), so this pins whether a delegated
     * predicate stays element-scoped.
     */
    public void testMultiLeafElementScopedPredicate() throws Exception {
        String index = "object_array_delegation_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{"
                + "\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\"},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity(
            "{\"index\":{}}\n"
                + "{\"id\":\"1\",\"events\":[{\"name\":\"exception\",\"time\":1},{\"name\":\"ok\",\"time\":2}]}\n"
        );
        bulk.addParameter("refresh", "true");
        client().performRequest(bulk);
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));

        String ppl = "source=" + index + " | where events.name='exception' and events.time=2 | stats count()";
        for (boolean prefer : new boolean[] { true, false }) {
            setPreferMetadataDriver(prefer);
            String backend = shardBackend(ppl);
            long got = count(ppl);
            logger.info("PROBE prefer={} backend={} count={}", prefer, backend, got);
            assertEquals(
                "prefer_metadata_driver=" + prefer + " backend=" + backend
                    + " — no single event is both 'exception' and time=2, so an element-scoped answer is 0",
                0L,
                got
            );
        }
        setPreferMetadataDriver(false);
    }

    /**
     * An array-of-objects leaf ANDed with a scalar predicate that delegation takes.
     *
     * <p>The delegated conjunct splits the filter and leaves the nested one as the residual, which
     * the indexed path lowers through {@code create_physical_expr} — no {@code Filter} node, so the
     * plan-level rule that turns the {@code nested_any_match} placeholder into a native
     * {@code array_any_match} never fires there. Left unlowered the placeholder reached execution and
     * the query failed with "placeholder invoked". The nested predicate on its own, and the same
     * conjunction with a numeric scalar, both took different paths and kept working — so only this
     * shape pins it.
     */
    public void testArrayLeafWithDelegatedScalarPredicate() throws Exception {
        String index = "object_array_delegated_scalar_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{"
                + "\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\"},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity(
            "{\"index\":{}}\n"
                + "{\"id\":\"1\",\"events\":[{\"name\":\"exception\",\"time\":1},{\"name\":\"ok\",\"time\":2}]}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":\"2\",\"events\":[{\"name\":\"exception\",\"time\":5}]}\n"
        );
        bulk.addParameter("refresh", "true");
        client().performRequest(bulk);
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));

        // Both documents carry an `exception` event; the scalar conjunct is what narrows it to one.
        assertCountUnderBothDrivers("source=" + index + " | where events.name='exception' | stats count()", 2L);
        assertCountUnderBothDrivers("source=" + index + " | where events.name='exception' and id='1' | stats count()", 1L);
        // Argument order must not matter — the residual is whichever conjunct delegation did not take.
        assertCountUnderBothDrivers("source=" + index + " | where id='1' and events.name='exception' | stats count()", 1L);
        // A projection alongside the delegated filter exercises the sibling nested_project placeholder.
        assertCountUnderBothDrivers(
            "source=" + index + " | where events.name='exception' and id='1' | fields events.name | stats count()",
            1L
        );
    }

}
