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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * PPL access to OpenSearch {@code object} fields — leaves via dotted paths
 * ({@code city.location.latitude}), whole objects, and objects as group keys. Mirrors the sql repo's
 * {@code ObjectFieldOperateIT}.
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
        // This test treats latitude as a double, not geo point.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.location.latitude > 40 | fields city.name",
            row("Seattle"),
            row("Portland")
        );
    }

    // ── Object-parent projection ───────────────────────────────────────────────
    //
    // Projecting an object parent (top-level "city" or intermediate "city.location")
    // returns the nested object. No query-then-fetch / _source read is needed: the object is
    // stored as a Parquet struct, so the scan reads it as one column and the value comes back
    // assembled.

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

    // ── Aggregation involving object fields ───────────────────────────────────
    //
    // Leaf aggregations (min/max/sum on city.population, city.location.latitude, …) are covered
    // above. These cover aggregating on the OBJECT VALUE itself — the group key is the struct
    // column the scan reads.

    /** Group by an intermediate object ({@code city.location}) — 3 distinct locations. */
    public void testGroupByIntermediateObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats count() by city.location", 3);
    }

    /** Group by a top-level object ({@code city}) — 3 distinct cities. */
    public void testGroupByTopLevelObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats count() by city", 3);
    }

    /** Aggregate a leaf while grouping by an object value. */
    public void testAggregateLeafGroupedByObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats max(city.population) by city.location", 3);
    }

    // ── helpers (mirrored from FieldsCommandIT) ────────────────────────────────

    /** Asserts only the row count — group order is not deterministic for a struct key. */
    private void assertRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected, actualRows.size());
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
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



    // ── select * ──────────────────────────────────────────────────────────────────────
    //
    // Nothing here names an object, so coverage depends entirely on how `*` expands. Verified
    // against the legacy engine on the same mapping: three top-level fields, objects as nested
    // JSON. The flat dotted leaves must NOT appear — an object's data is returned once, not twice.

    /** {@code source=idx} with no field list: objects come back as whole nested values. */
    public void testSelectStarReturnsObjectsAsNestedStructs() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | head 1");
        assertStarShape(response, "source=... | head 1");
    }

    /** Explicit {@code fields *} must behave identically to the implicit form above. */
    public void testFieldsStarReturnsObjectsAsNestedStructs() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | fields * | head 1");
        assertStarShape(response, "source=... | fields * | head 1");
    }

    /**
     * Asserts the star-expansion contract: exactly the top-level fields (no dotted leaves), with
     * each object materialized as a nested map. Column order is not asserted — it is not part of
     * the contract and differs from legacy — so the row is checked by column name.
     */
    private void assertStarShape(Map<String, Object> response, String context) {
        List<String> columns = extractColumnNames(response);
        assertEquals(
            "star expansion must yield only top-level fields (no dotted leaves) for " + context,
            List.of("account", "city", "id"),
            columns.stream().sorted().toList()
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("missing datarows for " + context, rows);
        assertEquals("expected a single row for " + context, 1, rows.size());
        Map<String, Object> row = new java.util.HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), rows.get(0).get(i));
        }

        assertEquals("id for " + context, "1", row.get("id"));
        assertEquals(
            "account must be a whole nested object for " + context,
            Map.of("owner", "alice", "balance", 1000.5),
            row.get("account")
        );
        // Nested sub-object arrives nested, not flattened to a dotted key.
        assertEquals(
            "city must nest location for " + context,
            Map.of(
                "name",
                "Seattle",
                "population",
                750000,
                "location",
                Map.of("latitude", 47.6062, "longitude", -122.3321)
            ),
            row.get("city")
        );
    }

    /**
     * A shapeless {@code {"type": "object"}} — no {@code properties}, which is what dynamic mapping
     * leaves before any document populates it — is addressable and resolves to null, as vanilla does.
     * The schema gives it a field-less ROW, so this is also the end-to-end check that such a type
     * survives Substrait serialization and DataFusion rather than only the schema builder.
     */
    public void testShapelessObjectResolvesToNull() throws IOException {
        String index = "shapeless_object_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"attrs\":{\"type\":\"object\"}}}}"
        );
        client().performRequest(create);
        // No custom _id: parquet indices are append-only and reject one.
        Request doc = new Request("POST", "/" + index + "/_bulk?refresh=true");
        doc.setJsonEntity("{\"index\":{}}\n{\"id\":\"1\"}\n");
        doc.setOptions(doc.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(doc);

        assertRowsEqual("source=" + index + " | fields attrs", row((Object) null));
        assertRowsEqual("source=" + index + " | fields id, attrs", row("1", null));
    }

    /**
     * An index whose ONLY mapped field is an object, with {@code dynamic: false} so nothing else is
     * ever added. Every query against it used to fail with {@code No backend can scan all requested
     * fields}: the materializer had no leaves to read, so it left the struct column in the scan, and
     * no backend can claim a column that has no storage. It now strips the struct regardless, leaving
     * a zero-column scan.
     *
     * <p>Known gap, deliberately not asserted here: a scalar aggregate on such an index while it is
     * still <em>empty</em> returns zero rows rather than one row containing 0. That needs all of —
     * every field an object, zero documents, and a scalar aggregate — and resolves on first ingest.
     * Cause: with no fields requested, {@code OpenSearchTableScanRule}'s viability loop never runs, so
     * {@code metadataOnlyCoversAny} stays false and the metadata driver (lucene) is vetoed for
     * covering no field, when vacuously it covers everything and a metadata-driven count is exactly
     * what is wanted. An index with an ordinary column keeps lucene and correctly returns 0.
     */
    public void testObjectOnlyIndexIsQueryable() throws IOException {
        String index = "object_only_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"dynamic\":false,"
                + "\"properties\":{\"meta\":{\"type\":\"object\"}}}}"
        );
        client().performRequest(create);

        // Empty index: no documents, so no rows — and no error, which is the point.
        assertRowsEqual("source=" + index + " | fields meta");

        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n{\"meta\":{\"x\":1}}\n");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // The object is unmapped inside (dynamic: false), so it has no leaves and resolves to null.
        assertRowsEqual("source=" + index + " | fields meta", row((Object) null));
        assertRowsEqual("source=" + index + " | stats count()", row(1));
    }

    /**
     * {@code isnull} / {@code isnotnull} on an object column. Answered by the struct's own validity
     * bit, which the writer sets only for a document that carried the object.
     *
     * <p>Both used to be no-ops: the object was assembled at query time by a struct constructor,
     * which builds no validity buffer, so the struct was never null — isnotnull always true, isnull
     * always false, while the same row rendered as null. Filtering on a leaf gave the right answer,
     * so it was easy to miss.
     */
    public void testNullPredicatesOnObjectField() throws IOException {
        String index = "objrev_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"node\":{\"properties\":{\"name\":{\"type\":\"keyword\"},"
                + "\"env\":{\"type\":\"keyword\"}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"1\",\"node\":{\"name\":\"svc-a\",\"env\":\"prod\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"2\",\"node\":{\"name\":\"svc-a\",\"env\":\"prod\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"3\",\"node\":{\"name\":\"svc-b\",\"env\":\"dev\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"4\"}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // Three docs populate `node`; doc 4 has none. The predicate must agree with what the row
        // renders as — the contradiction was the tell.
        assertRowsEqual("source=" + index + " | where isnotnull(node) | stats count()", row(3));
        assertRowsEqual("source=" + index + " | where isnull(node) | stats count()", row(1));
        // Leaf form was always right; pinned so the two can't drift apart again.
        assertRowsEqual("source=" + index + " | where isnotnull(node.name) | stats count()", row(3));
        // And the rendering that contradicted the old predicate.
        assertRowsEqual("source=" + index + " | where isnull(node) | fields id, node", row("4", null));
    }

    // ── aliasing an object column ─────────────────────────────────────────────────────
    //
    // Substrait carries schema names as one depth-first list; the top-level name comes from the
    // RelRoot field (which may be an alias) while nested names come from the struct type. These pin
    // that an alias renames the column without disturbing the object's own field names — there is no
    // alias for those, so any other behaviour would be wrong.

    /** {@code rename} on an object: the column is renamed, the nested value untouched. */
    public void testRenamedObjectKeepsItsNestedShape() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | rename city as c | fields c | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    /** Same via {@code eval}, which reaches the alias by a different path than rename. */
    public void testEvalAliasOfObjectKeepsItsNestedShape() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval c = city | fields c | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    /**
     * A bare {@code {"type": "object"}} that gains its leaves from the documents rather than the
     * mapping. Worth its own test because the leaves are not the types an explicit mapping gives: a
     * dynamically-mapped string becomes {@code text} with {@code store: true}, not {@code keyword},
     * so the struct's field types differ from the declared case — and the object is only as wide as
     * the documents made it.
     */
    public void testDynamicallyHydratedObjectIsAddressable() throws IOException {
        String index = "dyn_hydrated_object_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"attrs\":{\"type\":\"object\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n{\"id\":\"1\",\"attrs\":{\"a\":\"x\",\"n\":7}}\n");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        assertRowsEqual("source=" + index + " | fields attrs", row(Map.of("a", "x", "n", 7)));
        assertRowsEqual("source=" + index + " | fields attrs.a, attrs.n", row("x", 7));
        assertRowCount("source=" + index + " | stats count() by attrs", 1);
    }


    /**
     * Null semantics with a document that has no object at all, and one where only part of the
     * object is populated. Pins that the predicate, the aggregate, and the rendered value all agree —
     * they are computed by three different mechanisms, so they can drift apart. The rule is that an
     * object is null exactly when every one of its leaves is null — {@code owner-only} and
     * {@code branch-only} each populate a different single leaf, so neither a struct that is always
     * valid nor one whose nullness tracks one particular leaf would produce these counts:
     *
     * <ul>
     *   <li>rendering: {@code ArrowValues.structToMap} skips null children and returns null when the
     *       resulting map is empty, recursively for sub-objects;</li>
     *   <li>{@code isnull} / {@code isnotnull}: read from the struct column's validity, which the
     *       writer sets per row for the documents that carried the object;</li>
     *   <li>{@code count(object)}: counts non-null values of the struct column.</li>
     * </ul>
     *
     * <p>The documents are named for their shape: {@code both} populates the object and its sub-object,
     * {@code neither} omits the object entirely, and {@code owner-only} has the scalar but no
     * sub-object — the partially-populated case at both levels.
     */
    public void testNullSemanticsWithDocumentMissingTheObject() throws IOException {
        String index = "object_null_semantics_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"account\":{\"properties\":{\"owner\":{\"type\":\"keyword\"},"
                + "\"branch\":{\"properties\":{\"code\":{\"type\":\"keyword\"}}}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"both\",\"account\":{\"owner\":\"alice\",\"branch\":{\"code\":\"NYC\"}}}\n"
                + "{\"index\":{}}\n{\"id\":\"neither\"}\n"
                + "{\"index\":{}}\n{\"id\":\"owner-only\",\"account\":{\"owner\":\"bob\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"branch-only\",\"account\":{\"branch\":{\"code\":\"LAX\"}}}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // An object with no populated leaf renders as null, not as a struct of nulls.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, account",
            row("both", Map.of("owner", "alice", "branch", Map.of("code", "NYC"))),
            row("branch-only", Map.of("branch", Map.of("code", "LAX"))),
            row("neither", null),
            row("owner-only", Map.of("owner", "bob"))
        );
        // ...and the same recursively: doc 3's sub-object has no leaves at all.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, account.branch",
            row("both", Map.of("code", "NYC")),
            row("branch-only", Map.of("code", "LAX")),
            row("neither", null),
            row("owner-only", null)
        );

        // The predicate must agree with the rendering rather than with the struct's validity.
        assertRowsEqual("source=" + index + " | where isnotnull(account) | stats count()", row(3));
        assertRowsEqual("source=" + index + " | where isnull(account) | stats count()", row(1));
        assertRowsEqual("source=" + index + " | where isnotnull(account.branch) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | where isnull(account.branch) | stats count()", row(2));

        // And so must the aggregate. 3 of 4, which pins the rule as "non-null when ANY leaf is":
        // an always-valid struct would count 4, and summing populated leaves would count 5.
        assertRowsEqual("source=" + index + " | stats count(account)", row(3));
        assertRowsEqual("source=" + index + " | stats count()", row(4));
    }


    /**
     * Three levels of sub-object, each populated independently — where a prune-empty-levels rule can
     * go wrong in either direction: dropping a level that has populated descendants, or keeping one
     * that has none.
     *
     * <p>The shape is {@code company.address.geo}, with a scalar at each level:
     *
     * <pre>
     * company.name           company.address.city           company.address.geo.country
     * </pre>
     *
     * <p>full     — every level populated
     * <br>geo-only — <em>only</em> the deepest leaf, so {@code address} and {@code geo} must survive
     *               despite having no scalar of their own
     * <br>name-only — only the top scalar, so both sub-levels must vanish
     * <br>empty    — nothing at all
     */
    public void testNestedSubObjectsPruneOnlyEmptyLevels() throws IOException {
        String index = "object_deep_nesting_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"company\":{\"properties\":{\"name\":{\"type\":\"keyword\"},"
                + "\"address\":{\"properties\":{\"city\":{\"type\":\"keyword\"},"
                + "\"geo\":{\"properties\":{\"country\":{\"type\":\"keyword\"}}}}}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"full\",\"company\":{\"name\":\"Acme\","
                + "\"address\":{\"city\":\"Seattle\",\"geo\":{\"country\":\"US\"}}}}\n"
                + "{\"index\":{}}\n{\"id\":\"geo-only\",\"company\":{\"address\":{\"geo\":{\"country\":\"JP\"}}}}\n"
                + "{\"index\":{}}\n{\"id\":\"name-only\",\"company\":{\"name\":\"Solo\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"empty\"}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // geo-only keeps address and geo though neither has a scalar; name-only loses both.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company",
            row("empty", null),
            row("full", Map.of("name", "Acme", "address", Map.of("city", "Seattle", "geo", Map.of("country", "US")))),
            row("geo-only", Map.of("address", Map.of("geo", Map.of("country", "JP")))),
            row("name-only", Map.of("name", "Solo"))
        );
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company.address",
            row("empty", null),
            row("full", Map.of("city", "Seattle", "geo", Map.of("country", "US"))),
            row("geo-only", Map.of("geo", Map.of("country", "JP"))),
            row("name-only", null)
        );
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company.address.geo",
            row("empty", null),
            row("full", Map.of("country", "US")),
            row("geo-only", Map.of("country", "JP")),
            row("name-only", null)
        );

        // Predicates and aggregates agree with that rendering at depth.
        assertRowsEqual("source=" + index + " | where isnotnull(company.address.geo) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | where isnull(company.address) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | stats count(company.address)", row(2));
    }

    /**
     * The null test on an intermediate object, where the expansion has to descend into a sub-object's
     * leaves rather than test the sub-struct. Every fixture doc populates {@code city.location}, so
     * the predicate must keep all 3 and its negation none — a no-op would have kept all 3 either way,
     * which is why the negation is asserted too.
     */
    public void testNullPredicatesOnIntermediateObjectField() throws IOException {
        assertRowsEqual("source=" + DATASET.indexName + " | where isnotnull(city.location) | stats count()", row(3));
        assertRowsEqual("source=" + DATASET.indexName + " | where isnull(city.location) | stats count()", row(0));
        // ...and on the top-level object that contains it.
        assertRowsEqual("source=" + DATASET.indexName + " | where isnotnull(city) | stats count()", row(3));
    }

    /**
     * An array of objects must keep each element's fields together.
     *
     * <p>The two documents differ only in <em>which</em> element carries {@code time}, so listing
     * {@code events} must tell them apart. Storing an object's leaves as independent lists —
     * {@code STRUCT<name LIST, time LIST>} — cannot: both collapse to
     * {@code {name:[a,b], time:[2]}}, and "which event happened at time 2" stops having an answer.
     *
     * <p>Ragged elements are the normal case for OpenTelemetry {@code events} and {@code links},
     * whose attribute keys vary per element, so this is not a corner case. The shape that
     * distinguishes them is {@code LIST<STRUCT<name, time>>}, where an element that omitted a key
     * carries a null for it inside that element.
     */
    public void testArrayOfObjectsKeepsElementsTogether() throws IOException {
        String index = "object_array_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                // `events` is left to dynamic mapping, which is how an OTel template arrives: the
                // array shape is discovered from the first document, not declared.
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            // Element 0 has only `name`; element 1 has both.
            "{\"index\":{}}\n{\"id\":\"1\",\"events\":[{\"name\":\"a\"},{\"name\":\"b\",\"time\":2}]}\n"
                // The mirror image: `time` sits on element 0 instead.
                + "{\"index\":{}}\n{\"id\":\"2\",\"events\":[{\"name\":\"a\",\"time\":2},{\"name\":\"b\"}]}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        assertRowsEqual(
            "source=" + index + " | sort id | fields id, events",
            row("1", List.of(Map.of("name", "a"), Map.of("name", "b", "time", 2))),
            row("2", List.of(Map.of("name", "a", "time", 2), Map.of("name", "b")))
        );
    }


    /**
     * The OpenTelemetry span shape: {@code events} is an array of objects, each with its own
     * {@code attributes} object whose keys vary per element. Nothing is declared — the array shape
     * and every attribute key arrive by dynamic mapping, as they do from a real collector.
     */
    public void testOtelSpanEventsWithPerElementAttributes() throws IOException {
        String index = "otel_span_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"traceId\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n"
                + "{\"traceId\":\"t1\",\"events\":["
                + "{\"name\":\"exception\",\"attributes\":{\"http_method\":\"GET\"}},"
                + "{\"name\":\"retry\",\"attributes\":{\"db_statement\":\"select 1\"}}]}\n"
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t2\",\"events\":[{\"name\":\"exception\",\"attributes\":{\"http_method\":\"POST\"}}]}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // Each event keeps its own attributes: `exception` has the http_method, `retry` has the
        // db_statement. A key an element did not carry is absent from that element, exactly as a key
        // a document did not carry is absent from a plain object in _source — not rendered as "".
        assertRowsEqual(
            "source=" + index + " | sort traceId | fields traceId, events",
            row(
                "t1",
                List.of(
                    Map.of("name", "exception", "attributes", Map.of("http_method", "GET")),
                    Map.of("name", "retry", "attributes", Map.of("db_statement", "select 1"))
                )
            ),
            row("t2", List.of(Map.of("name", "exception", "attributes", Map.of("http_method", "POST"))))
        );
    }


    /**
     * An absent array, an empty array, and an array of one stay three different things.
     *
     * <p>{@code null} and {@code []} are distinguished by the list's own validity bit, the same
     * distinction the struct validity bit draws for a plain object. Collapsing them would make
     * "this span reported no events" indistinguishable from "this span had no events field".
     */
    public void testAbsentAndEmptyArraysOfObjectsStayDistinct() throws IOException {
        String index = "object_array_empty_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"1\",\"events\":[{\"name\":\"a\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"2\",\"events\":[]}\n"
                + "{\"index\":{}}\n{\"id\":\"3\"}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        assertRowsEqual(
            "source=" + index + " | sort id | fields id, events",
            row("1", List.of(Map.of("name", "a"))),
            row("2", List.of()),
            row("3", null)
        );
    }


    /**
     * A shard whose files predate a sub-field the mapping later gained still binds.
     *
     * <p>The plan's schema comes from the cluster-state mapping — the union of every field ever seen
     * — while the shard's schema is inferred from its parquet files, which only contain what their
     * own documents carried. For a struct the child list is the type, so an object whose child one
     * file lacks is a type mismatch on a column present in both, not a missing column. Flushing
     * between the two documents forces the first file to be written before {@code y} exists.
     */
    public void testObjectSubFieldAddedAfterAFileWasWritten() throws IOException {
        String index = "object_grow_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);

        // A pluggable-format index is append-only, so documents cannot carry a custom _id.
        bulkIndex(index, "{\"index\":{}}\n{\"id\":\"1\",\"a\":{\"x\":1}}\n");
        // Close the file before `y` exists, so the shard has one file that never saw it.
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        bulkIndex(index, "{\"index\":{}}\n{\"id\":\"2\",\"a\":{\"x\":1,\"y\":2}}\n");
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        assertRowsEqual(
            "source=" + index + " | sort id | fields id, a",
            row("1", Map.of("x", 1)),
            row("2", Map.of("x", 1, "y", 2))
        );
        // The leaf the older file never saw reads as null there rather than failing the scan.
        assertRowsEqual("source=" + index + " | where isnull(a.y) | stats count()", row(1));
        assertRowsEqual("source=" + index + " | stats count(a.y)", row(1));
    }


    /**
     * The same divergence one level deeper: an array of objects whose elements gained an attribute
     * after a file was written.
     *
     * <p>`events` is a {@code LIST<STRUCT<..>>}, so an attribute only later documents carry is a
     * child of the element struct — a type mismatch on a column present in both schemas, exactly as
     * for a plain object. This is what OpenTelemetry hits immediately, since an exception event
     * carries keys an ordinary event does not.
     */
    public void testArrayElementAttributeAddedAfterAFileWasWritten() throws IOException {
        String index = "object_array_grow_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"traceId\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);

        bulkIndex(index, "{\"index\":{}}\n{\"traceId\":\"t1\",\"events\":[{\"name\":\"ok\"}]}\n");
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        // An exception event carries a key the first file never saw.
        bulkIndex(
            index,
            "{\"index\":{}}\n{\"traceId\":\"t2\",\"events\":[{\"name\":\"exception\",\"reason\":\"boom\"}]}\n"
        );
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        assertRowsEqual(
            "source=" + index + " | sort traceId | fields traceId, events",
            row("t1", List.of(Map.of("name", "ok"))),
            row("t2", List.of(Map.of("name", "exception", "reason", "boom")))
        );
    }

    /**
     * The divergence across shards rather than across files: each shard binds its own inferred schema
     * against the one plan, so a shard that never saw a sub-field must widen its own.
     */
    public void testObjectSubFieldMissingOnOneShard() throws IOException {
        String index = "object_shard_grow_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":3,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        // Auto-generated ids spread these over the shards; only some documents carry `y`.
        StringBuilder body = new StringBuilder();
        for (int i = 1; i <= 6; i++) {
            body.append("{\"index\":{}}\n");
            body.append("{\"id\":\"").append(i).append("\",\"a\":{\"x\":").append(i);
            if (i % 2 == 0) {
                body.append(",\"y\":").append(i * 10);
            }
            body.append("}}\n");
        }
        bulkIndex(index, body.toString());
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        assertRowsEqual("source=" + index + " | stats count()", row(6));
        assertRowsEqual("source=" + index + " | stats count(a.y)", row(3));
        assertRowsEqual("source=" + index + " | where isnull(a.y) | stats count()", row(3));
    }


    /**
     * Indexes an ndjson bulk body and fails on any item error.
     *
     * <p>A pluggable-format index sets {@code index.append_only.enabled}, which rejects a custom
     * {@code _id}, so every action line must be a bare {@code {"index":{}}}. Asserting on the
     * response matters because a bulk reports per-item failures with a 200 overall — a silently empty
     * index otherwise shows up later as a a mystified count assertion.
     */
    private void bulkIndex(String index, String ndjson) throws IOException {
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(ndjson);
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        Response response = client().performRequest(bulk);
        String body = new String(response.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat("bulk reported item errors: " + body, body, org.hamcrest.Matchers.containsString("\"errors\":false"));
    }




    /**
     * Reading a leaf of an array of objects, element-wise.
     *
     * <p>An array-valued object is declared only as {@code ARRAY<ROW<..>>}, so {@code events.name} has
     * no scalar column to resolve against and Calcite produces {@code ITEM($events,'name')}, which
     * {@code OpenSearchNestedFieldRewriter} turns into an element-wise projection (one array per row)
     * or an existential filter (element-scoped). Declaring the leaf as a flat dotted column instead
     * would let it resolve to a column that no longer exists physically, and read null.
     */
    public void testArrayOfObjectsLeafIsReadElementWise() throws IOException {
        String index = "object_array_leaf_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
        bulkIndex(
            index,
            "{\"index\":{}}\n{\"id\":\"1\",\"events\":[{\"name\":\"a\"},{\"name\":\"b\"}]}\n"
                + "{\"index\":{}}\n{\"id\":\"2\",\"events\":[{\"name\":\"c\"}]}\n"
        );

        // One array per row, in element order — not a scalar, and not null.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, events.name",
            row("1", List.of("a", "b")),
            row("2", List.of("c"))
        );
        // Existential over the elements: doc 1 has an event named `a`, doc 2 does not.
        assertRowsEqual("source=" + index + " | where events.name='a' | stats count()", row(1));
        assertRowsEqual("source=" + index + " | where events.name='c' | stats count()", row(1));
        assertRowsEqual("source=" + index + " | where events.name='zzz' | stats count()", row(0));
    }


    /**
     * Two files whose object children were first seen in a different order.
     *
     * <p>{@code DataType::Struct} equality compares the ordered child list, so identical children in
     * a different order are a different type. The plan's order comes from the cluster-state mapping,
     * which is sorted; a file's is the order its writer first saw each key. Flushing between two
     * documents that introduce the keys in opposite orders is what makes the two disagree — for
     * OpenTelemetry attributes it is the normal case, not a corner.
     */
    public void testObjectChildrenFirstSeenInDifferentOrders() throws IOException {
        String index = "object_order_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);

        // File 1 sees zeta first, then alpha.
        bulkIndex(index, "{\"index\":{}}\n{\"id\":\"1\",\"attrs\":{\"zeta\":\"z1\",\"alpha\":\"a1\"}}\n");
        client().performRequest(new Request("POST", "/" + index + "/_flush"));
        // File 2 introduces a third key, so its child order differs again.
        bulkIndex(index, "{\"index\":{}}\n{\"id\":\"2\",\"attrs\":{\"mid\":\"m2\",\"alpha\":\"a2\",\"zeta\":\"z2\"}}\n");
        client().performRequest(new Request("POST", "/" + index + "/_flush"));

        assertRowsEqual(
            "source=" + index + " | sort id | fields id, attrs",
            row("1", Map.of("zeta", "z1", "alpha", "a1")),
            row("2", Map.of("mid", "m2", "alpha", "a2", "zeta", "z2"))
        );
        // Values must land on the right leaf, not be permuted with a sibling.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, attrs.alpha, attrs.zeta",
            row("1", "a1", "z1"),
            row("2", "a2", "z2")
        );
        assertRowsEqual("source=" + index + " | stats count(attrs.mid)", row(1));
    }

}
