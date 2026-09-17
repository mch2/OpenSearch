/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.multivalue;

import org.opensearch.client.Request;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Builds the composite parquet index the multi-value experiments run against.
 *
 * <p>Every covered OpenSearch type gets a matched pair of columns: {@code <t>_s} mapped normally
 * (Parquet scalar column) and {@code <t>_m} mapped {@code multi_value: true} (Parquet
 * {@code LIST<element>} column). Both hold the same logical values, so any behavior difference
 * between a probe run against {@code _s} and the same probe against {@code _m} is attributable to
 * cardinality alone — that is the whole point of the harness.
 *
 * <p>{@code multi_value: true} is declared at index creation rather than reached by auto-promotion.
 * A promoted field leaves pre-promotion scalar files and post-promotion LIST files side by side in
 * the same shard, and reconciling those two physical schemas at read time is a separate problem
 * (tracked upstream). Declaring up front keeps every file LIST so the matrix measures function
 * semantics, not schema reconciliation. {@link #promoteField} exercises the promotion route on its
 * own index for the tests that want it.
 */
public final class MultiValueDataset {

    /** One covered type: its OpenSearch mapping type plus the scalar/multi column name pair. */
    public record TypeSpec(String osType, String scalarField, String multiField) {
        public static TypeSpec of(String osType, String prefix) {
            return new TypeSpec(osType, prefix + "_s", prefix + "_m");
        }
    }

    /**
     * The covered types — every Parquet-backed type, each with a {@code LIST} writer. {@code text} is
     * included: an array of strings is the shape dynamic mapping produces for a JSON string array, so
     * it is the most common multi-value column there is, not an edge case.
     */
    public static final List<TypeSpec> TYPES = List.of(
        TypeSpec.of("keyword", "kw"),
        TypeSpec.of("long", "lng"),
        TypeSpec.of("integer", "intg"),
        TypeSpec.of("double", "dbl"),
        TypeSpec.of("boolean", "bool"),
        TypeSpec.of("date", "dt"),
        TypeSpec.of("ip", "ip"),
        TypeSpec.of("text", "txt")
    );

    public static final String INDEX = "mv_matrix";

    /** Docs are shaped to expose every cardinality case the read path has to handle. */
    public enum DocShape {
        /** Exactly one value — the case a scalar plan would get right by accident. */
        SINGLE,
        /** Two values — the minimum that makes per-element vs per-array semantics distinguishable. */
        PAIR,
        /** Three values including a duplicate — separates dedup semantics from cardinality. */
        TRIPLE_WITH_DUPLICATE,
        /** Explicit {@code []} — must read back identically to an absent field. */
        EMPTY_ARRAY,
        /** Field absent — the null-list case. */
        MISSING
    }

    private static final List<DocShape> DOCS = List.of(
        DocShape.SINGLE,
        DocShape.PAIR,
        DocShape.TRIPLE_WITH_DUPLICATE,
        DocShape.EMPTY_ARRAY,
        DocShape.MISSING,
        // A second SINGLE doc sharing values with the first, so `stats count() by <f>` produces a
        // bucket with count 2 and a group-by that silently keys on the whole array is visible as a
        // count difference rather than needing element-level inspection.
        DocShape.SINGLE
    );

    private MultiValueDataset() {}

    /** Creates {@link #INDEX} (deleting any previous copy) and ingests {@link #DOCS}. */
    public static void provision(RestClient client) throws IOException {
        recreate(client, INDEX, TYPES);
        bulk(client, INDEX, TYPES);
        awaitGreen(client, INDEX);
    }

    /**
     * Creates an index where {@code field} starts as an ordinary scalar keyword, then indexes a
     * two-value document so the mapper's auto-promotion publishes the scalar → LIST mapping update.
     * Returns the index name. Used to measure the mixed-generation read path: the first flush holds
     * a scalar column and the second a LIST column.
     */
    public static String promoteField(RestClient client, String indexName, String field) throws IOException {
        deleteIfExists(client, indexName);
        String body = "{\"settings\":{"
            + settings(1)
            + "},\"mappings\":{\"properties\":{\""
            + field
            + "\":{\"type\":\"keyword\"},\"id\":{\"type\":\"integer\"}}}}";
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity(body);
        client.performRequest(create);

        // Generation 1: scalar values only, flushed on its own so it lands as a scalar-column file.
        bulkAndFlush(client, indexName, "{\"index\":{}}\n{\"id\":1,\"" + field + "\":\"alpha\"}\n");
        // Generation 2: the multi-value document that triggers promotion.
        bulkAndFlush(client, indexName, "{\"index\":{}}\n{\"id\":2,\"" + field + "\":[\"beta\",\"gamma\"]}\n");
        awaitGreen(client, indexName);
        return indexName;
    }

    private static void recreate(RestClient client, String indexName, List<TypeSpec> types) throws IOException {
        deleteIfExists(client, indexName);
        StringBuilder props = new StringBuilder("\"id\":{\"type\":\"integer\"}");
        for (TypeSpec type : types) {
            props.append(",\"").append(type.scalarField()).append("\":{\"type\":\"").append(type.osType()).append("\"}");
            props.append(",\"")
                .append(type.multiField())
                .append("\":{\"type\":\"")
                .append(type.osType())
                .append("\",\"multi_value\":true}");
        }
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity("{\"settings\":{" + settings(1) + "},\"mappings\":{\"properties\":{" + props + "}}}");
        client.performRequest(create);
    }

    /**
     * Single shard: the matrix is about per-row value semantics, and a single shard removes
     * partial-aggregate merging as a confound when a probe returns a surprising row count.
     */
    private static String settings(int shards) {
        return "\"index.pluggable.dataformat.enabled\":true,"
            + "\"index.pluggable.dataformat\":\"composite\","
            + "\"index.composite.primary_data_format\":\"parquet\","
            + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
            + "\"number_of_shards\":"
            + shards
            + ",\"number_of_replicas\":0";
    }

    private static void bulk(RestClient client, String indexName, List<TypeSpec> types) throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < DOCS.size(); i++) {
            ndjson.append("{\"index\":{}}\n").append(document(i + 1, DOCS.get(i), types)).append('\n');
        }
        bulkAndFlush(client, indexName, ndjson.toString());
    }

    private static String document(int id, DocShape shape, List<TypeSpec> types) {
        List<String> fields = new ArrayList<>();
        fields.add("\"id\":" + id);
        for (TypeSpec type : types) {
            List<String> values = values(type.osType(), shape);
            if (shape == DocShape.MISSING) {
                continue;
            }
            // The scalar column takes the first value (or is omitted for the empty case) so the pair
            // stays comparable: probes against _s see the same leading value the _m list starts with.
            if (values.isEmpty() == false) {
                fields.add("\"" + type.scalarField() + "\":" + values.get(0));
            }
            fields.add("\"" + type.multiField() + "\":[" + String.join(",", values) + "]");
        }
        return "{" + String.join(",", fields) + "}";
    }

    /** Literal JSON values per type and shape, ordered so element order is observable in results. */
    private static List<String> values(String osType, DocShape shape) {
        List<String> pool = switch (osType) {
            case "keyword", "text" -> List.of("\"alpha\"", "\"beta\"", "\"alpha\"");
            case "long" -> List.of("10", "20", "10");
            case "integer" -> List.of("1", "2", "1");
            case "double" -> List.of("1.5", "2.5", "1.5");
            case "boolean" -> List.of("true", "false", "true");
            case "date" -> List.of("\"2026-01-01T00:00:00Z\"", "\"2026-06-15T12:30:00Z\"", "\"2026-01-01T00:00:00Z\"");
            case "ip" -> List.of("\"10.0.0.1\"", "\"192.168.1.1\"", "\"10.0.0.1\"");
            default -> throw new IllegalArgumentException("no sample values for type [" + osType + "]");
        };
        return switch (shape) {
            case SINGLE -> pool.subList(0, 1);
            case PAIR -> pool.subList(0, 2);
            case TRIPLE_WITH_DUPLICATE -> pool;
            case EMPTY_ARRAY, MISSING -> List.of();
        };
    }

    private static void bulkAndFlush(RestClient client, String indexName, String ndjson) throws IOException {
        Request bulk = new Request("POST", "/" + indexName + "/_bulk");
        bulk.setJsonEntity(ndjson);
        bulk.addParameter("refresh", "true");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        assertEquals("bulk into [" + indexName + "] failed", 200, client.performRequest(bulk).getStatusLine().getStatusCode());
        Request flush = new Request("POST", "/" + indexName + "/_flush");
        flush.addParameter("force", "true");
        client.performRequest(flush);
    }

    private static void deleteIfExists(RestClient client, String indexName) {
        try {
            client.performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception e) {
            // absent — nothing to delete
        }
    }

    private static void awaitGreen(RestClient client, String indexName) throws IOException {
        Request health = new Request("GET", "/_cluster/health/" + indexName);
        health.addParameter("wait_for_status", "green");
        health.addParameter("wait_for_active_shards", "all");
        health.addParameter("wait_for_no_initializing_shards", "true");
        health.addParameter("timeout", "60s");
        client.performRequest(health);
    }
}
