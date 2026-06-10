/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteDataTypeIT} (extends {@code DataTypeIT}) on the
 * analytics-engine route. Uses {@code datatypes_numeric} and {@code datatypes_nonnumeric} datasets.
 */
public class CalciteDataTypeReproIT extends CalciteReproTestCase {

    private static final Dataset NUM = new Dataset("datatypes_numeric", "repro_dt_numeric");
    private static final Dataset NONNUM = new Dataset("datatypes_nonnumeric", "repro_dt_nonnumeric");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), NUM);
        DatasetProvisioner.provision(client(), NONNUM);
        provisioned = true;
    }

    public void test_numeric_data_types() throws IOException {
        Map<String, Object> result = executePpl("source=" + NUM.indexName);
        verifySchema(result,
            schema("long_number", "bigint"),
            schema("integer_number", "int"),
            schema("short_number", "smallint"),
            schema("byte_number", "tinyint"),
            schema("double_number", "double"),
            schema("float_number", "float"),
            schema("half_float_number", "float"),
            schema("scaled_float_number", "double"));
    }

    public void test_nonnumeric_data_types() throws IOException {
        // BUCKET E: upstream's datatypes_nonnumeric index carries a geo_point field. On the AE
        // route, creating a parquet-primary index with a geo_point field fails at index-creation
        // (HTTP 400 "searchCapability is not supported for field ... of type: geo_point"), so the
        // upstream schema (which includes geo_point_value:geo_point) can never be produced here.
        // Reproduce the gap directly: the index create must fail with that error.
        String idx = "repro_dt_nonnumeric_geo";
        deleteIndexQuietly(idx);
        org.opensearch.client.Request create = new org.opensearch.client.Request("PUT", "/" + idx);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\","
                + "\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"geo_point_value\":{\"type\":\"geo_point\"}}}}");
        try {
            client().performRequest(create);
            fail("expected AE parquet to reject geo_point at index creation (bucket E)");
        } catch (org.opensearch.client.ResponseException re) {
            String body = entityAsString(re.getResponse());
            verifyErrorMessageContains(body, "searchCapability is not supported");
            verifyErrorMessageContains(body, "geo_point");
        } finally {
            deleteIndexQuietly(idx);
        }

        // The non-geo subset of the schema (what AE *can* serve) still validates the type labels.
        Map<String, Object> result = executePpl("source=" + NONNUM.indexName);
        verifySchema(result,
            schema("text_value", "string"),
            schema("date_nanos_value", "timestamp"),
            schema("date_value", "timestamp"),
            schema("boolean_value", "boolean"),
            schema("ip_value", "ip"),
            schema("nested_value", "array"),
            schema("object_value", "struct"),
            schema("keyword_value", "string"),
            schema("binary_value", "binary"));
    }

    public void testBooleanFieldFromString() throws Exception {
        indexDoc(NONNUM.indexName, "2",
            "{\"boolean_value\": \"true\", \"keyword_value\": \"test\"}");
        Map<String, Object> result = executePpl("source=" + NONNUM.indexName
            + " | where keyword_value='test' | fields boolean_value");
        verifySchema(result, schema("boolean_value", "boolean"));
        verifyDataRows(result, rows(true));
        deleteDoc(NONNUM.indexName, "2");
    }

    public void testBooleanFieldFromNumberAcrossWildcardIndices() throws Exception {
        // Issue #5269: wildcard across boolean-typed and text-typed indices with a numeric 0.
        String indexBool = "repro_bool_test_bb";
        String indexText = "repro_bool_test_aa";
        try {
            createParquetIndex(indexBool,
                "{\"flag\":{\"type\":\"boolean\"},\"startTime\":{\"type\":\"date_nanos\"}}");
            createParquetIndex(indexText,
                "{\"flag\":{\"type\":\"text\"},\"startTime\":{\"type\":\"date_nanos\"}}");
            indexDoc(indexBool, "1", "{\"startTime\":\"2026-03-25T20:25:00.000Z\",\"flag\":false}");
            indexDoc(indexText, "1", "{\"startTime\":\"2026-03-24T20:25:00.000Z\",\"flag\":0}");

            Map<String, Object> result = executePpl("source=repro_bool_test_* | fields flag");
            List<List<Object>> rows = dataRowsOf(result);
            assertEquals("expected 2 rows across wildcard indices", 2, rows.size());
        } finally {
            deleteIndexQuietly(indexBool);
            deleteIndexQuietly(indexText);
        }
    }

    private void deleteDoc(String index, String id) throws IOException {
        org.opensearch.client.Request del =
            new org.opensearch.client.Request("DELETE", "/" + index + "/_doc/" + id + "?refresh=true");
        client().performRequest(del);
    }
}
