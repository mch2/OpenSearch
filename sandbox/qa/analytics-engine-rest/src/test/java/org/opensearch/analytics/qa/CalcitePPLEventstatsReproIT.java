/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Map;

/**
 * Reproduction of failing {@code eventstats dc()/distinct_count()} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLEventstatsIT} on the analytics-engine route.
 * Uses {@code state_country} and {@code state_country_null}.
 */
public class CalcitePPLEventstatsReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country", "repro_es_sc");
    private static final Dataset SCN = new Dataset("state_country_null", "repro_es_scn");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        DatasetProvisioner.provision(client(), SCN);
        provisioned = true;
    }

    public void testEventstatsDistinctCount() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SC.indexName
            + " | eventstats dc(state) as dc_state | fields name, country, state, month, year, age, dc_state");
        verifySchemaInOrder(actual,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("dc_state", "bigint"));
        verifyDataRows(actual,
            rows("John", "Canada", "Ontario", 4, 2023, 25, 4),
            rows("Jake", "USA", "California", 4, 2023, 70, 4),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
            rows("Hello", "USA", "New York", 4, 2023, 30, 4));
    }

    public void testEventstatsDistinctCountByCountry() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SC.indexName
            + " | eventstats dc(state) as dc_state by country | fields name, country, state, month, year, age, dc_state");
        verifySchemaInOrder(actual,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("dc_state", "bigint"));
        verifyDataRows(actual,
            rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
            rows("Jake", "USA", "California", 4, 2023, 70, 2),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2),
            rows("Hello", "USA", "New York", 4, 2023, 30, 2));
    }

    public void testEventstatsDistinctCountFunction() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SC.indexName
            + " | eventstats distinct_count(country) as dc_country | fields name, country, state, month, year, age, dc_country");
        verifySchemaInOrder(actual,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("dc_country", "bigint"));
        verifyDataRows(actual,
            rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
            rows("Jake", "USA", "California", 4, 2023, 70, 2),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2),
            rows("Hello", "USA", "New York", 4, 2023, 30, 2));
    }

    public void testEventstatsDistinctCountWithNull() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SCN.indexName
            + " | eventstats dc(state) as dc_state | fields name, country, state, month, year, age, dc_state");
        verifySchemaInOrder(actual,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("dc_state", "bigint"));
        verifyDataRows(actual,
            rows(null, "Canada", null, 4, 2023, 10, 4),
            rows("Kevin", null, null, 4, 2023, null, 4),
            rows("John", "Canada", "Ontario", 4, 2023, 25, 4),
            rows("Jake", "USA", "California", 4, 2023, 70, 4),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
            rows("Hello", "USA", "New York", 4, 2023, 30, 4));
    }
}
