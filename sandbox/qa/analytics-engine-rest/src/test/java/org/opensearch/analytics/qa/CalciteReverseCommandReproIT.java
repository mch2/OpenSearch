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
 * Reproduction of failing {@code streamstats ... | reverse} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteReverseCommandIT} on the analytics-engine route.
 * Uses {@code state_country}.
 */
public class CalciteReverseCommandReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country", "repro_rev_sc");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        provisioned = true;
    }

    private String src() { return "source=" + SC.indexName; }

    public void testStreamstatsWithReverse() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | streamstats count() as cnt, avg(age) as avg | reverse");
        verifySchema(r,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"),
            schema("cnt", "bigint"), schema("avg", "double"));
        verifyDataRowsInOrder(r,
            rows("Jake", "USA", "California", 4, 2023, 70, 1, 70),
            rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50),
            rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25));
    }

    public void testStreamstatsByWithReverse() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | streamstats count() as cnt, avg(age) as avg by country | reverse");
        verifySchema(r,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"),
            schema("cnt", "bigint"), schema("avg", "double"));
        verifyDataRowsInOrder(r,
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5),
            rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25),
            rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50),
            rows("Jake", "USA", "California", 4, 2023, 70, 1, 70));
    }

    public void testStreamstatsWindowWithReverse() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | streamstats window=2 avg(age) as avg | reverse");
        verifySchema(r,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("avg", "double"));
        verifyDataRowsInOrder(r,
            rows("Jake", "USA", "California", 4, 2023, 70, 70),
            rows("Hello", "USA", "New York", 4, 2023, 30, 50),
            rows("John", "Canada", "Ontario", 4, 2023, 25, 27.5),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5));
    }

    public void testStreamstatsWithSortThenReverse() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | streamstats count() as cnt | sort age | reverse | head 3");
        verifySchema(r,
            schema("name", "string"), schema("country", "string"), schema("state", "string"),
            schema("month", "int"), schema("year", "int"), schema("age", "int"), schema("cnt", "bigint"));
        verifyDataRowsInOrder(r,
            rows("Jake", "USA", "California", 4, 2023, 70, 1),
            rows("Hello", "USA", "New York", 4, 2023, 30, 2),
            rows("John", "Canada", "Ontario", 4, 2023, 25, 3));
    }
}
