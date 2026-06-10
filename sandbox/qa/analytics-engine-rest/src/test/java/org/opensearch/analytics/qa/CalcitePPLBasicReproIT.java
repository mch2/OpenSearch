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
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLBasicIT} on the analytics-engine route.
 *
 * <p>Each test mirrors the exact PPL query + expected schema/rows from the upstream test, but
 * provisions parquet-primary indices and runs through {@code POST /_plugins/_ppl} with
 * {@code cluster.pluggable.dataformat=composite} so the analytics engine serves the query.
 *
 * <p>Upstream indices reproduced here:
 * <ul>
 *   <li>{@code test}  — {name:string, age:bigint}, docs (hello,20),(world,30)</li>
 *   <li>{@code test1} — {name:string, alias:string}, doc (HELLO,Hello)</li>
 *   <li>{@code opensearch_dashboards_sample_data_bank}-equivalent {@code bank} dataset</li>
 * </ul>
 */
public class CalcitePPLBasicReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        // test: {name, age}
        createParquetIndex("repro_test",
            "{\"name\":{\"type\":\"keyword\"},\"age\":{\"type\":\"long\"}}");
        indexDoc("repro_test", "1", "{\"name\": \"hello\", \"age\": 20}");
        indexDoc("repro_test", "2", "{\"name\": \"world\", \"age\": 30}");

        // test1: {name, alias}
        createParquetIndex("repro_test1",
            "{\"name\":{\"type\":\"keyword\"},\"alias\":{\"type\":\"keyword\"}}");
        indexDoc("repro_test1", "1", "{\"name\": \"HELLO\", \"alias\": \"Hello\"}");

        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    // ── single-source ─────────────────────────────────────────────────────────

    public void testSourceQuery() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_test");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
        verifyDataRows(actual, rows("hello", 20), rows("world", 30));
    }

    public void testFilterQuery2() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_test | where age = 20 | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
        verifyDataRows(actual, rows("hello", 20));
    }

    public void testRegexpFilter() throws IOException {
        Map<String, Object> actual =
            executePpl("source=repro_test | where name REGEXP 'he.*' | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
        verifyDataRows(actual, rows("hello", 20));
    }

    // ── multi-source ────────────────────────────────────────────────────────────

    public void testMultipleSourceQuery_SameTable() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_test, repro_test");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
        verifyDataRows(actual, rows("hello", 20), rows("world", 30));
    }

    public void testMultipleSourceQuery_DifferentTables() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_test, repro_test1");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"), schema("alias", "string"));
        verifyDataRows(actual,
            rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
    }

    public void testIndexPatterns() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_test*");
        verifySchema(actual, schema("name", "string"), schema("age", "bigint"), schema("alias", "string"));
        verifyDataRows(actual,
            rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
    }

    // ── literals ──────────────────────────────────────────────────────────────

    public void testNumericLiteral() throws IOException {
        Map<String, Object> result = executePpl(
            "source=repro_test | eval decimalLiteral = 0.06 - 0.01, doubleLiteral = 0.06d - 0.01d,"
                + " floatLiteral = 0.06f - 0.01f");
        verifySchema(result,
            schema("name", "string"),
            schema("age", "bigint"),
            schema("decimalLiteral", "double"),
            schema("doubleLiteral", "double"),
            schema("floatLiteral", "float"));
        verifyDataRows(result,
            rows("hello", 20, 0.05, 0.049999999999999996, 0.049999999999999996),
            rows("world", 30, 0.05, 0.049999999999999996, 0.049999999999999996));
    }

    public void testDecimalLiteral() throws IOException {
        Map<String, Object> result = executePpl(
            "source=repro_test | eval r1 = 22 / 7.0, r2 = 22 / 7.0d, r3 = 22.0 / 7, r4 = 22.0d / 7,"
                + " r5 = 0.1 * 0.2, r6 = 0.1d * 0.2d, r7 = 0.1 + 0.2, r8 = 0.1d + 0.2d,"
                + " r9 = 0.06 - 0.01, r10 = 0.06d - 0.01d, r11 = 0.1 / 0.3 * 0.3,"
                + " r12 = 0.1d / 0.3d * 0.3d, r13 = pow(sqrt(2.0), 2), r14 = pow(sqrt(2.0d), 2),"
                + " r15 = 7.0 / 0, r16 = 7 / 0.0");
        verifyDataRows(result,
            rows("hello", 20, 3.142857142857143, 3.142857142857143, 3.142857142857143,
                3.142857142857143, 0.02, 0.020000000000000004, 0.3, 0.30000000000000004, 0.05,
                0.049999999999999996, 0.1, 0.1, 2.0000000000000004, 2.0000000000000004, null, null),
            rows("world", 30, 3.142857142857143, 3.142857142857143, 3.142857142857143,
                3.142857142857143, 0.02, 0.020000000000000004, 0.3, 0.30000000000000004, 0.05,
                0.049999999999999996, 0.1, 0.1, 2.0000000000000004, 2.0000000000000004, null, null));
    }

    // ── bank: fields exclusion ──────────────────────────────────────────────────

    public void testQueryMinusFields() throws IOException {
        Map<String, Object> actual = executePpl(
            "source=repro_bank | fields - firstname, lastname, birthdate");
        verifySchema(actual,
            schema("account_number", "bigint"),
            schema("address", "string"),
            schema("gender", "string"),
            schema("city", "string"),
            schema("balance", "bigint"),
            schema("employer", "string"),
            schema("state", "string"),
            schema("age", "int"),
            schema("email", "string"),
            schema("male", "boolean"));
        verifyDataRows(actual,
            rows(1, "880 Holmes Lane", "M", "Brogan", 39225, "Pyrami", "IL", 32, "amberduke@pyrami.com", true),
            rows(6, "671 Bristol Street", "M", "Dante", 5686, "Netagy", "TN", 36, "hattiebond@netagy.com", true),
            rows(13, "789 Madison Street", "F", "Nogal", 32838, "Quility", "VA", 28, "nanettebates@quility.com", false),
            rows(18, "467 Hutchinson Court", "M", "Orick", 4180, "Boink", "MD", 33, "daleadams@boink.com", true),
            rows(20, "282 Kings Place", "M", "Ribera", 16418, "Scentric", "WA", 36, "elinorratliff@scentric.com", true),
            rows(25, "171 Putnam Avenue", "F", "Nicholson", 40540, "Filodyne", "PA", 39, "virginiaayala@filodyne.com", false),
            rows(32, "702 Quentin Street", "F", "Veguita", 48086, "Quailcom", "IN", 34, "dillardmcpherson@quailcom.com", false));
    }

    public void testQueryMinusFieldsWithFilter() throws IOException {
        Map<String, Object> actual = executePpl(
            "source=repro_bank | where (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                + " fields - firstname, lastname");
        verifySchema(actual,
            schema("account_number", "bigint"),
            schema("address", "string"),
            schema("birthdate", "timestamp"),
            schema("gender", "string"),
            schema("city", "string"),
            schema("balance", "bigint"),
            schema("employer", "string"),
            schema("state", "string"),
            schema("age", "int"),
            schema("email", "string"),
            schema("male", "boolean"));
        verifyDataRows(actual,
            rows(1, "880 Holmes Lane", "2017-10-23 00:00:00", "M", "Brogan", 39225, "Pyrami", "IL", 32, "amberduke@pyrami.com", true),
            rows(20, "282 Kings Place", "2018-06-27 00:00:00", "M", "Ribera", 16418, "Scentric", "WA", 36, "elinorratliff@scentric.com", true));
    }

    public void testFilterQueryWithOr2() throws IOException {
        Map<String, Object> actual = executePpl(
            "source=repro_bank (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                + " fields firstname, lastname");
        verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
        verifyDataRows(actual,
            rows("Amber JOHnny", "Duke Willmington"), rows("Elinor", "Ratliff"));
    }

    // ── multi-table count (bank + test) ─────────────────────────────────────────

    public void testMultipleTables_DifferentTables() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_bank, repro_test | stats count() as c");
        verifySchema(actual, schema("c", "bigint"));
        verifyDataRows(actual, rows(9));
    }

    public void testMultipleTables_WithIndexPattern() throws IOException {
        Map<String, Object> actual = executePpl("source=repro_bank, repro_test* | stats count() as c");
        verifySchema(actual, schema("c", "bigint"));
        verifyDataRows(actual, rows(10));
    }

    public void testMultipleTablesAndFilters_WithIndexPattern() throws IOException {
        Map<String, Object> actual =
            executePpl("source=repro_bank, repro_test* gender = 'F' | stats count() as c");
        verifySchema(actual, schema("c", "bigint"));
        verifyDataRows(actual, rows(3));
    }
}
