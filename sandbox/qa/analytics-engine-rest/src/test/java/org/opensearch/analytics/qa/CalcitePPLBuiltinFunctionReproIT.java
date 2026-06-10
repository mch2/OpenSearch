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
 * Reproduction of failing arithmetic methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLBuiltinFunctionIT} on the analytics-engine
 * route — division and modulo type-widening / precision. Uses {@code datatypes_numeric}.
 */
public class CalcitePPLBuiltinFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset NUM = new Dataset("datatypes_numeric", "repro_bf_numeric");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), NUM);
        provisioned = true;
    }

    private String src() { return "source=" + NUM.indexName; }

    public void testDivide() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval r1 = 22 / 7, r2 = integer_number / 1, r3 = 21 / 7, r4 = byte_number / short_number,"
            + " r5 = half_float_number / float_number, r6 = float_number / short_number, r7 = 22 / 7.0,"
            + " r8 = 22.0 / 7, r9 = 21.0 / 7.0, r10 = half_float_number / short_number,"
            + " r11 = double_number / float_number"
            + " | fields r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11");
        verifySchema(actual,
            schema("r1", "int"), schema("r2", "int"), schema("r3", "int"), schema("r4", "smallint"),
            schema("r5", "float"), schema("r6", "float"), schema("r7", "double"), schema("r8", "double"),
            schema("r9", "double"), schema("r10", "float"), schema("r11", "double"));
        verifyDataRows(actual,
            rows(3, 2, 3, 1, 1.1774194, 2.0666666, 3.142857142857143, 3.142857142857143, 3.0,
                2.4333334, 0.8225806704669051));
    }

    public void testModFloatAndNegative() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval f = mod(float_number, 2), n = -1 * short_number % 2, nd = -1 * double_number % 2"
            + " | fields f, n, nd");
        verifySchema(actual, schema("f", "float"), schema("n", "int"), schema("nd", "double"));
        verifyDataRows(actual, rows(0.2, -1, -1.1));
    }

    public void testModShouldReturnWiderTypes() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval b = byte_number % 2, i = mod(integer_number, 3), l = mod(long_number, 2),"
            + " f = float_number % 2, d = mod(double_number, 2), s = short_number % byte_number"
            + " | fields b, i, l, f, d, s");
        verifySchema(actual,
            schema("b", "int"), schema("i", "int"), schema("l", "bigint"),
            schema("f", "float"), schema("d", "double"), schema("s", "smallint"));
        verifyDataRows(actual, rows(0, 2, 1, 0.2, 1.1, 3));
    }
}
