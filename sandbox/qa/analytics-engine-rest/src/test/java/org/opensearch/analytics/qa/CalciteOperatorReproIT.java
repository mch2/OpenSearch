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
 * {@code org.opensearch.sql.calcite.remote.CalciteOperatorIT} (extends {@code OperatorIT}) on the
 * analytics-engine route. Uses the {@code bank} dataset. These all use explicit
 * {@code | fields age}, so bucket-A column ordering does not apply — divergences here are real
 * filter/operator semantics on the AE path.
 */
public class CalciteOperatorReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_op_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    private String src() { return "source=" + BANK.indexName; }

    public void testEqualOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age = 32 | fields age"), rows(32));
        verifyDataRows(executePpl(src() + " | where age = 32 | fields age"), rows(32));
        verifyDataRows(executePpl(src() + " | where 32 = age | fields age"), rows(32));
    }

    public void testNotEqualOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age != 32 | fields age"),
            rows(28), rows(33), rows(34), rows(36), rows(36), rows(39));
        verifyDataRows(executePpl(src() + " | where age != 32 | fields age"),
            rows(28), rows(33), rows(34), rows(36), rows(36), rows(39));
        verifyDataRows(executePpl(src() + " | where 32 != age | fields age"),
            rows(28), rows(33), rows(34), rows(36), rows(36), rows(39));
    }

    public void testLessOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age < 32 | fields age"), rows(28));
    }

    public void testLteOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age <= 32 | fields age"), rows(28), rows(32));
    }

    public void testGreaterOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age > 36 | fields age"), rows(39));
    }

    public void testGteOperator() throws IOException {
        verifyDataRows(executePpl(src() + " age >= 36 | fields age"), rows(36), rows(36), rows(39));
    }

    public void testNotOperator() throws IOException {
        verifyDataRows(executePpl(src() + " not age > 32 | fields age"), rows(28), rows(32));
    }
}
