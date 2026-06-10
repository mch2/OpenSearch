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
 * Reproduction of the failing {@code testDoubleAppendPipeWithFilter} from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLAppendPipeCommandIT} on the analytics-engine
 * route. Uses the {@code account} dataset (1000 docs).
 */
public class CalcitePPLAppendPipeCommandReproIT extends CalciteReproTestCase {

    private static final Dataset ACCOUNT = new Dataset("account", "repro_account");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), ACCOUNT);
        provisioned = true;
    }

    public void testDoubleAppendPipeWithFilter() throws IOException {
        Map<String, Object> actual = executePpl("source=" + ACCOUNT.indexName
            + " | stats sum(age) as sum_age by gender"
            + " | appendpipe [ where gender = 'F' ]"
            + " | appendpipe [ where gender = 'M' ]");
        // 2 original + 1 (F filter from original) + 1 (M filter from cumulative 3 rows)
        verifyDataRows(actual,
            rows(14947, "F"), rows(15224, "M"), rows(14947, "F"), rows(15224, "M"));
    }
}
