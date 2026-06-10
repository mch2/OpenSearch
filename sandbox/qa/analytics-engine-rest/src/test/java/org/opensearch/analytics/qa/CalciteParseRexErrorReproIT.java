/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;

/**
 * Reproduction of failing invalid-capture-group error tests from upstream
 * {@code CalciteParseCommandIT} and {@code CalciteRexCommandIT} on the analytics-engine route.
 *
 * <p>Each expects a 4xx with {@code Invalid capture group name '...'} +
 * {@code capture groups must be alphanumeric}. These are validation-path tests: a divergence
 * here means the AE route either accepts the bad group name or surfaces a different error.
 */
public class CalciteParseRexErrorReproIT extends CalciteReproTestCase {

    private static final String SUGGESTION = "capture groups must be alphanumeric";
    private static final Dataset BANK = new Dataset("bank", "repro_parserex_bank");
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

    private void assertInvalidGroup(String ppl, String badName) throws IOException {
        String err = executePplExpectingFailure(ppl);
        verifyErrorMessageContains(err, "Invalid capture group name '" + badName + "'");
        verifyErrorMessageContains(err, SUGGESTION);
    }

    // ── parse ─────────────────────────────────────────────────────────────────

    public void testParseErrorInvalidGroupNameHyphen() throws IOException {
        assertInvalidGroup(src() + " | parse email '.+@(?<host-name>.+)' | fields email", "host-name");
    }

    public void testParseErrorInvalidGroupNameSpecialCharacter() throws IOException {
        assertInvalidGroup(src() + " | parse email '.+@(?<host@name>.+)' | fields email", "host@name");
    }

    public void testParseErrorInvalidGroupNameStartingWithDigit() throws IOException {
        assertInvalidGroup(src() + " | parse email '.+@(?<1host>.+)' | fields email", "1host");
    }

    public void testParseErrorInvalidGroupNameUnderscore() throws IOException {
        assertInvalidGroup(src() + " | parse email '.+@(?<host_name>.+)' | fields email", "host_name");
    }

    // ── rex ─────────────────────────────────────────────────────────────────

    public void testRexErrorInvalidGroupNameHyphen() throws IOException {
        assertInvalidGroup(src() + " | rex field=email \"(?<user-name>[^@]+)@(?<domain>.+)\" | fields email", "user-name");
    }

    public void testRexErrorInvalidGroupNameSpecialCharacter() throws IOException {
        assertInvalidGroup(src() + " | rex field=email \"(?<user@name>[^@]+)@(?<domain>.+)\" | fields email", "user@name");
    }

    public void testRexErrorInvalidGroupNameStartingWithDigit() throws IOException {
        assertInvalidGroup(src() + " | rex field=email \"(?<1user>[^@]+)@(?<domain>.+)\" | fields email", "1user");
    }

    public void testRexErrorInvalidGroupNameUnderscore() throws IOException {
        assertInvalidGroup(src() + " | rex field=email \"(?<user_name>[^@]+)@(?<domain>.+)\" | fields email", "user_name");
    }
}
