/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for cryptographic hash functions routed through PPL →
 * Calcite → Substrait → DataFusion. Bank fixture row 1: firstname='Amber'.
 */
public class ScalarCryptoFunctionIT extends BaseScalarFunctionIT {

    public void testMd5() {
        // md5('Amber') = 'a26f64aff7d75d8e0e9deb14b1cd6e3a' (verified independently)
        assertScalarString("md5(firstname)", "a26f64aff7d75d8e0e9deb14b1cd6e3a");
    }

    public void testSha1() {
        // sha1('Amber')
        assertScalarString("sha1(firstname)", "1cb1cb14e1d9a17d9c25aaedd5d2cb9e89dba2bb");
    }

    public void testSha2_256() {
        // sha2('Amber', 256)
        assertScalarString("sha2(firstname, 256)", "f4f3a6dabbf48a4ef3a4dadcc8b4a8d3e0fe527acca6c8da95e3c7dab5d65a02");
    }
}
