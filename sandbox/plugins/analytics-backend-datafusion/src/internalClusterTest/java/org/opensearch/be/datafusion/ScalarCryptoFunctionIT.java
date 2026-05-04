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
        // md5('Amber') over UTF-8 — verified with `echo -n Amber | md5`.
        assertScalarString("md5(firstname)", "88068e33c78eb72f1b371c7110846085");
    }

    public void testSha1() {
        // sha1('Amber') over UTF-8 — verified with `echo -n Amber | shasum`.
        assertScalarString("sha1(firstname)", "27a01d4772038a3f83552908e0470604e773f8af");
    }

    public void testSha2_256() {
        // sha2('Amber', 256) over UTF-8 — verified with `echo -n Amber | shasum -a 256`.
        assertScalarString("sha2(firstname, 256)", "a5cccbcead7dcd65375ec6ea6ec28e3cd59af94417bbbee3276764c5d60ae5e9");
    }

    /**
     * crc32('Amber') = 2268019078 over UTF-8 bytes — verified via Python zlib.crc32.
     * Implemented as a Rust UDF in rust/src/udf/mod.rs and declared in
     * opensearch_scalar.yaml as i64.
     */
    public void testCrc32() {
        assertScalarLong("crc32(firstname)", 2268019078L);
    }
}
