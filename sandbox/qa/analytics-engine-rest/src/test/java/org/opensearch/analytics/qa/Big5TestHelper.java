/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper constants for the Big5 dataset. Schema and queries are derived from the
 * opensearch-benchmark big5 workload (HTTP-logs / ECS-flavored log records); the bundled
 * sample is synthesized so every query has hits without needing the full ~60GB corpus.
 *
 * <p>Provisioned via {@link DatasetProvisioner} using resources from {@code datasets/big5/}.
 */
public final class Big5TestHelper {

    /** Big5 dataset descriptor. */
    public static final Dataset DATASET = new Dataset("big5", "big5_logs");

    private Big5TestHelper() {
        // utility class
    }
}
