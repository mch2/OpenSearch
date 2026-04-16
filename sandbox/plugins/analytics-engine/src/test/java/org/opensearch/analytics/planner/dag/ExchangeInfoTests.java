/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;
import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link ExchangeInfo} constructor validation and field access.
 *
 * Validates: Requirements 1.1
 */
public class ExchangeInfoTests extends OpenSearchTestCase {

    /**
     * HASH_DISTRIBUTED with valid keyColumns and partitionCount succeeds.
     */
    public void testHashDistributedValidConstruction() {
        ExchangeInfo info = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0, 1), 4);
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, info.distributionType());
        assertEquals(List.of(0, 1), info.partitionKeyIndices());
        assertEquals(4, info.partitionCount());
        assertEquals(ShuffleImpl.FILE, info.shuffleImpl());
        assertTrue(info.isShuffle());
    }

    /**
     * HASH_DISTRIBUTED with empty keyColumns throws IllegalArgumentException.
     */
    public void testHashDistributedRequiresNonEmptyKeyColumns() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(), 4)
        );
        assertTrue(e.getMessage().contains("HASH_DISTRIBUTED requires non-empty partitionKeyIndices"));
    }

    /**
     * HASH_DISTRIBUTED with partitionCount less than 1 throws IllegalArgumentException.
     */
    public void testHashDistributedRequiresPartitionCountAtLeastOne() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0), 0)
        );
        assertTrue(e.getMessage().contains("partitionCount must be >= 1"));
    }

    /**
     * HASH_DISTRIBUTED with null keyColumns throws NullPointerException
     * (from List.copyOf).
     */
    public void testHashDistributedNullKeyColumnsThrows() {
        expectThrows(NullPointerException.class, () -> new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, null, 4));
    }

    /**
     * SINGLETON exchange with the 3-arg convenience constructor defaults
     * partitionCount to 1.
     */
    public void testSingletonConvenienceConstructorDefaultsPartitionCountToOne() {
        ExchangeInfo info = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        assertEquals(1, info.partitionCount());
        assertFalse(info.isShuffle());
    }

    /**
     * SINGLETON exchange with explicit partitionCount = 1 succeeds.
     */
    public void testSingletonExplicitPartitionCount() {
        ExchangeInfo info = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of(), 1);
        assertEquals(RelDistribution.Type.SINGLETON, info.distributionType());
        assertEquals(1, info.partitionCount());
    }

    /**
     * partitionKeyIndices are defensively copied.
     */
    public void testPartitionKeyIndicesAreDefensivelyCopied() {
        java.util.ArrayList<Integer> mutableKeys = new java.util.ArrayList<>(List.of(0, 1));
        ExchangeInfo info = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, mutableKeys, 4);
        mutableKeys.add(2);
        assertEquals(List.of(0, 1), info.partitionKeyIndices());
    }
}
