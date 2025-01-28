/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import java.util.Set;

/**
 *
 */
public interface PartitionedStreamProducer extends StreamProducer {
   Set<StreamTicket> partitions();
}
