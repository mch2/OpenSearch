/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.transport.TransportRequestOptions;

public interface ReplicationSettings {

    RateLimiter rateLimiter();

    TimeValue internalActionTimeout();

    TransportRequestOptions.Type getTransportRequestType();

    int getMaxConcurrentFileChunks();

    int getChunkSize();

}
