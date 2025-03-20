/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.OpenSearchException;

/**
 * Error that can return a seqNo to indicate partial failures from a sink.
 */
public class ReplicationSinkException extends OpenSearchException {

    public long getMaxReplicated() {
        return maxReplicated;
    }

    private final long maxReplicated;

    public ReplicationSinkException(String message) {
        super(message);
        this.maxReplicated = 0L;
    }

    public ReplicationSinkException(String message, long maxReplicated) {
        super(message);
        this.maxReplicated = maxReplicated;
    }

    public ReplicationSinkException(String message, Throwable cause, long maxReplicated) {
        super(message, cause);
        this.maxReplicated = maxReplicated;
    }
}
