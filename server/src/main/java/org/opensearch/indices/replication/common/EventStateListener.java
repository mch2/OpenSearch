/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.OpenSearchException;

public interface EventStateListener<T> {

    void onDone(T state);

    void onFailure(T state, OpenSearchException exception, boolean sendShardFailure);

}
