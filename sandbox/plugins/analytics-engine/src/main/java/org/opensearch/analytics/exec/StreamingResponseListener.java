/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;

/**
 * Listener for streaming responses from a single shard request.
 * Follows {@code StreamSearchActionListener.onStreamResponse(result, isLast)} pattern.
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #onStreamResponse} called 1+ times; exactly one call has {@code isLast=true} (terminal success)</li>
 *   <li>{@link #onFailure} called at most once (terminal failure)</li>
 *   <li>Exactly one terminal event per request</li>
 * </ul>
 */
public interface StreamingResponseListener {

    /**
     * Called for each batch received from the data node.
     *
     * @param response the fragment execution response batch
     * @param isLast   {@code true} if this is the final batch (terminal success event)
     */
    void onStreamResponse(FragmentExecutionResponse response, boolean isLast);

    /**
     * Called when the request fails. Terminal failure event.
     *
     * @param e the exception that caused the failure
     */
    void onFailure(Exception e);
}
