/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation;

/**
 * Checked exception thrown when a delegation operation fails.
 * Carries the backend name and operation type for diagnostics.
 *
 * @opensearch.internal
 */
public class DelegationException extends Exception {

    private final String backendName;
    private final String operationType;

    /**
     * @param backendName   the backend that failed (e.g. "lucene")
     * @param operationType the operation that failed ("filter" or "scan")
     * @param message       detail message
     */
    public DelegationException(String backendName, String operationType, String message) {
        super(message);
        this.backendName = backendName;
        this.operationType = operationType;
    }

    /**
     * @param backendName   the backend that failed
     * @param operationType the operation that failed
     * @param message       detail message
     * @param cause         underlying cause
     */
    public DelegationException(String backendName, String operationType, String message, Throwable cause) {
        super(message, cause);
        this.backendName = backendName;
        this.operationType = operationType;
    }

    public String getBackendName() {
        return backendName;
    }

    public String getOperationType() {
        return operationType;
    }
}
