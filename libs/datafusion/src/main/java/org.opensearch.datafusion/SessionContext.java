/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

/**
 * Session context for datafusion
 */
public class SessionContext implements AutoCloseable {

    static {
        System.loadLibrary("datafusion_jni");
    }

    // ptr to context in df
    private final long ptr;
    private final long runtime;

    static native void destroySessionContext(long pointer);

    static native long createSessionContext(int batchSize);

//    static native long createTable();

    static native long createRuntime();

    static native void destroyRuntime(long pointer);

    public long getRuntime() {
        return runtime;
    }

    public long getPointer() {
        return ptr;
    }

    public SessionContext(int batchSize) {
        this.ptr = createSessionContext(batchSize);
        this.runtime = createRuntime();
    }

    // init with datafusion default batch size.
    public SessionContext() {
        this(8192);
    }

    @Override
    public void close() throws Exception {
        destroySessionContext(this.ptr);
        destroyRuntime(this.runtime);
    }
}
