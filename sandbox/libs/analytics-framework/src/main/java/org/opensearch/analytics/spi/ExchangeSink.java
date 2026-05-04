/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Write-only interface for feeding Arrow batches into a stage exchange.
 * Producers (shard scan stages, local compute stages) call {@link #feed}
 * to push data; they never read from the sink.
 *
 * <p>Implementations are backend-specific and created via {@link ExchangeSinkProvider}.
 * A coordinator-side sink runs the root stage computation (final aggregate, sort, etc.)
 * over the batches it receives.
 *
 * <p>Implementations must be thread-safe — multiple shard response handlers
 * may call {@link #feed} concurrently.
 *
 * <p>Multi-input sinks (coord-side joins) override {@link #feed(int, VectorSchemaRoot)}
 * to dispatch by input index. Single-input sinks implement only the legacy
 * {@link #feed(VectorSchemaRoot)}; the default {@link #feed(int, VectorSchemaRoot)}
 * delegates to it for {@code inputIndex == 0} and throws otherwise — preventing
 * silent fan-in collapse if a multi-input stage is mistakenly wired to a
 * single-input sink.
 *
 * @opensearch.internal
 */
public interface ExchangeSink {

    /**
     * Ingest an Arrow batch into this sink. The sink takes ownership of the
     * batch and is responsible for releasing it when no longer needed.
     */
    void feed(VectorSchemaRoot batch);

    /**
     * Multi-input feed. Default routes to the legacy single-input
     * {@link #feed(VectorSchemaRoot)} when {@code inputIndex == 0}; throws
     * {@link UnsupportedOperationException} otherwise. Multi-input sinks
     * override this directly and typically make {@link #feed(VectorSchemaRoot)}
     * throw.
     */
    default void feed(int inputIndex, VectorSchemaRoot batch) {
        if (inputIndex != 0) {
            throw new UnsupportedOperationException(
                getClass().getSimpleName() + " is single-input but received inputIndex=" + inputIndex
            );
        }
        feed(batch);
    }

    /**
     * Signal that no more batches will be fed. Releases resources.
     */
    void close();
}
