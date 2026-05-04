/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Context passed to {@link ExchangeSinkProvider#createSink} when a
 * coordinator-reduce stage is being set up. Carries everything the backend
 * needs to build an {@link ExchangeSink}: serialized plan, buffer allocator,
 * one or more input descriptors (one per child stage), and the downstream
 * sink the backend writes results to.
 *
 * <p>Multi-input shapes (e.g. coord-side joins) carry one {@link InputDescriptor}
 * per child stage; the {@code inputId} of each descriptor is the substrait
 * {@code NamedScan} reference the backend uses to register the corresponding
 * input stream. The convention is {@code "input-" + i} where {@code i} is the
 * child's index in the parent stage's {@code getChildStages()} list — same
 * convention used by the substrait fragment's {@code NamedScan} table names,
 * so registration and lookup line up by string equality.
 *
 * <p>Single-input shapes (every reduce shape today outside joins) construct
 * via {@link #singleInput} and carry one descriptor named {@code "input-0"}.
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(
    String queryId,
    int stageId,
    byte[] fragmentBytes,
    BufferAllocator allocator,
    List<InputDescriptor> inputs,
    ExchangeSink downstream
) {

    /**
     * Describes one input stream into the sink. The {@code childStageId} identifies
     * the producing stage; the {@code inputId} is the substrait {@code NamedScan}
     * reference the backend resolves against its session catalog.
     *
     * @param childStageId stage id of the child producing this input
     * @param inputId      substrait NamedScan reference; conventionally {@code "input-" + i}
     * @param schema       Arrow schema of batches arriving on this input
     */
    public record InputDescriptor(int childStageId, String inputId, Schema schema) {}

    /**
     * Convenience for callers with a single input — wraps the schema as a one-element
     * list with {@code inputId = "input-0"}. Equivalent to today's single-input behavior.
     */
    public static ExchangeSinkContext singleInput(
        String queryId,
        int stageId,
        byte[] fragmentBytes,
        BufferAllocator allocator,
        Schema inputSchema,
        int childStageId,
        ExchangeSink downstream
    ) {
        return new ExchangeSinkContext(
            queryId,
            stageId,
            fragmentBytes,
            allocator,
            List.of(new InputDescriptor(childStageId, "input-0", inputSchema)),
            downstream
        );
    }
}
