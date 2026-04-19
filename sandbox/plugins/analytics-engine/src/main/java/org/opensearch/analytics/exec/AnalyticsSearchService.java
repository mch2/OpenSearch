/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.arrow.flight.transport.ArrowFlightChannel;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.transport.TransportChannel;

import java.util.Map;

/**
 * Data-node service that executes plan fragments against local shards.
 * Acquires a reader from the shard's composite engine, builds an
 * {@link ExecutionContext}, and invokes the backend's {@link SearchExecEngine}
 * to produce result batches, streaming each back to the coordinator via
 * {@link TransportChannel#sendResponseBatch}.
 *
 * <p>Does NOT hold {@code IndicesService} — receives an already-resolved
 * {@link IndexShard} from the transport action.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService {

    private final Map<String, AnalyticsSearchBackendPlugin> backends;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends) {
        this.backends = backends;
    }

    /**
     * Executes a plan fragment against the given shard and streams each
     * engine result batch back to the coordinator as a
     * {@link FragmentExecutionResponse} via {@code channel.sendResponseBatch}.
     * After the stream is drained, {@code channel.completeStream()} is called.
     *
     * <p>Polls the shard task for cancellation at each batch boundary.
     *
     * @param request the fragment execution request
     * @param shard   the already-resolved index shard
     * @param task    the shard task to poll for cancellation (nullable)
     * @param channel the transport channel to stream responses over
     */
    public void executeFragmentStreaming(
        FragmentExecutionRequest request,
        IndexShard shard,
        AnalyticsShardTask task,
        TransportChannel channel
    ) {
        DataFormatAwareEngine compositeEngine = shard.getCompositeEngine();
        if (compositeEngine == null) {
            throw new IllegalStateException("No CompositeEngine on " + shard.shardId());
        }

        // Select the first available plan alternative whose backend is registered on this node.
        FragmentExecutionRequest.PlanAlternative selectedPlan = null;
        for (FragmentExecutionRequest.PlanAlternative alt : request.getPlanAlternatives()) {
            if (backends.containsKey(alt.getBackendId())) {
                selectedPlan = alt;
                break;
            }
        }
        if (selectedPlan == null) {
            throw new IllegalArgumentException(
                "No plan alternative matches available backends. Alternatives: "
                    + request.getPlanAlternatives().stream().map(FragmentExecutionRequest.PlanAlternative::getBackendId).toList()
                    + ". Available: "
                    + backends.keySet()
            );
        }

        try (GatedCloseable<Reader> gatedReader = compositeEngine.acquireReader()) {
            SearchShardTask searchShardTask = null; // TODO: real task for cancellation
            ExecutionContext ctx = new ExecutionContext(request.getShardId().getIndexName(), searchShardTask, gatedReader.get());
            ctx.setFragmentBytes(selectedPlan.getFragmentBytes());
            // Share Flight's channel allocator tree with the backend so result-stream
            // buffers end up in the same tree as Flight's wire-side shared root. This
            // lets transferTo() do true zero-copy moves and keeps per-allocator
            // accounting consistent when the engine stream's allocator closes.
            ArrowFlightChannel flightChannel = ArrowFlightChannel.from(channel);
            ctx.setAllocator(flightChannel.getAllocator());

            AnalyticsSearchBackendPlugin backend = backends.get(selectedPlan.getBackendId());

            SearchExecEngine<ExecutionContext, EngineResultStream> engine = backend.createSearchExecEngine(ctx);
            EngineResultStream stream = engine.execute(ctx);
            try {
                for (EngineResultBatch batch : stream) {
                    // TODO: cancellation — the shard task is not a CancellableTask
                    // (see AnalyticsShardTask javadoc), so per-batch cancellation
                    // polling is moved to the query-level AnalyticsQueryTask path.
                    channel.sendResponseBatch(new FragmentExecutionResponse(batch.getArrowRoot()));
                }
                // Defer stream and engine close until after Flight's single-threaded
                // executor has drained every queued sendResponseBatch + completeStream
                // task. Flight guarantees the onComplete callback runs after the last
                // transfer and after the shared root is closed, so producer resources
                // (engine stream's allocator) are released exactly once, with no race
                // against pending async transfers.
                channel.completeStream(() -> {
                    stream.close();
                    engine.close();
                });
            } catch (Exception inner) {
                stream.close();
                engine.close();
                throw inner;
            }
        } catch (TaskCancelledException e) {
            throw e; // do NOT wrap — preserve type
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
                throw new RuntimeException("Failed to execute fragment on " + shard.shardId() + " and failed to send error", e);
            }
        }
    }
}
