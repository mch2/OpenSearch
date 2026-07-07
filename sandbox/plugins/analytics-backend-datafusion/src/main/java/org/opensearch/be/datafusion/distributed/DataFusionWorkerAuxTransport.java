/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.transport.AuxTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * OpenSearch {@link AuxTransport} that owns the data-node {@code datafusion-distributed} Worker gRPC
 * server's lifecycle (Model B). Registered via {@code DataFusionPlugin.getAuxTransports} under the
 * {@code datafusion-worker} key, so it gets the standard auxiliary-transport bind-port-range +
 * managed lifecycle contract (mirrors the gRPC plugin's {@code Netty4GrpcServerTransport}).
 *
 * <p>The actual server is a Rust tonic server bound via FFM; this class delegates binding to
 * {@link DataFusionService#startWorker(int)} (which shares the node's native runtime) and shutdown
 * to {@link DataFusionService#stopWorker()}. The bound port is discovered by coordinators through the
 * {@code GetWorkerPort} transport action (NOT published as {@code streamAddress} — that field is
 * reserved for the single OpenSearch stream transport).
 *
 * <p>Enable by adding {@code datafusion-worker} to {@code aux.transport.types}; configure the port
 * via {@code aux.transport.datafusion-worker.port} (default range {@code 9400-9500}).
 */
public final class DataFusionWorkerAuxTransport extends AuxTransport {

    private static final Logger logger = LogManager.getLogger(DataFusionWorkerAuxTransport.class);

    public static final String DATAFUSION_WORKER_TRANSPORT_KEY = "datafusion-worker";

    private final DataFusionService dataFusionService;
    private final PortsRange portRange;
    private volatile BoundTransportAddress boundAddress;

    public DataFusionWorkerAuxTransport(Settings settings, DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
        this.portRange = AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(DATAFUSION_WORKER_TRANSPORT_KEY).get(settings);
    }

    @Override
    public String settingKey() {
        return DATAFUSION_WORKER_TRANSPORT_KEY;
    }

    @Override
    public BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    @Override
    protected void doStart() {
        // Bind the first available port in the configured range. The Rust server actually binds, so
        // we ask it to bind a chosen port and report what it got; on collision we advance the range.
        // (Most setups configure a single port or a small range.) startWorker(0) = ephemeral.
        int bound = -1;
        StringBuilder tried = new StringBuilder();
        // PortsRange.iterate returns true once the predicate succeeds for some port. A bind collision
        // (two nodes per host, leftover socket) surfaces as an exception from startWorker — catch it,
        // record the attempt, and return false so iterate advances to the next candidate port.
        boolean ok = portRange.iterate(candidate -> {
            try {
                return dataFusionService.startWorker(candidate) > 0;
            } catch (RuntimeException e) {
                if (tried.length() > 0) {
                    tried.append(", ");
                }
                tried.append(candidate).append(" (").append(e.getMessage()).append(')');
                return false;
            }
        });
        if (ok) {
            bound = dataFusionService.getWorkerPort();
        }
        if (bound <= 0) {
            // Distributed engine may be unavailable (older native lib) — that's not fatal; the node
            // simply can't serve distributed leaf/shuffle tasks. Leave boundAddress null.
            logger.warn(
                "DataFusion Worker aux transport did not bind a port (range={}, tried={}); distributed engine disabled on this node",
                portRange,
                tried
            );
            return;
        }
        try {
            InetAddress local = InetAddress.getLoopbackAddress();
            TransportAddress addr = new TransportAddress(new InetSocketAddress(local, bound));
            this.boundAddress = new BoundTransportAddress(new TransportAddress[] { addr }, addr);
            logger.info("DataFusion Worker aux transport bound on port {}", bound);
        } catch (Exception e) {
            throw new RuntimeException("failed to record DataFusion Worker bound address", e);
        }
    }

    @Override
    protected void doStop() {
        dataFusionService.stopWorker();
        this.boundAddress = null;
    }

    @Override
    protected void doClose() throws IOException {
        dataFusionService.stopWorker();
    }
}
