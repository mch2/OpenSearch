/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.AnalyticsQueryRequest;
import org.opensearch.analytics.exec.action.AnalyticsQueryResponse;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.analytics.settings.PlannerSettings;
import org.opensearch.analytics.stats.AnalyticsStatsCollector;
import org.opensearch.arrow.allocator.AllocationRejection;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING;

/**
 * Coordinator-level plan executor. Registered as a {@link HandledTransportAction}
 * so that Guice injects all dependencies ({@link TransportService},
 * {@link ClusterService}, {@link ThreadPool}, etc.) automatically.
 *
 * <p>Front-end plugins resolve this class from the Node's Guice injector and invoke
 * {@link #execute(RelNode, QueryRequestContext, ActionListener)} directly. Execution is asynchronous —
 * the listener is fired by the scheduler once the query completes (or fails). The transport
 * path ({@code doExecute}) is reserved for future remote query invocation.
 *
 * @opensearch.internal
 */
public class DefaultPlanExecutor extends HandledTransportAction<AnalyticsQueryRequest, AnalyticsQueryResponse>
    implements
        QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;
    private final Executor searchExecutor;
    private final ThreadPool threadPool;
    private final NodeClient client;
    private final EngineContextProvider contextProvider;
    private final AnalyticsStatsCollector statsCollector;
    // Owned and closed by AnalyticsPlugin via the injected CoordinatorAllocatorHandle so that
    // shutdown closes this child of POOL_QUERY before arrow-base closes the root allocator.
    private final BufferAllocator coordinatorAllocator;
    private volatile long perQueryBufferLimit;
    private volatile int maxShardsPerQuery;
    private volatile int maxConcurrentShardRequestsPerNode;
    private volatile boolean preferMetadataDriver;
    private final PlannerSettings plannerSettings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AnalyticsSearchSlowLog analyticsSearchSlowLog;
    // Node-level circuit-breaker service (Guice-bound singleton). The result-heap guard charges the
    // shared REQUEST breaker (indices.breaker.request.limit) — the same budget used by aggregations
    // and QueryPhaseResultConsumer — resolved per request via getBreaker(CircuitBreaker.REQUEST).
    private final CircuitBreakerService circuitBreakerService;
    private volatile double resultHeapExpansionFactor;
    private volatile long varWidthAllowanceBytes;
    // Row cap enforced by the result sink (RowProducingSink); the upfront admission estimate reads
    // the SAME constant so enforcement and estimate can never drift.
    private final int maxResultRows = RowProducingSink.DEFAULT_MAX_ROWS_INT;

    @Inject
    public DefaultPlanExecutor(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        CapabilityRegistry capabilityRegistry,
        EngineContextProvider contextProvider,
        NodeClient client,
        Scheduler scheduler,
        CoordinatorAllocatorHandle coordinatorAllocatorHandle,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AnalyticsSearchSlowLog analyticsSearchSlowLog,
        AnalyticsStatsCollector statsCollector,
        CircuitBreakerService circuitBreakerService
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, AnalyticsQueryRequest::new);
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.threadPool = threadPool;
        this.client = client;
        this.scheduler = scheduler;
        this.contextProvider = contextProvider;
        this.statsCollector = statsCollector;
        // Use the plugin-owned coordinator allocator (a child of POOL_QUERY). The plugin
        // closes the underlying allocator on Plugin.close() via the handle, so coordinator-
        // side allocations are released deterministically before arrow-base tears down the
        // root allocator. Long.MAX_VALUE on the child means dynamic resizes of
        // parquet.native.pool.query.max take effect immediately via Arrow's parent-cap check
        // at allocateBytes — no listener needed.
        this.coordinatorAllocator = coordinatorAllocatorHandle.getAllocator();
        this.perQueryBufferLimit = AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsPlugin.COORDINATOR_BUFFER_LIMIT, v -> perQueryBufferLimit = v);

        // TODO: These should be honored as query params, but requires front-end changes to pass request options.
        this.maxShardsPerQuery = AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY, v -> maxShardsPerQuery = v);
        this.maxConcurrentShardRequestsPerNode = AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.get(
            clusterService.getSettings()
        );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE,
                v -> maxConcurrentShardRequestsPerNode = v
            );
        this.preferMetadataDriver = AnalyticsPlugin.PREFER_METADATA_DRIVER.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsPlugin.PREFER_METADATA_DRIVER, v -> preferMetadataDriver = v);
        this.circuitBreakerService = circuitBreakerService;
        this.resultHeapExpansionFactor = AnalyticsQuerySettings.RESULT_HEAP_EXPANSION_FACTOR.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsQuerySettings.RESULT_HEAP_EXPANSION_FACTOR, v -> resultHeapExpansionFactor = v);
        this.varWidthAllowanceBytes = AnalyticsQuerySettings.RESULT_VARWIDTH_ALLOWANCE.get(clusterService.getSettings()).getBytes();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyticsQuerySettings.RESULT_VARWIDTH_ALLOWANCE, v -> varWidthAllowanceBytes = v.getBytes());
        // Planner settings (oversampling factor + delegation block-list); self-registers update
        // consumers for live changes.
        this.plannerSettings = PlannerSettings.create(
            clusterService.getClusterSettings(),
            clusterService.getSettings(),
            capabilityRegistry
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.analyticsSearchSlowLog = analyticsSearchSlowLog;
    }

    /** Visible for testing: the live per-node concurrent-shard-request limit (reflects dynamic updates). */
    public int maxConcurrentShardRequestsPerNode() {
        return maxConcurrentShardRequestsPerNode;
    }

    /** Visible for testing: the live max-shards-per-query limit (reflects dynamic updates). */
    public int maxShardsPerQuery() {
        return maxShardsPerQuery;
    }

    @Override
    public void execute(RelNode logicalFragment, QueryRequestContext queryCtx, ActionListener<Iterable<Object[]>> listener) {
        // Dispatch through ActionModule so the SecurityFilter evaluates index-level
        // permissions before any planning work begins. The AnalyticsQueryRequest
        // implements IndicesRequest.Replaceable, exposing target indices extracted
        // from the RelNode's TableScan nodes.
        String[] indices = RelNodeUtils.extractIndices(logicalFragment);
        AnalyticsQueryRequest request = new AnalyticsQueryRequest(logicalFragment, queryCtx, indices);
        applyParentTask(request, queryCtx);
        client.execute(
            AnalyticsQueryAction.INSTANCE,
            request,
            ActionListener.wrap(resp -> listener.onResponse(resp.getRows()), listener::onFailure)
        );
    }

    @Override
    public void executeWithProfile(RelNode logicalFragment, QueryRequestContext queryCtx, ActionListener<ProfiledResult> listener) {
        // Route through the framework action (profile=true) just like execute(), so a profiling
        // query runs under the framework-provided, cancellable task rather than detached on
        // searchExecutor. The SecurityFilter also evaluates index permissions on this path.
        String[] indices = RelNodeUtils.extractIndices(logicalFragment);
        AnalyticsQueryRequest request = new AnalyticsQueryRequest(logicalFragment, queryCtx, indices, true);
        applyParentTask(request, queryCtx);
        client.execute(
            AnalyticsQueryAction.INSTANCE,
            request,
            ActionListener.wrap(resp -> listener.onResponse(resp.getProfiledResult()), listener::onFailure)
        );
    }

    /**
     * Registers the analytics query task as a child of the front-end task so a front-end cancel
     * (disconnect / timeout / {@code _tasks/_cancel}) cascades into the query and its fragments.
     * Set here, not in {@code createTask}, because the parent {@link TaskId} needs the local node id.
     */
    private void applyParentTask(AnalyticsQueryRequest request, QueryRequestContext queryCtx) {
        if (queryCtx == null || queryCtx.parentTask() == null) {
            return;
        }
        request.setParentTask(new TaskId(clusterService.localNode().getId(), queryCtx.parentTask().getId()));
    }

    /**
     * Unified planning + execution path. When {@code profile} is true, captures the CBO
     * plan text and snapshots per-stage timing into the {@link ProfiledResult}; when false,
     * wraps rows into a ProfiledResult with null profile for uniform listener handling.
     *
     * <p>If {@code queryCtx} is non-null, its {@link QueryRequestContext#clusterState()} is
     * used for Calcite planning so the planner sees the same snapshot the front-end built
     * the schema from. Otherwise a fresh {@code clusterService.state()} is read.
     */
    private void executeInternal(
        AnalyticsQueryTask queryTask,
        RelNode logicalFragment,
        QueryRequestContext queryCtx,
        boolean profile,
        ActionListener<ProfiledResult> listener
    ) {
        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(logicalFragment.getCluster().getMetadataProvider()));
        logicalFragment.getCluster().invalidateMetadataQuery();

        // Create the slow log wrapper at the start so it observes the full query lifecycle.
        final String querySource = queryCtx != null ? queryCtx.querySource() : null;
        final AnalyticsSearchSlowLog.QuerySlowLogListener queryListener = analyticsSearchSlowLog.createQueryListener(querySource);
        final long queryStartNanos = System.nanoTime();

        // Always time planning and capture the full plan: every query produces a QueryProfile
        // that's fed into AnalyticsStatsCollector for the _plugins/_analytics/stats endpoint.
        // The non-explain path drops the profile from the response after recording it; the
        // explain path returns it inline.
        final long planStartNanos = System.nanoTime();
        // Reuse the snapshot captured at REST entry when present; this is the same ClusterState
        // OpenSearchSchemaBuilder used to build the SchemaPlus, so planner and schema agree.
        // TODO: remove the null fallback once every front-end (test-ppl-frontend,
        // dsl-query-executor) threads an EngineContextProvider.getContext() snapshot through.
        ClusterState planningState = queryCtx != null ? queryCtx.clusterState() : clusterService.state();
        PlannerContext plannerContext = new PlannerContext(
            capabilityRegistry,
            planningState,
            indexNameExpressionResolver,
            false,
            preferMetadataDriver
        );
        plannerContext.setPlannerSettings(plannerSettings);
        RelNode plan = PlannerImpl.createPlan(logicalFragment, plannerContext);
        final String fullPlan = profile ? RelOptUtil.toString(plan) : null;
        QueryDAG dag = DAGBuilder.build(plan, capabilityRegistry, clusterService, indexNameExpressionResolver);
        PlanForker.forkAll(dag, capabilityRegistry);
        BackendPlanAdapter.adaptAll(dag, capabilityRegistry);
        // Collapse multi-backend stages to a single chosen alternative before conversion
        // so the convertor runs once per stage and the wire request carries one PlanAlternative.
        PlanAlternativeSelector.selectAll(dag, capabilityRegistry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(dag, capabilityRegistry);
        final long planningTimeNanos = System.nanoTime() - planStartNanos;
        final long planningTimeMs = TimeUnit.NANOSECONDS.toMillis(planningTimeNanos);
        logger.debug("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        queryListener.onPlanningComplete(dag.queryId(), planningTimeNanos);

        // The task is the framework-provided task from doExecute (registered by
        // HandledTransportAction before doExecute, unregistered when the listener completes).
        // Using it — rather than self-registering a detached task — is what lets a client
        // disconnect / explicit task cancel propagate into the running query.
        queryListener.setHeaders(queryTask.getHeader(Task.X_OPAQUE_ID), queryTask.getHeader(Task.X_REQUEST_ID));
        final BufferAllocator queryAllocator;
        final boolean ownsAllocator;
        if (perQueryBufferLimit <= 0) {
            queryAllocator = coordinatorAllocator;
            ownsAllocator = false;
        } else {
            // Per-request allocation: a failure here means the QUERY pool is exhausted, not
            // a framework misconfiguration. Translate Arrow's OutOfMemoryException into
            // OpenSearchRejectedExecutionException so the REST layer maps it to HTTP 429
            // and the client sees a proper backpressure signal rather than a generic 500.
            queryAllocator = AllocationRejection.wrap(
                "query-" + dag.queryId(),
                () -> coordinatorAllocator.newChildAllocator("query-" + dag.queryId(), 0, perQueryBufferLimit)
            );
            ownsAllocator = true;
        }
        logger.debug("[query-{}] Arrow allocator created, limit={}B", dag.queryId(), perQueryBufferLimit);

        // ─── Build query context ──────────────────────────────────────────
        final QueryContext context;
        try {
            context = new QueryContext(
                dag,
                threadPool,
                queryTask,
                maxConcurrentShardRequestsPerNode,
                maxShardsPerQuery,
                List.of(queryListener),
                queryAllocator,
                ownsAllocator,
                profile
            );
        } catch (Exception e) {
            if (ownsAllocator) queryAllocator.close();
            throw e;
        }

        // ─── Execution + materialization ──────────────────────────────────
        /*
        Profile and explain are captured within the QueryExecution, however QueryExecution requires the complete
        batchesListener to construct the ExecutionGraph. To get around this circular dependency we build a profiling
        listener with an empty QueryExecution reference, and then populate it once constructed.
         */
        final AtomicReference<QueryExecution> execRef = new AtomicReference<>();

        // ─── Result-heap admission guard ──────────────────────────────────
        // Charge the shared REQUEST breaker (indices.breaker.request.limit) — the same coordinator-heap
        // budget used by aggregations and QueryPhaseResultConsumer, and subordinate to the parent
        // real-memory breaker (indices.breaker.total.limit). A CircuitBreakingException maps to HTTP
        // 429 via the standard exception→status path. The charge label <analytics_result[queryId]> is
        // the attribution mechanism in trip messages, since the budget is shared with other consumers.
        //
        // Admission is UPFRONT and pessimistic: before dispatching any execution, reserve the
        // worst-case result footprint (row cap × per-column widths, expansion-adjusted). This rejects
        // an over-budget query before the full distributed execution runs, rather than mid-drain after
        // the work is wasted. batchesToRows later shrinks the reservation to the actual native size.
        // The reservation is held until the whole action settles (materialized rows stay live through
        // response construction) and released exactly once on every terminal via runAfter below.
        //
        // If analytics/search isolation is ever required, the graduation path is a dedicated breaker
        // via CircuitBreakerPlugin — only the getBreaker(...) lookup here would change.
        final CircuitBreaker requestBreaker = circuitBreakerService == null
            ? null
            : circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        final ResultHeapCharge heapCharge = new ResultHeapCharge(requestBreaker, dag.queryId(), resultHeapExpansionFactor);
        // Release exactly once across ALL terminals. Two paths register the SAME releaseOnce so the
        // charge is freed no matter which fires first (the other becomes a no-op):
        // 1. runAfter(listener) — the fast path on a delivered ActionListener terminal (success,
        // failure, or the upfront-charge rejection below, which has no execution/context yet).
        // 2. context.onClose — the safety net. QueryExecution.close() fires context close from
        // EVERY terminal transition (incl. CANCELLED and the cancel races where the scheduler
        // never delivers a listener terminal), which the runAfter path alone leaks.
        final Releasable heapChargeRelease = Releasables.releaseOnce(heapCharge);
        final ActionListener<ProfiledResult> releasingListener = ActionListener.runAfter(listener, heapChargeRelease::close);
        context.onClose(heapChargeRelease);

        // Upfront worst-case admission — may throw CircuitBreakingException (→ 429) with ZERO
        // execution dispatched and ZERO batches created. No QueryExecution is created on this path, so
        // close the context here to release BOTH the heap charge (via context.onClose above) and, when
        // analytics.coordinator.buffer_limit > 0, the per-query child allocator the context owns —
        // neither of which any later terminal would otherwise free. Routed through releasingListener so
        // the user terminal still fires; the charge is released exactly once (context.close → onClose).
        try {
            heapCharge.charge(estimateWorstCaseResultBytes(logicalFragment.getRowType(), maxResultRows, varWidthAllowanceBytes));
        } catch (RuntimeException admissionRejected) {
            try {
                context.close();
            } catch (RuntimeException closeError) {
                admissionRejected.addSuppressed(closeError);
            }
            releasingListener.onFailure(admissionRejected);
            return;
        }

        // Build the profile on every terminal — both for the _explain payload (when profile=true)
        // and for the stats collector (always). When profile=false the resulting profile is
        // recorded into the collector and dropped from the response.
        ActionListener<Iterable<Object[]>> rowsListener = buildProfilingRowsListener(
            execRef,
            context,
            fullPlan,
            planningTimeMs,
            statsCollector,
            profile,
            releasingListener
        );

        final List<String> outputColumnOrder = logicalFragment.getRowType().getFieldNames();
        // No taskManager.unregister here: the framework (HandledTransportAction) unregisters the
        // task it created for doExecute once this listener settles. Unregistering it ourselves
        // would double-free a task we no longer own.
        ActionListener<Iterable<VectorSchemaRoot>> batchesListener = ActionListener.wrap(batches -> {
            Iterable<Object[]> rows = batchesToRows(batches, outputColumnOrder, heapCharge);
            long totalRows = rows instanceof List ? ((List<?>) rows).size() : 0;
            queryListener.onQueryComplete(dag.queryId(), System.nanoTime() - queryStartNanos, totalRows);
            rowsListener.onResponse(rows);
        }, rowsListener::onFailure);

        TimeValue taskTimeout = queryTask.getCancelAfterTimeInterval();
        TimeValue clusterTimeout = clusterService.getClusterSettings().get(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING);
        if (taskTimeout != null || SearchService.NO_TIMEOUT.equals(clusterTimeout) == false) {
            batchesListener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                queryTask,
                clusterTimeout,
                batchesListener,
                e -> {}
            );
        }

        // A synchronous throw from scheduler.execute (ExecutionGraph.build / execution.start() failing
        // under native pressure during the storm) would otherwise propagate to doExecute's outer
        // try/catch — which completes the TRANSPORT listener, not this releasingListener, and never
        // reaches a QueryExecution terminal to fire context.close(). Both the runAfter release AND the
        // context.onClose safety-net would be orphaned, leaking the upfront charge. Catch it here and
        // route through releasingListener so the charge is released exactly once; also close the
        // context (its own cleanup + our registered release) since no QueryExecution owns it yet.
        try {
            execRef.set(scheduler.execute(context, batchesListener)); // execRef read by profile listener after execution completes
        } catch (RuntimeException dispatchFailed) {
            try {
                context.close();
            } catch (RuntimeException closeError) {
                dispatchFailed.addSuppressed(closeError);
            }
            releasingListener.onFailure(dispatchFailed);
        }
    }

    /**
     * Builds a rows listener that, on every terminal, feeds the {@link ExecutionGraph} into the
     * stats collector via {@link AnalyticsStatsCollector#recordExecution} — no allocation, no
     * plan stringification. The full {@link QueryProfile} is only built when
     * {@code includeProfileInResponse} is true (i.e. for the {@code _explain} response).
     */
    private static ActionListener<Iterable<Object[]>> buildProfilingRowsListener(
        AtomicReference<QueryExecution> execRef,
        QueryContext context,
        String fullPlan,
        long planningTimeMs,
        AnalyticsStatsCollector statsCollector,
        boolean includeProfileInResponse,
        ActionListener<ProfiledResult> listener
    ) {
        return ActionListener.wrap(rows -> {
            // A fast query can fire this terminal inline (on the calling thread, inside
            // scheduler.execute) BEFORE execRef.set runs — so execRef may still be null. Guard it
            // like the failure path below: skip graph-based profiling rather than NPE on getGraph().
            QueryExecution exec = execRef.get();
            ExecutionGraph graph = exec != null ? exec.getGraph() : null;
            statsCollector.recordExecution(graph, context.dag(), planningTimeMs);
            QueryProfile qp = includeProfileInResponse && graph != null
                ? QueryProfileBuilder.snapshot(graph, context, fullPlan, planningTimeMs)
                : null;
            listener.onResponse(new ProfiledResult(rows, null, qp));
        }, e -> {
            QueryExecution exec = execRef.get();
            ExecutionGraph graph = exec != null ? exec.getGraph() : null;
            statsCollector.recordExecution(graph, context.dag(), planningTimeMs);
            QueryProfile qp = includeProfileInResponse
                ? (graph != null
                    ? QueryProfileBuilder.snapshot(graph, context, fullPlan, planningTimeMs)
                    : new QueryProfile(context.queryId(), java.util.List.of(), planningTimeMs, 0L, java.util.List.of()))
                : null;
            listener.onResponse(new ProfiledResult(null, e, qp));
        });
    }

    @Override
    protected void doExecute(Task task, AnalyticsQueryRequest request, ActionListener<AnalyticsQueryResponse> listener) {
        // Runs after SecurityFilter has authorized the request.
        // Fork the entire query lifecycle (planning, scheduling, cleanup) onto the SEARCH
        // executor so the calling thread — which may be a transport thread — is freed
        // immediately. The listener is wrapped to convert backend-specific exceptions.
        ActionListener<AnalyticsQueryResponse> convertingListener = ActionListener.wrap(listener::onResponse, e -> {
            Exception converted = e instanceof Exception ex ? contextProvider.convertException(ex) : new RuntimeException(e);
            // A typed status (e.g. a 429 breaker) often arrives buried in a wrapper —
            // ShardFragmentStageExecution reports shard failures as RuntimeException("Stage N failed", cause),
            // so isInternalError (top-level only) would redact it to a generic 500 and drop the chain. Surface
            // the buried status-bearing exception directly so 429/503 reach the client instead of an opaque 500.
            Exception statusBearing = statusBearingCause(converted);
            if (statusBearing != null) {
                listener.onFailure(statusBearing);
            } else if (converted == e && isInternalError(converted)) {
                AnalyticsQueryTask queryTask = (AnalyticsQueryTask) task;
                String queryId = queryTask.getQueryId();
                String identifier = "unassigned".equals(queryId)
                    ? "task_id=" + task.getId()
                    : "task_id=" + task.getId() + ", query_id=" + queryId;
                logger.error(
                    new org.apache.logging.log4j.message.ParameterizedMessage("[analytics-engine] internal error [{}]", identifier),
                    converted
                );
                listener.onFailure(new RuntimeException("Internal error [" + identifier + "]"));
            } else {
                listener.onFailure(converted);
            }
        });
        ContextAwareExecutor.wrap(searchExecutor, threadPool).execute(() -> {
            try {
                executeInternal(
                    (AnalyticsQueryTask) task,
                    request.getPlan(),
                    request.getQueryCtx(),
                    request.isProfile(),
                    ActionListener.wrap(result -> {
                        if (result.isSuccess()) {
                            convertingListener.onResponse(
                                request.isProfile() ? new AnalyticsQueryResponse(result) : new AnalyticsQueryResponse(result.rows())
                            );
                        } else {
                            // executeWithProfile delivers failures via onResponse with a populated
                            // ProfiledResult.failure so the profile is always recorded; surface it
                            // here as a true onFailure so the caller doesn't see a null-rows response.
                            convertingListener.onFailure(
                                result.failure() instanceof Exception ex ? ex : new RuntimeException(result.failure())
                            );
                        }
                    }, convertingListener::onFailure)
                );
            } catch (Exception e) {
                convertingListener.onFailure(e);
            } catch (AssertionError e) {
                convertingListener.onFailure(
                    new IllegalStateException("Analytics-engine executor rejected the plan: " + e.getMessage(), e)
                );
            }
        });
    }

    /**
     * Returns true if the exception would produce a 500 response and should be redacted.
     */
    private static boolean isInternalError(Exception e) {
        return ExceptionsHelper.status(e) == RestStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * Walks the cause/suppressed chain for a typed {@link OpenSearchException} carrying a non-500 status
     * (e.g. a 429 breaker wrapped as {@code RuntimeException("Stage N failed", cbe)}). Returns it so the
     * real status reaches the client instead of being redacted to a generic 500; null when the failure is
     * genuinely internal and the redaction path should run.
     */
    static Exception statusBearingCause(Exception converted) {
        return ExceptionsHelper.<OpenSearchException>unwrapCausesAndSuppressed(
            converted,
            t -> t instanceof OpenSearchException ose && ose.status() != RestStatus.INTERNAL_SERVER_ERROR
        ).map(t -> (Exception) t).orElse(null);
    }

    /**
     * Materializes Arrow batches into row-oriented {@code Object[]}s for the
     * external query API. The scheduler yields batches (the native wire format);
     * the row materialization happens here, once, at the API edge.
     *
     * <p>Package-private for unit testing.
     */
    static Iterable<Object[]> batchesToRows(Iterable<VectorSchemaRoot> batches) {
        return batchesToRows(batches, null);
    }

    static Iterable<Object[]> batchesToRows(Iterable<VectorSchemaRoot> batches, List<String> targetColumnOrder) {
        return batchesToRows(batches, targetColumnOrder, null);
    }

    /**
     * Materializes native Arrow batches into heap {@code Object[]} rows.
     *
     * <p>Heap accounting is UPFRONT, not per-batch: {@code executeInternal} already charged the shared
     * REQUEST breaker with the pessimistic worst-case result footprint before dispatch (see
     * {@link #estimateWorstCaseResultBytes}). Here we accumulate the actual native size of the batches
     * as we drain them and, once the true size is known, call {@link ResultHeapCharge#shrinkTo} to
     * release the difference between the worst-case reservation and the actual — never growing it (the
     * upfront estimate is the ceiling). The remaining reservation is released on the terminal by the
     * caller.
     *
     * <p><b>Batch ownership.</b> The producer fulfils the batches listener and hands ownership of the
     * {@link VectorSchemaRoot}s to this method; from here they are owned by a {@link Releasables#wrap}
     * cleanup held in try-with-resources, so every path — normal completion, a {@link RuntimeException},
     * or an {@link Error} thrown mid-conversion — releases all batches' native buffers exactly once
     * (the previous {@code catch (RuntimeException)} drain leaked on {@link Error}). Consumed batches
     * are also closed eagerly inside the loop to bound peak native footprint; the wrapper close is
     * idempotent per batch, so the try-with-resources close of an already-consumed batch is a no-op.
     *
     * <p>When {@code heapCharge} is {@code null} (tests / no registered breaker) this is the original
     * unaccounted materialization.
     */
    static Iterable<Object[]> batchesToRows(
        Iterable<VectorSchemaRoot> batches,
        List<String> targetColumnOrder,
        ResultHeapCharge heapCharge
    ) {
        List<Object[]> rows = new ArrayList<>();
        // Own every batch via a single Releasable so success / RuntimeException / Error all release
        // uniformly. Each per-batch Releasable guards against double-close so eager in-loop closes and
        // the final wrapper close cannot double-free.
        List<Releasable> batchReleasables = new ArrayList<>();
        for (VectorSchemaRoot batch : batches) {
            batchReleasables.add(closeOnce(batch));
        }
        long actualNativeBytes = 0;
        try (Releasable batchCleanup = Releasables.wrap(batchReleasables)) {
            for (int i = 0; i < batchReleasables.size(); i++) {
                VectorSchemaRoot batch = ((BatchReleasable) batchReleasables.get(i)).batch();
                actualNativeBytes += estimateNativeBatchBytes(batch);
                List<FieldVector> ordered = orderedColumns(batch, targetColumnOrder);
                int colCount = ordered.size();
                int rowCount = batch.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[colCount];
                    for (int c = 0; c < colCount; c++) {
                        row[c] = ArrowValues.toJavaValue(ordered.get(c), r);
                    }
                    rows.add(row);
                }
                // Close eagerly to bound peak native footprint; idempotent, so the wrapper close is safe.
                batchReleasables.get(i).close();
            }
        }
        // Shrink the upfront worst-case reservation down to the actual materialized size (no-op if the
        // caller passed a null charge, or if actual >= worst-case which shrinkTo refuses to grow).
        if (heapCharge != null) {
            heapCharge.shrinkTo(actualNativeBytes);
        }
        return rows;
    }

    /** A {@link Releasable} that closes a {@link VectorSchemaRoot} at most once and exposes it. */
    private interface BatchReleasable extends Releasable {
        VectorSchemaRoot batch();
    }

    private static BatchReleasable closeOnce(VectorSchemaRoot batch) {
        return new BatchReleasable() {
            private boolean closed;

            @Override
            public VectorSchemaRoot batch() {
                return batch;
            }

            @Override
            public void close() {
                if (closed == false) {
                    closed = true;
                    batch.close();
                }
            }
        };
    }

    /** Native footprint of a batch: sum of its vectors' buffer sizes. Cheap, measured, no ratio. */
    private static long estimateNativeBatchBytes(VectorSchemaRoot batch) {
        long bytes = 0;
        for (FieldVector v : batch.getFieldVectors()) {
            bytes += v.getBufferSize();
        }
        return bytes;
    }

    /**
     * Worst-case <em>native</em> (pre-expansion) byte footprint of a full result, used for upfront
     * admission: {@code rowCap} rows of the output {@code rowType}. Per column:
     *
     * <ul>
     *   <li>fixed-width (INT/FLOAT/DATE → 4B, BIGINT/DOUBLE/TIMESTAMP → 8B, BOOLEAN → 1B): the Arrow
     *       value-buffer width plus 1 byte amortized validity;</li>
     *   <li>variable-width (VARCHAR/VARBINARY/other): {@code varWidthAllowanceBytes} per value plus 4
     *       bytes offset plus 1 byte validity.</li>
     * </ul>
     *
     * <p>Returns {@code rowCap × Σ(per-column width)}. This is a native-byte estimate; the caller's
     * {@link ResultHeapCharge} applies the heap-expansion factor. It is intentionally pessimistic —
     * the ceiling that {@link ResultHeapCharge#shrinkTo} later relaxes to the actual size.
     */
    static long estimateWorstCaseResultBytes(RelDataType rowType, int rowCap, long varWidthAllowanceBytes) {
        long perRow = 0;
        for (RelDataTypeField field : rowType.getFieldList()) {
            perRow += perColumnWidthBytes(field.getType().getSqlTypeName(), varWidthAllowanceBytes);
        }
        return (long) rowCap * perRow;
    }

    private static long perColumnWidthBytes(SqlTypeName type, long varWidthAllowanceBytes) {
        final long validity = 1L;
        switch (type) {
            case BOOLEAN:
                return 1L + validity;
            case TINYINT:
                return 1L + validity;
            case SMALLINT:
                return 2L + validity;
            case INTEGER:
            case FLOAT:
            case REAL:
            case DATE:
            case TIME:
                return 4L + validity;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 8L + validity;
            default:
                // variable-width (VARCHAR, VARBINARY, CHAR, and anything else): allowance + offset + validity
                return varWidthAllowanceBytes + 4L + validity;
        }
    }

    private static List<FieldVector> orderedColumns(VectorSchemaRoot batch, List<String> targetColumnOrder) {
        if (targetColumnOrder == null || targetColumnOrder.isEmpty()) {
            return batch.getFieldVectors();
        }
        List<FieldVector> ordered = new ArrayList<>(targetColumnOrder.size());
        for (String name : targetColumnOrder) {
            FieldVector vector = batch.getVector(name);
            if (vector == null) {
                throw new IllegalStateException(
                    "Column [" + name + "] expected by plan row type not found in batch schema: " + batch.getSchema().getFields()
                );
            }
            ordered.add(vector);
        }
        return ordered;
    }
}
