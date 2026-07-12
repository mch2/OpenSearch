/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-side entry for the distributed path.
//!
//! Takes whole-query **Substrait** (what Java emits — phases marked, NOT pre-split) and produces a
//! distributed physical plan (`DistributedExec`) via `datafusion-distributed`. The data plane is
//! direct rust↔rust: the head stage runs here and pulls from data-node Workers over native gRPC.
//!
//! Flow:
//! 1. Decode Substrait, find each `NamedTable` leaf, derive its output schema from the plan's
//!    `base_schema` (in-memory, no data-node contact — Spike F), and register a `ShardScanTable`
//!    under that name so `from_substrait_plan` binds the leaf to a `ShardScanExec` placeholder.
//! 2. `from_substrait_plan` → `LogicalPlan` → `create_physical_plan`. The registered distributed
//!    planner wraps it in a `DistributedExec`, fanning the leaf out per shard via our TaskEstimator
//!    and cutting `NetworkShuffleExec`/`NetworkCoalesceExec` at the agg/sort boundaries.
//!
//! The caller supplies the per-table shard id lists (from Java's `ShardTargetResolver`) used to
//! size + place the leaf tasks. Worker/channel resolvers are registered on the `SessionState` by
//! the caller (real gRPC in prod; in-memory in tests).

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{exec_datafusion_err, exec_err, Result};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    from_substrait_named_struct, from_substrait_plan, DefaultSubstraitConsumer,
};
use prost::Message;
use substrait::proto::Plan;

use crate::api::DataFusionRuntime;
use crate::distributed::codec::ShardScanCodec;
use crate::distributed::shard_scan_exec::ShardScanExec;
use crate::distributed::shard_task_estimator::ShardScanTaskEstimator;
use crate::distributed::worker_resolver::OsWorkerResolver;

/// The marker UDF names a delegated predicate carries in a whole-query Substrait plan; the leaf
/// absorbs these so no marker FilterExec survives to a worker stage.
const DELEGATED_PREDICATE_UDF: &str = "delegated_predicate";
const DELEGATION_POSSIBLE_UDF: &str = "delegation_possible";

/// True if `expr` is (or contains at its root) a delegation marker UDF call — the predicate that
/// must be pushed into the leaf (evaluated by Lucene), never run as a DataFusion FilterExec.
fn is_delegation_marker(expr: &datafusion::logical_expr::Expr) -> bool {
    use datafusion::logical_expr::Expr;
    match expr {
        Expr::ScalarFunction(f) => {
            let n = f.func.name();
            n == DELEGATED_PREDICATE_UDF || n == DELEGATION_POSSIBLE_UDF
        }
        // A conjunction of markers (INTERLEAVED tree) — treat the whole predicate as delegated when
        // any conjunct is a marker; the leaf's indexed executor rebuilds the full BoolNode tree.
        Expr::BinaryExpr(b) => is_delegation_marker(&b.left) || is_delegation_marker(&b.right),
        _ => false,
    }
}

/// A coordinator-side TableProvider that yields a `ShardScanExec` placeholder. The placeholder's
/// schema is the plan-derived output schema; the TaskEstimator later clones it per shard.
///
/// When the query delegates a predicate to a secondary backend (Lucene), `java_delegation` carries
/// Java's serialized `DelegationDescriptor` (the per-annotation Lucene queries + tree classification)
/// verbatim; the leaf hands it back to the JVM to build the `FilterDelegationHandle`. `tree_shape` /
/// `predicate_count` classify the delegated filter for the leaf's indexed executor.
#[derive(Debug)]
pub struct ShardScanTable {
    table_name: String,
    index_uuid: String,
    schema: SchemaRef,
    /// Java-serialized DelegationDescriptor (per-annotation Lucene queries) for the FilterDelegationHandle.
    java_delegation: Option<Vec<u8>>,
    /// Shard-local leaf fragment Substrait (Filter(markers)->Read) the worker's indexed executor decodes.
    leaf_fragment: Vec<u8>,
    tree_shape: i32,
    predicate_count: i32,
    /// Non-delegated filter-pushdown leaf fragment Substrait (`Filter(real predicate)->Read`). Unlike
    /// `leaf_fragment` (Lucene-delegated, marker-driven), this carries the REAL WHERE predicate for a
    /// datafusion-scanned leaf so the worker re-plans `Filter->ListingTable` and DataFusion pushes the
    /// predicate into the parquet scan (row-group / page-index pruning). Attached UNCONDITIONALLY when
    /// present (no delegation marker needed); routes to the vanilla ListingTable path, NOT the indexed
    /// executor. Empty = no pushable filter (bare scan).
    plain_leaf_fragment: Vec<u8>,
}

impl ShardScanTable {
    pub fn new(table_name: String, index_uuid: String, schema: SchemaRef) -> Self {
        Self {
            table_name,
            index_uuid,
            schema,
            java_delegation: None,
            leaf_fragment: Vec::new(),
            tree_shape: 0,
            predicate_count: 0,
            plain_leaf_fragment: Vec::new(),
        }
    }

    /// Attach the delegation descriptor + leaf fragment + classification (indexed-query path).
    pub fn with_delegation(
        mut self,
        java_delegation: Vec<u8>,
        leaf_fragment: Vec<u8>,
        tree_shape: i32,
        predicate_count: i32,
    ) -> Self {
        self.java_delegation = Some(java_delegation);
        self.leaf_fragment = leaf_fragment;
        self.tree_shape = tree_shape;
        self.predicate_count = predicate_count;
        self
    }

    /// Attach a non-delegated filter-pushdown leaf fragment (`Filter(real predicate)->Read`). The
    /// worker re-plans it against the vanilla ListingTable so DataFusion pushes the predicate into
    /// the parquet scan. Independent of `with_delegation` (a query has at most one WHERE filter, so
    /// only one of the two is set for a given leaf).
    pub fn with_plain_leaf_fragment(mut self, plain_leaf_fragment: Vec<u8>) -> Self {
        self.plain_leaf_fragment = plain_leaf_fragment;
        self
    }
}

#[async_trait::async_trait]
impl TableProvider for ShardScanTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use datafusion::logical_expr::TableProviderFilterPushDown;
        // Absorb the delegation-marker predicate EXACTLY (drops the FilterExec — the marker UDF must
        // never execute on a worker; Lucene evaluates it at the leaf). Everything else is Unsupported
        // (left for DataFusion to evaluate natively over the parquet the leaf emits).
        Ok(filters
            .iter()
            .map(|f| {
                if self.java_delegation.is_some() && is_delegation_marker(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Honor DataFusion's projection: the leaf's OUTPUT schema must be the projected columns, not
        // the full base schema. This matters when a pushed-down (delegated) predicate references a
        // column that the output does not (e.g. `where match(body,..) | stats count() by service`
        // projects the scan to [service] while Lucene evaluates the `body` predicate internally). If
        // we kept the full base schema, DataFusion's logical (projected) vs physical (full) schemas
        // disagree → "Physical input schema should be the same ... Different number of fields".
        let output_schema: SchemaRef = match projection {
            Some(indices) => Arc::new(self.schema.project(indices).map_err(|e| {
                exec_datafusion_err!("ShardScanTable: projection {indices:?} invalid for schema: {e}")
            })?),
            None => Arc::clone(&self.schema),
        };
        let mut exec = ShardScanExec::unassigned(self.table_name.clone(), self.index_uuid.clone(), output_schema);
        // If DataFusion pushed the delegation marker into this scan, attach the descriptor so the
        // codec ships it to the leaf worker (which runs the Lucene indexed scan).
        if let Some(java) = self.java_delegation.as_ref() {
            let has_marker = filters.iter().any(is_delegation_marker);
            if has_marker {
                exec = exec.with_delegation(Some(crate::distributed::shard_scan_exec::DelegationDescriptor {
                    filter_tree: self.leaf_fragment.clone(),
                    tree_shape: self.tree_shape,
                    delegated_predicate_count: self.predicate_count,
                    requests_row_ids: false,
                    descriptor_bytes: java.clone(),
                }));
            }
        } else if self.plain_leaf_fragment.is_empty() == false {
            // Non-delegated filter pushdown: ship the `Filter(real predicate)->Read` fragment as the
            // leaf substrait with EMPTY descriptor_bytes. The Java bridge sees no DelegationDescriptor
            // (descriptor empty) -> indexed=false -> createSessionContext (ListingTable) + passes the
            // substrait to scan_stream_from_handle, which re-plans Filter->Read so DataFusion pushes
            // the predicate into the parquet scan (row-group / page-index pruning). No marker required.
            exec = exec.with_delegation(Some(crate::distributed::shard_scan_exec::DelegationDescriptor {
                filter_tree: self.plain_leaf_fragment.clone(),
                tree_shape: 0,
                delegated_predicate_count: 0,
                requests_row_ids: false,
                descriptor_bytes: Vec::new(),
            }));
        }
        Ok(Arc::new(exec))
    }
}

/// Derives a leaf's output Arrow schema from the Substrait plan's per-table `base_schema`, fully
/// in-memory (no data-node contact). Mirror of the widening path; see Spike F.
fn schema_for_table(plan: &Plan, table_name: &str, state: &SessionState) -> Result<SchemaRef> {
    let base_schema = crate::api::base_schema_for_table(plan, table_name).ok_or_else(|| {
        exec_datafusion_err!("Substrait plan has no base_schema for table '{table_name}'")
    })?;
    let extensions = Extensions::default();
    let consumer = DefaultSubstraitConsumer::new(&extensions, state);
    let df_schema = from_substrait_named_struct(&consumer, &base_schema)?;
    let arrow = df_schema.as_arrow().clone();
    // Normalize to the forms the data-node scan will emit (force-view + Substrait-narrowing), so the
    // coordinator's planned types are wire-identical to what workers produce. (Spike F caveat.)
    let arrow = if state
        .config()
        .options()
        .execution
        .parquet
        .schema_force_view_types
    {
        datafusion::datasource::file_format::parquet::transform_schema_to_view(&arrow)
    } else {
        arrow
    };
    Ok(crate::schema_coerce::coerce_inferred_schema(Arc::new(arrow)))
}

/// Registers a `ShardScanTable` for the Substrait plan's leaf table, deriving its schema from the
/// plan's base_schema. `index_uuid_for` maps a table name to its index uuid (diagnostics / future
/// routing). Phase 1 handles the single-table case (the common analytics query shape); multi-table
/// (joins/unions) extends this to every distinct `NamedTable` in Phase 2.
pub fn register_shard_tables(
    ctx: &SessionContext,
    plan_bytes: &[u8],
    index_uuid_for: impl Fn(&str) -> String,
) -> Result<Vec<String>> {
    register_shard_tables_with_delegation(ctx, plan_bytes, index_uuid_for, None, Vec::new(), 0, 0, Vec::new())
}

/// As [`register_shard_tables`], but attaches the Java-supplied delegation descriptor + the leaf
/// fragment Substrait to the leaf table so a delegated (Lucene) predicate is pushed into
/// `ShardScanExec` and shipped to the worker (which runs the indexed executor).
#[allow(clippy::too_many_arguments)]
pub fn register_shard_tables_with_delegation(
    ctx: &SessionContext,
    plan_bytes: &[u8],
    index_uuid_for: impl Fn(&str) -> String,
    java_delegation: Option<Vec<u8>>,
    leaf_fragment: Vec<u8>,
    tree_shape: i32,
    predicate_count: i32,
    plain_leaf_fragment: Vec<u8>,
) -> Result<Vec<String>> {
    let plan = Plan::decode(plan_bytes)
        .map_err(|e| exec_datafusion_err!("failed to decode Substrait plan: {e}"))?;
    let state = ctx.state();

    // Register a ShardScanTable for EVERY distinct NamedTable leaf (joins/unions have >1; a self-join
    // references the same name once). Each binds the Substrait leaf to a ShardScanExec placeholder.
    let names = crate::api::all_named_table_names(plan_bytes);
    if names.is_empty() {
        return exec_err!("Substrait plan has no NamedTable leaf to scan");
    }
    // Delegation is single-leaf today (the WHERE predicate lives on one table); attach it to the
    // first table only. Multi-leaf delegation is a follow-on.
    let delegation_table = names.first().cloned();
    for name in &names {
        let schema = schema_for_table(&plan, name, &state)?;
        let mut table = ShardScanTable::new(name.clone(), index_uuid_for(name), schema);
        if let (Some(java), Some(dt)) = (java_delegation.as_ref(), delegation_table.as_ref()) {
            if name == dt {
                table = table.with_delegation(java.clone(), leaf_fragment.clone(), tree_shape, predicate_count);
            }
        } else if plain_leaf_fragment.is_empty() == false {
            // Non-delegated filter pushdown: attach the real Filter->Read fragment to the first
            // (single) table. Mutually exclusive with delegation — a query has one WHERE filter.
            if delegation_table.as_ref() == Some(name) {
                table = table.with_plain_leaf_fragment(plain_leaf_fragment.clone());
            }
        }
        ctx.register_table(name.as_str(), Arc::new(table))?;
    }
    Ok(names)
}

/// Consumes whole-query Substrait against the (already configured) distributed coordinator context
/// and returns the distributed physical plan. The caller must have:
///   * registered `.with_distributed_planner()` + worker/channel resolvers + the `ShardScanCodec`
///     + a `ShardScanTaskEstimator` on the context's `SessionState`, and
///   * called [`register_shard_tables`] so the Substrait `NamedTable`s bind to `ShardScanExec`.
pub async fn plan_distributed(ctx: &SessionContext, plan_bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = Plan::decode(plan_bytes)
        .map_err(|e| exec_datafusion_err!("failed to decode Substrait plan: {e}"))?;
    let logical = from_substrait_plan(&ctx.state(), &plan).await?;
    ctx.state().create_physical_plan(&logical).await
}

/// Convenience for tests / simple callers: derive a single table's schema directly.
pub fn derive_single_table_schema(plan_bytes: &[u8], state: &SessionState) -> Result<(String, SchemaRef)> {
    let plan = Plan::decode(plan_bytes)
        .map_err(|e| exec_datafusion_err!("failed to decode Substrait plan: {e}"))?;
    let name = crate::api::first_named_table_name(plan_bytes)
        .ok_or_else(|| exec_datafusion_err!("no NamedTable in plan"))?;
    let schema = schema_for_table(&plan, &name, state)?;
    Ok((name, schema))
}

/// Builds a per-query distributed coordinator `SessionContext`, wired exactly like the proven
/// `distributed_tcp_test.rs` recipe: distributed planner + `OsWorkerResolver` (seeded with this
/// query's worker URLs) + `ShardScanCodec` + a `ShardScanTaskEstimator` over `shard_ids`, plus our
/// UDF/UDAF/UDWF registrations so the Substrait consumer + workers resolve functions by name.
///
/// Rebuilt per query because the `ShardScanTaskEstimator` carries this query's shard list (the
/// library binds the estimator at `SessionState` build time). The node `RuntimeEnv` is shared so we
/// reuse the node memory pool / disk / cache rather than allocating a fresh one per query.
#[allow(clippy::too_many_arguments)]
pub fn build_coordinator_context(
    runtime: &DataFusionRuntime,
    worker_urls: Vec<String>,
    by_table: std::collections::HashMap<String, crate::distributed::shard_task_estimator::TableRouting>,
    target_partitions: usize,
    query_id: i64,
    partial_reduce: bool,
    cardinality_task_count_factor: f64,
    max_tasks_per_stage: usize,
    force_partitioned_joins: bool,
) -> Result<SessionContext> {
    use datafusion_distributed::DistributedExt;

    let resolver = OsWorkerResolver::new();
    resolver.set_urls_from_strs(&worker_urls)?;

    let estimator = ShardScanTaskEstimator::per_table(by_table);

    // Propagate the query id to every worker so build_worker_session resolves the right
    // ShardRegistry entry (the worker reads "x-opensearch-query-id" from WorkerQueryContext.headers).
    let mut headers = http::HeaderMap::new();
    headers.insert(
        "x-opensearch-query-id",
        http::HeaderValue::from_str(&query_id.to_string())
            .map_err(|e| exec_datafusion_err!("bad query id header: {e}"))?,
    );

    // with_distributed_passthrough_headers returns Result (unlike the other builder methods), so
    // apply it as a discrete step before build().
    // Force PARTITIONED hash joins (both sides hash-repartitioned on the join key) rather than
    // CollectLeft (broadcast the build side). The distributed planner shuffles at a hash
    // RepartitionExec but caps a CollectLeft join to a single task — so with distributed per-shard
    // leaves underneath, a CollectLeft join collects only SOME shards' rows (placement-dependent →
    // wrong join counts). Setting the single-partition thresholds to 0 makes DataFusion always pick
    // Partitioned, which the library shuffles correctly on both legs. Gated by
    // analytics.query.distributed.force_partitioned_joins (default true; the escape hatch is opt-out).
    let mut config = SessionConfig::new().with_target_partitions(target_partitions.max(1));
    if force_partitioned_joins {
        config.options_mut().optimizer.hash_join_single_partition_threshold = 0;
        config.options_mut().optimizer.hash_join_single_partition_threshold_rows = 0;
        config.options_mut().optimizer.repartition_joins = true;
    }
    let builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime.runtime_env.clone()))
        .with_default_features()
        // Guarantee every ShardScanExec is distributed. The distributed planner only fans a leaf out
        // where it finds a network-boundary seam (hash repartition / coalesce / SPM) above it; a
        // single-partition head stage with no such seam (global window `OVER ()`, bare SELECT *,
        // filter-only) would leave the leaf UNASSIGNED and executed on the coordinator, which hosts no
        // shards. This rule (appended AFTER DataFusion's built-ins, so it sees the final physical plan
        // just before the distributed planner runs) inserts the coalesce seam over any bare leaf.
        .with_physical_optimizer_rule(Arc::new(
            crate::distributed::force_distribute_leaf::ForceDistributeLeaf::new(target_partitions),
        ))
        .with_distributed_worker_resolver(resolver)
        .with_distributed_user_codec(ShardScanCodec)
        .with_distributed_task_estimator(estimator)
        .with_distributed_passthrough_headers(headers)?
        .with_distributed_planner();
    let mut state = builder.build();
    // Tune the distributed planner (the DistributedConfig extension is registered by
    // with_distributed_planner above, so mutate it now):
    //  - partial_reduce: insert an AggregateMode::PartialReduce ABOVE the hash RepartitionExec, before
    //    the network shuffle, so high-cardinality group-bys merge partials locally and the shuffle
    //    carries far fewer rows (the "intermediate reduce" that avoids a coordinator bottleneck).
    //  - cardinality_task_count_factor: scale a stage's task count up when a node increases cardinality
    //    (>1) so wide reduces/joins spread across more workers.
    //  - max_tasks_per_stage: hard cap (0 = inherit worker count).
    //  - elide_single_task_network_boundaries=false: our leaves are node-pinned (a ShardScanExec can
    //    only run on the node hosting its shard; the coordinator hosts none). The library's default
    //    elides a 1-producer/1-consumer boundary and runs the producer inline on the head — which for a
    //    SINGLE-SHARD index would run the leaf on the shard-less coordinator ("ShardScanExec executed
    //    while still unassigned"). Disabling elision keeps the boundary so the single task is dispatched
    //    to its worker. (Multi-shard leaves already have >1 task and were unaffected.)
    {
        let opts = state.config_mut().options_mut();
        if let Ok(dcfg) = datafusion_distributed::DistributedConfig::from_config_options_mut(opts) {
            dcfg.partial_reduce = partial_reduce;
            if cardinality_task_count_factor > 0.0 {
                dcfg.cardinality_task_count_factor = cardinality_task_count_factor;
            }
            dcfg.max_tasks_per_stage = max_tasks_per_stage;
            dcfg.elide_single_task_network_boundaries = false;
        }
    }
    let ctx = SessionContext::from(state);
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);
    crate::udwf::register_all(&ctx);
    // Register the delegation marker UDFs so from_substrait_plan can resolve a
    // `delegated_predicate(annotationId)` / `delegation_possible(...)` call in a whole-query plan
    // whose WHERE predicate was delegated to a secondary backend (Lucene). Without these the
    // Substrait consumer fails name resolution. ShardScanTable pushes the marker down into the leaf
    // (see supports_filters_pushdown), so no marker FilterExec survives to a worker stage.
    ctx.register_udf(crate::indexed_table::substrait_to_tree::create_index_filter_udf());
    ctx.register_udf(crate::indexed_table::substrait_to_tree::create_delegation_possible_udf());
    Ok(ctx)
}

/// Full coordinator-side distributed execution, called from the FFM entry. Builds the per-query
/// coordinator context, registers the shard table(s) from the Substrait, plans the distributed
/// physical plan, executes its head stage, and wraps the output in the same `CrossRtStream` +
/// `QueryStreamHandle` shape as the reduce path so the existing `df_stream_next`/`df_stream_close`
/// FFM exports drain it unchanged. Returns the boxed `QueryStreamHandle` pointer as i64.
///
/// `index_uuid` stamps the single Phase-1 table's `ShardScanTable.index_uuid` (diagnostics).
#[allow(clippy::too_many_arguments)]
pub async fn distributed_execute(
    runtime: &DataFusionRuntime,
    plan_bytes: &[u8],
    worker_urls: Vec<String>,
    by_table: std::collections::HashMap<String, crate::distributed::shard_task_estimator::TableRouting>,
    index_uuid_by_table: std::collections::HashMap<String, String>,
    manager: &crate::runtime_manager::RuntimeManager,
    context_id: i64,
    java_delegation: Option<Vec<u8>>,
    leaf_fragment: Vec<u8>,
    tree_shape: i32,
    predicate_count: i32,
    plain_leaf_fragment: Vec<u8>,
    partial_reduce: bool,
    cardinality_task_count_factor: f64,
    max_tasks_per_stage: usize,
    force_partitioned_joins: bool,
) -> Result<i64> {
    use datafusion::physical_plan::execute_stream;

    // target_partitions: max shards over any single table (bounds the head-stage partitioning).
    let target_partitions = by_table.values().map(|r| r.shard_ids.len()).max().unwrap_or(1).max(worker_urls.len()).max(1);
    // partial_reduce ON: high-cardinality group-bys merge partials locally before the network shuffle
    // (the intermediate-reduce that keeps the shuffle + coordinator from bottlenecking on wide keys).
    // cardinality_task_count_factor 0.0 = keep the library default; max_tasks_per_stage 0 = inherit
    // worker count. (These become dynamic cluster settings in the Java layer below.)
    let ctx = build_coordinator_context(
        runtime,
        worker_urls,
        by_table,
        target_partitions,
        context_id,
        partial_reduce,
        cardinality_task_count_factor,
        max_tasks_per_stage,
        force_partitioned_joins,
    )?;

    // Per-table index uuid (each join leg may be a different index); fall back to the empty-key entry.
    let fallback_uuid = index_uuid_by_table.get("").cloned().unwrap_or_default();
    register_shard_tables_with_delegation(
        &ctx,
        plan_bytes,
        |name| index_uuid_by_table.get(name).cloned().unwrap_or_else(|| fallback_uuid.clone()),
        java_delegation,
        leaf_fragment,
        tree_shape,
        predicate_count,
        plain_leaf_fragment,
    )?;
    let dplan = plan_distributed(&ctx, plan_bytes).await?;

    // Log the distributed physical plan shape (stage tree: NetworkShuffleExec/NetworkCoalesceExec +
    // per-shard DistributedLeafExec task fan-out) so the MPP execution is verifiable in the node log
    // without a debugger. Routed through the native logger (native_bridge_common) — which forwards to
    // Java's RustLoggerBridge — NOT the `log` facade, which has no subscriber and never reaches the
    // node log. Gated at DEBUG: enable with
    //   PUT _cluster/settings {"transient":{"logger.org.opensearch.nativebridge.spi.RustLoggerBridge":"DEBUG"}}
    // The macro short-circuits on the Rust-side level before formatting the (potentially large) plan.
    if native_bridge_common::logger::enabled(native_bridge_common::logger::LogLevel::Debug) {
        let displayed = datafusion_distributed::display_plan_ascii(dplan.as_ref(), false);
        native_bridge_common::log_debug!("[dist-{context_id}] distributed physical plan:\n{displayed}");
    }

    // Per-query tracking + cancellation so a Java-side cancel_query(context_id) interrupts the head
    // stage. Mirrors execute_local_plan (api.rs).
    let query_context = crate::query_tracker::QueryTrackingContext::new(
        context_id,
        ctx.runtime_env().memory_pool.clone(),
        crate::query_tracker::QueryType::Coordinator,
    );
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let df_stream = execute_stream(Arc::clone(&dplan), ctx.task_ctx())?;
    let cpu_exec = manager.cpu_executor();
    let (cross_rt_stream, _abort, task_done) =
        crate::cross_rt_stream::CrossRtStream::new_with_df_error_stream_cancellable(
            df_stream,
            cpu_exec.clone(),
            token.clone(),
        );
    if let Some(rt) = cpu_exec.handle() {
        crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
    }
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    // Keep the coordinator SessionContext alive for the stream's lifetime: the DistributedExec head
    // stage holds resolver/codec/estimator state on the session. with_physical_plan stashes both.
    let handle = crate::api::QueryStreamHandle::with_physical_plan(wrapped, query_context, ctx, None, dplan)
        .with_task_done(task_done);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}
