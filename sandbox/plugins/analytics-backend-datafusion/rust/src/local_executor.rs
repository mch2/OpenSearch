/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-reduce local execution.
//!
//! A [`LocalSession`] holds a DataFusion [`SessionContext`] configured to share
//! the caller-supplied [`RuntimeEnv`] (and therefore its memory pool) with the
//! rest of the node. The session is the Rust-side counterpart of
//! `DatafusionReduceSink` on the Java side:
//!
//! 1. For each declared stage input, [`LocalSession::register_partition`]
//!    creates a [`PartitionStreamSender`] / [`PartitionStreamReceiver`] pair,
//!    wraps the receiver in a [`SingleReceiverPartition`], and registers it as
//!    a [`StreamingTable`] on the session under the input id. The schema is
//!    supplied by the caller — the Java-side planner derives it from the
//!    backend's [`AggregateCapability`] (which knows each function's
//!    `state_fields` shape).
//! 2. [`LocalSession::execute_substrait`] decodes the Substrait plan against
//!    the session and produces a physical plan. Every `AggregateExec` in the
//!    resulting physical plan is then rewritten to [`AggregateMode::Final`]
//!    via [`crate::agg_phase::rewrite_aggregate_mode`] — DataFusion's
//!    substrait consumer ignores the substrait `AggregationPhase`, so this
//!    walker is what actually drives final-mode merge semantics on the coord.
//!
//! The session has no knowledge of the FFM bridge; it is exposed to Java via a
//! raw `Box::into_raw` pointer managed in `api.rs`, matching the lifecycle
//! model used by `DataFusionRuntime` / `ShardView` / `QueryStreamHandle`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, SessionStateBuilder};
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::{Optimizer, OptimizerRule};
use datafusion_physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::agg_phase::strip_partial_aggregate;
use crate::partition_stream::{channel, PartitionStreamSender, SingleReceiverPartition};

/// Returns the default physical optimizer rules with [`CombinePartialFinalAggregate`] removed.
///
/// In our distributed model the coordinator-reduce session executes the FINAL
/// aggregate over streamed partial results from data nodes. DataFusion's
/// `CombinePartialFinalAggregate` rule recombines partial+final when they're in
/// the same plan, undoing the split (e.g. averaging averages instead of merging
/// sum/count). Disable it on this session.
fn physical_optimizer_rules_without_combine() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != combine_name)
        .collect()
}

/// Returns the default logical optimizer rules with [`OptimizeProjections`] removed.
///
/// `OptimizeProjections` prunes unused columns from upstream operators based on
/// what downstream operators reference textually. The Java planner emits a
/// substrait plan whose `NamedScan.base_schema` is the partial-state schema for
/// every aggregate (declared via the backend's
/// [`AggregateDecomposition::partialStateSchema`] capability) and inserts an
/// explicit identity Project above the scan to pin every state column. Pruning
/// would still drop state columns the FINAL aggregate's argList doesn't
/// textually reference (e.g. `avg(col 0)` only mentions the count column, so
/// `OptimizeProjections` would prune the sum column despite the Project) — at
/// runtime DataFusion's Final-mode `AggregateExec` reads accumulator state
/// positionally from all state columns, so dropping any breaks the merge.
///
/// Java is the source of truth for the input schema; the optimizer must honor
/// it.
fn logical_optimizer_rules_without_projection_pruning(
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let prune_name = OptimizeProjections::new().name().to_string();
    Optimizer::new()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != prune_name)
        .collect()
}

/// Coordinator-reduce DataFusion session.
///
/// Owns a [`SessionContext`] that reuses the caller's [`RuntimeEnv`] so memory
/// accounting shares the node-wide pool. One session corresponds to one reduce
/// stage; it holds the streaming inputs registered by
/// [`Self::register_partition`] and is drained exactly once via
/// [`Self::execute_substrait`].
pub struct LocalSession {
    ctx: SessionContext,
}

impl LocalSession {
    /// Builds a session whose `SessionContext` reuses the given [`RuntimeEnv`].
    ///
    /// The runtime's memory pool, disk manager, and caches are inherited —
    /// every batch consumed or produced by this session counts against the
    /// same limits as the shard-scan path.
    pub fn new(runtime_env: &RuntimeEnv) -> Self {
        // Cheaply clone the env so the session owns a handle independent of
        // the caller. `RuntimeEnv` internally holds `Arc`s — this is a
        // lightweight clone, not a deep copy of the pool or disk manager.
        let runtime_env = Arc::new(runtime_env.clone());
        // Default target_partitions (= num_cpus) so DataFusion can fan out HashJoinExec's
        // build/probe and any inner aggregates across CPUs. The single-partition output
        // contract enforced by `execute_physical_plan` is satisfied separately by wrapping
        // the final plan in `CoalescePartitionsExec` if it ends up multi-partition (see
        // `ensure_single_partition_output`).
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_optimizer_rules(logical_optimizer_rules_without_projection_pruning())
            .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::udaf::register_all(&ctx);
        crate::udf::register_all(&ctx);
        Self { ctx }
    }

    /// Resolves the intermediate (partial) state schema for a sequence of
    /// aggregate functions by looking up each in the session's UDAF registry and
    /// invoking `state_fields`.
    ///
    /// The returned schema has, in order:
    /// 1. `group_fields` (copied verbatim — group columns are passed through
    ///    from the input unchanged).
    /// 2. For each `(func_name, input_fields)` pair, the fields produced by the
    ///    corresponding UDAF's `state_fields` (e.g. `avg` → `[count U64, sum F64]`,
    ///    `approx_distinct` → `[sketch Binary]`, `sum` → `[sum]`).
    ///
    /// This is the exact schema a data node's `AggregateExec(Partial)` emits —
    /// identical to what DataFusion uses in its own `Final(Partial(...))` physical
    /// plan. The coordinator can use this schema for the substrait
    /// `NamedScan.base_schema` and for the streaming-table registration so Final-
    /// mode merge sees the correct types (no Int64→UInt64 cast needed).
    pub fn resolve_aggregate_state_schema(
        &self,
        group_fields: &[FieldRef],
        aggregates: &[(String, Vec<FieldRef>)],
    ) -> Result<SchemaRef, DataFusionError> {
        let mut fields: Vec<FieldRef> = Vec::with_capacity(group_fields.len() + aggregates.len());
        fields.extend(group_fields.iter().cloned());
        for (name, input_fields) in aggregates {
            let udaf = self.ctx.state().udaf(name).map_err(|e| {
                DataFusionError::Execution(format!(
                    "resolve_aggregate_state_schema: UDAF '{}' not registered: {}",
                    name, e
                ))
            })?;
            let return_field = udaf
                .return_field(input_fields)
                .map_err(|e| DataFusionError::Execution(format!(
                    "resolve_aggregate_state_schema: return_field for '{}': {}",
                    name, e
                )))?;
            let args = StateFieldsArgs {
                name,
                input_fields,
                return_field,
                ordering_fields: &[],
                is_distinct: false,
            };
            let state = udaf.state_fields(args).map_err(|e| {
                DataFusionError::Execution(format!(
                    "resolve_aggregate_state_schema: state_fields for '{}': {}",
                    name, e
                ))
            })?;
            fields.extend(state);
        }
        let plain_fields: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
        Ok(Arc::new(Schema::new(plain_fields)))
    }

    /// Registers a streaming input on the session under `name` and returns the
    /// producer side of the channel.
    ///
    /// The caller supplies the schema — for FINAL-stage inputs that's the
    /// partial-state schema declared by the backend's
    /// [`AggregateCapability`] (e.g. AVG → `[F64 sum, U64 count]`,
    /// HLL → `[Binary sketch]`). The Java-side planner sets the
    /// `OpenSearchStageInputScan` row type from that capability and the
    /// substrait `NamedScan.base_schema` we receive here matches.
    pub fn register_partition(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<PartitionStreamSender, DataFusionError> {
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition: Arc<dyn PartitionStream> =
            Arc::new(SingleReceiverPartition::new(receiver));
        let table = StreamingTable::try_new(schema, vec![partition])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register streaming table '{}': {}",
                    name, e
                ))
            })?;
        Ok(sender)
    }

    /// Registers an in-memory input on the session under `name`, holding all
    /// `batches` in a single [`MemTable`] partition.
    ///
    /// Unlike [`Self::register_partition`], this method does not return a
    /// channel sender — the batches are fully materialized in the table. Used
    /// by the memtable variant of the coordinator-reduce sink, which buffers
    /// shard responses in Java and hands them across in one call.
    pub fn register_memtable(
        &mut self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register memtable '{}': {}",
                    name, e
                ))
            })?;
        Ok(())
    }

    /// Decodes a Substrait plan against the session and returns the resulting
    /// stream.
    ///
    /// Table references in the plan resolve through the session's registered
    /// streaming/memtables, so input batches pushed into
    /// [`PartitionStreamSender`]s flow naturally into the DataFusion physical
    /// plan. After physical planning, every [`AggregateExec`] is rewritten to
    /// [`AggregateMode::Final`] — DataFusion's substrait consumer ignores
    /// substrait's `AggregationPhase`, so this walker is what actually drives
    /// final-mode behavior at runtime. The returned stream is hot — polling it
    /// drives both the merge and the consumption of the streaming inputs.
    ///
    /// [`AggregateExec`]: datafusion::physical_plan::aggregates::AggregateExec
    pub async fn execute_substrait(
        &self,
        bytes: &[u8],
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let plan = Plan::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        let dataframe = self.ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let final_plan = strip_partial_aggregate(physical_plan)?;
        let coalesced_plan = ensure_single_partition_output(final_plan);
        execute_physical_plan(&self.ctx, coalesced_plan).await
    }

    /// Returns the memory pool the session's `RuntimeEnv` was built with.
    ///
    /// Used by the bridge layer to seed a per-query tracking context so
    /// reduce-stage allocations count against the same pool as the shard-scan
    /// path.
    pub fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.ctx.runtime_env().memory_pool)
    }
}

/// Executes a physical plan against the given session and returns its single
/// output stream.
async fn execute_physical_plan(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let task_ctx = ctx.task_ctx();
    if plan.output_partitioning().partition_count() != 1 {
        return Err(DataFusionError::Execution(format!(
            "Coordinator-reduce plan must produce a single partition, got {}",
            plan.output_partitioning().partition_count()
        )));
    }
    let stream = plan.execute(0, task_ctx)?;
    let schema = stream.schema();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

/// Wraps a multi-partition physical plan in a [`CoalescePartitionsExec`] so the
/// single-partition contract enforced by [`execute_physical_plan`] is satisfied.
///
/// `HashJoinExec` and `RepartitionExec` produce N partitions when
/// `target_partitions > 1`; the coord-reduce path consumes exactly one drained
/// stream, so the final operator must coalesce N → 1. This is a no-op for
/// already-single-partition plans (e.g. coord-reduce aggregates over a single
/// streaming input partition).
fn ensure_single_partition_output(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.output_partitioning().partition_count() == 1 {
        plan
    } else {
        Arc::new(CoalescePartitionsExec::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use futures::StreamExt;
    use tokio::runtime::Handle;

    fn test_runtime_env() -> RuntimeEnv {
        RuntimeEnvBuilder::new()
            .build()
            .expect("runtime env builds")
    }

    fn i64_schema(column: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, false)]))
    }

    fn i64_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("batch builds")
    }

    #[tokio::test]
    async fn execute_substrait_sums_streaming_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register succeeds");

        // Build the Substrait bytes from a SQL-built logical plan against a
        // matching session — the plan only references `input-0`, so it is
        // portable onto our real session.
        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            let _unused = producer
                .register_partition("input-0", Arc::clone(&schema))
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        // Push three batches totaling 45 = 1+2+3+4+5+6+7+8+9, then close.
        let producer_schema = Arc::clone(&schema);
        let handle = Handle::current();
        let producer = std::thread::spawn(move || {
            for chunk in &[vec![1i64, 2, 3], vec![4, 5, 6], vec![7, 8, 9]] {
                sender
                    .send_blocking(Ok(i64_batch(&producer_schema, chunk)), &handle)
                    .expect("send");
            }
            drop(sender);
        });

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        producer.join().expect("producer thread");
        assert_eq!(total, 45);
    }

    #[tokio::test]
    async fn execute_substrait_sums_memtable_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");

        let batches = vec![
            i64_batch(&schema, &[1, 2, 3]),
            i64_batch(&schema, &[4, 5, 6]),
            i64_batch(&schema, &[7, 8, 9]),
        ];
        session
            .register_memtable("input-0", Arc::clone(&schema), batches)
            .expect("register memtable");

        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            producer
                .register_memtable("input-0", Arc::clone(&schema), vec![])
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        assert_eq!(total, 45);
    }

}
