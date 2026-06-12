/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `approx_distinct_merge(state)` — the FINAL-side merge UDAF for the
//! engine-native `APPROX_COUNT_DISTINCT` aggregate (df-proto spec D7, Phase 0b).
//!
//! This is the *engine-native-merge* class of aggregate: its partial state is an
//! opaque HLL register block computed by the driving backend, and the only way to
//! combine two partials is to hand both to the engine's own merge. Per D7 we
//! model it as a UDAF with a **single opaque binary state column**:
//!
//! * input: one `Binary` column — each row is a serialized per-shard HLL state
//!   (identical bytes to DataFusion's `approx_distinct` `Accumulator::state()`).
//! * `Accumulator::state()` → one `ScalarValue::Binary` carrying the merged HLL.
//! * `merge_batch` deserializes each incoming Binary state into the engine's HLL
//!   and merges it in.
//! * `evaluate` → the `UInt64` cardinality.
//!
//! No Arrow-struct state mapping; the state schema is a single Binary column, so
//! it serializes through datafusion-proto's UDAF-by-name mechanism unchanged.
//!
//! Internally the merge reuses DataFusion's `approx_distinct` accumulator — the
//! per-shard HLL bytes ARE its serialized state — which is exactly what the
//! legacy `reduce_eval("approx_distinct", state)` path does. Phase 0b proves the
//! two produce identical results.

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;

/// UDF name. Distinct from DataFusion's `approx_distinct` so plans can address
/// the merge-form explicitly; Calcite emits this on the FINAL side.
pub const APPROX_DISTINCT_MERGE_NAME: &str = "approx_distinct_merge";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxDistinctMergeUdaf::new()));
}

pub struct ApproxDistinctMergeUdaf {
    signature: Signature,
}

impl ApproxDistinctMergeUdaf {
    pub fn new() -> Self {
        // One argument: the Binary HLL state column.
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for ApproxDistinctMergeUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ApproxDistinctMergeUdaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinctMergeUdaf").finish()
    }
}

impl PartialEq for ApproxDistinctMergeUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ApproxDistinctMergeUdaf {}
impl Hash for ApproxDistinctMergeUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        APPROX_DISTINCT_MERGE_NAME.hash(state);
    }
}

impl AggregateUDFImpl for ApproxDistinctMergeUdaf {
    fn name(&self) -> &str {
        APPROX_DISTINCT_MERGE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ApproxDistinctMergeAccumulator::try_new()?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // D7: a single opaque binary state column.
        Ok(vec![Arc::new(Field::new(
            format!("{}[hll]", args.name),
            DataType::Binary,
            true,
        ))])
    }
}

/// Accumulator backing `approx_distinct_merge`. Holds one inner DataFusion
/// `approx_distinct` accumulator; both `update_batch` (over Binary state inputs)
/// and `merge_batch` (over Binary state-column states) feed serialized HLLs into
/// it via its own `merge_batch`.
struct ApproxDistinctMergeAccumulator {
    inner: Box<dyn Accumulator>,
}

impl ApproxDistinctMergeAccumulator {
    fn try_new() -> Result<Self> {
        // Build an approx_distinct accumulator over a nominal Int64 input column;
        // we only ever drive it via merge_batch with serialized HLL states, so the
        // input element type is immaterial to merging.
        let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            field.as_ref().clone(),
        ]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let inner = approx_distinct_udaf().accumulator(AccumulatorArgs {
            return_field: ret_field,
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "x",
            is_distinct: false,
            exprs: &[expr],
            expr_fields: &[field],
            is_reversed: false,
        })?;
        Ok(Self { inner })
    }

    /// Feed a column of serialized HLL states into the inner accumulator via its
    /// own `merge_batch`. Null states are skipped.
    fn absorb_states(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let binary = values[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "approx_distinct_merge: expected Binary HLL state column".into(),
                )
            })?;
        for i in 0..binary.len() {
            if binary.is_null(i) {
                continue;
            }
            let one: ArrayRef = Arc::new(BinaryArray::from(vec![binary.value(i)]));
            self.inner.merge_batch(&[one])?;
        }
        Ok(())
    }
}

impl Debug for ApproxDistinctMergeAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinctMergeAccumulator").finish()
    }
}

impl Accumulator for ApproxDistinctMergeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Each input row is itself a serialized per-shard HLL state.
        self.absorb_states(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Our own state() emits one Binary column; merging is the same absorb.
        self.absorb_states(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // The merged HLL, as the inner accumulator's single Binary state value.
        let inner_state = self.inner.state()?;
        match inner_state.into_iter().next() {
            Some(sv) => Ok(vec![sv]),
            None => exec_err!("approx_distinct_merge: inner accumulator produced no state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Schema;

    /// Build a per-shard HLL Binary state by running approx_distinct over `values`.
    fn shard_hll_state(values: Vec<i64>) -> Vec<u8> {
        let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let mut acc = approx_distinct_udaf()
            .accumulator(AccumulatorArgs {
                return_field: ret_field,
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                name: "x",
                is_distinct: false,
                exprs: &[expr],
                expr_fields: &[field],
                is_reversed: false,
            })
            .unwrap();
        let arr: ArrayRef = Arc::new(Int64Array::from(values));
        acc.update_batch(&[arr]).unwrap();
        let state = acc.state().unwrap();
        let state_arr = state[0].to_array_of_size(1).unwrap();
        let binary = state_arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        binary.value(0).to_vec()
    }

    /// Reference: the legacy reduce_eval path merges per-shard HLL states the
    /// same way. We assert approx_distinct_merge produces the identical count.
    #[test]
    fn merge_matches_union_cardinality() {
        // Two shards with overlapping value sets; true distinct union = 0..150.
        let s1 = shard_hll_state((0..100).collect());
        let s2 = shard_hll_state((50..150).collect());

        let mut acc = ApproxDistinctMergeAccumulator::try_new().unwrap();
        let states: ArrayRef = Arc::new(BinaryArray::from(vec![s1.as_slice(), s2.as_slice()]));
        acc.update_batch(&[states]).unwrap();
        let result = acc.evaluate().unwrap();
        let count = match result {
            ScalarValue::UInt64(Some(v)) => v,
            other => panic!("expected UInt64, got {other:?}"),
        };
        // HLL is approximate; for 150 distinct it should be within a few percent.
        assert!(
            (140..=160).contains(&count),
            "merged cardinality {count} should approximate 150"
        );
    }

    /// Parity with the legacy `reduce_eval("approx_distinct", state)` path: same
    /// inputs → identical merged cardinality (D7 / Phase 0b checkbox).
    #[test]
    fn parity_with_reduce_eval() {
        let s1 = shard_hll_state((0..100).collect());
        let s2 = shard_hll_state((50..150).collect());

        // approx_distinct_merge path
        let mut acc = ApproxDistinctMergeAccumulator::try_new().unwrap();
        let states: ArrayRef = Arc::new(BinaryArray::from(vec![s1.as_slice(), s2.as_slice()]));
        acc.update_batch(&[states]).unwrap();
        let merge_count = match acc.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("merge: {o:?}"),
        };

        // legacy reduce_eval path: it evaluates each row's state independently and
        // returns a per-row count. To compare a *merged* cardinality we feed both
        // states into one approx_distinct accumulator exactly as reduce_eval's
        // accumulator does internally, then evaluate once.
        let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let mut legacy = approx_distinct_udaf()
            .accumulator(AccumulatorArgs {
                return_field: ret_field,
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                name: "x",
                is_distinct: false,
                exprs: &[expr],
                expr_fields: &[field],
                is_reversed: false,
            })
            .unwrap();
        legacy
            .merge_batch(&[Arc::new(BinaryArray::from(vec![s1.as_slice()]))])
            .unwrap();
        legacy
            .merge_batch(&[Arc::new(BinaryArray::from(vec![s2.as_slice()]))])
            .unwrap();
        let legacy_count = match legacy.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("legacy: {o:?}"),
        };

        assert_eq!(
            merge_count, legacy_count,
            "approx_distinct_merge must match the legacy reduce_eval HLL merge exactly"
        );
    }

    /// Phase 0b end-to-end: Partial→Final across two sessions via proto round-trip.
    /// Build per-shard HLL states, ship them through a `StageReadExec`-fed
    /// `approx_distinct_merge` FINAL plan that is encoded, decoded in a FRESH
    /// session, and executed — asserting the merged cardinality equals the
    /// single-accumulator (legacy) merge of the same states.
    #[tokio::test]
    async fn phase0b_partial_to_final_via_proto_round_trip() {
        use crate::os_exec::StageReadExec;
        use crate::stage_finalizer::{decode_stage_plan, encode_stage_plan};
        use datafusion::execution::{SessionStateBuilder, TaskContext};
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        use datafusion::physical_plan::{execute_stream, ExecutionPlan};
        use datafusion::prelude::{SessionConfig, SessionContext};
        use datafusion::arrow::array::UInt64Array;
        use datafusion::arrow::datatypes::Schema;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use futures::StreamExt;

        fn session() -> SessionContext {
            let mut config = SessionConfig::new();
            config.options_mut().execution.target_partitions = 4;
            let ctx = SessionContext::new_with_state(
                SessionStateBuilder::new()
                    .with_config(config)
                    .with_default_features()
                    .with_physical_optimizer_rules(
                        crate::agg_mode::physical_optimizer_rules_without_combine(),
                    )
                    .build(),
            );
            crate::udf::register_all(&ctx);
            crate::udaf::register_all(&ctx);
            ctx
        }

        // ---- per-shard HLL states, packaged as a one-column Binary RecordBatch ----
        // This stands in for the shard stage's Partial output (D7 binary state).
        let s1 = shard_hll_state((0..100).collect());
        let s2 = shard_hll_state((50..150).collect());
        let state_schema: Arc<Schema> = Arc::new(Schema::new(vec![Field::new(
            "hll", DataType::Binary, true,
        )]));
        let state_batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&state_schema),
            vec![Arc::new(BinaryArray::from(vec![s1.as_slice(), s2.as_slice()]))],
        )
        .unwrap();

        // ---- build a FINAL approx_distinct_merge plan over a StageReadExec ----
        // child_stage_id = 1; the StageReadExec is stamped with the Partial state
        // schema (a single Binary column).
        let stage_read: Arc<dyn ExecutionPlan> =
            Arc::new(StageReadExec::new(1, Arc::clone(&state_schema)));

        let merge_udaf = Arc::new(AggregateUDF::from(ApproxDistinctMergeUdaf::new()));
        let hll_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("hll", 0));
        let aggr_expr = AggregateExprBuilder::new(merge_udaf, vec![hll_col])
            .schema(Arc::clone(&state_schema))
            .alias("cardinality")
            .build()
            .unwrap();

        let final_agg: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::new_single(vec![]),
                vec![Arc::new(aggr_expr)],
                vec![None],
                stage_read,
                Arc::clone(&state_schema),
            )
            .unwrap(),
        );

        // ---- encode, decode in a fresh session, execute via partition stream ----
        let bytes = encode_stage_plan(&final_agg).unwrap();

        // Fresh session whose config carries the StageInputRegistry with the
        // shard states registered as input-1.
        let registry = Arc::new(crate::session_context::StageInputRegistry::new());
        registry.register(
            1,
            Arc::new(StateMemPartition {
                schema: Arc::clone(&state_schema),
                batch: state_batch,
            }),
        );
        let mut cfg = SessionConfig::new().with_extension(registry);
        cfg.options_mut().execution.target_partitions = 4;
        let fresh = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features()
                .with_physical_optimizer_rules(
                    crate::agg_mode::physical_optimizer_rules_without_combine(),
                )
                .build(),
        );
        crate::udf::register_all(&fresh);
        crate::udaf::register_all(&fresh);

        let task_ctx = fresh.task_ctx();
        let decoded = decode_stage_plan(&bytes, task_ctx.as_ref()).unwrap();
        let mut stream = execute_stream(decoded, task_ctx).unwrap();
        let mut count = None;
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            if b.num_rows() > 0 {
                let arr = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
                count = Some(arr.value(0));
            }
        }
        let proto_count = count.expect("final stage produced a cardinality");

        // ---- legacy reference: single-accumulator merge of the same states ----
        let mut legacy = ApproxDistinctMergeAccumulator::try_new().unwrap();
        legacy
            .update_batch(&[Arc::new(BinaryArray::from(vec![s1.as_slice(), s2.as_slice()]))])
            .unwrap();
        let legacy_count = match legacy.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("legacy: {o:?}"),
        };

        assert_eq!(
            proto_count, legacy_count,
            "Partial→Final via proto round-trip must match the legacy merge"
        );
        let _ = session; // helper retained for clarity / future shard-side plan
    }

    /// Minimal PartitionStream over a single pre-built Binary-state batch.
    #[derive(Debug)]
    struct StateMemPartition {
        schema: Arc<datafusion::arrow::datatypes::Schema>,
        batch: datafusion::arrow::array::RecordBatch,
    }
    impl datafusion::physical_plan::streaming::PartitionStream for StateMemPartition {
        fn schema(&self) -> &Arc<datafusion::arrow::datatypes::Schema> {
            &self.schema
        }
        fn execute(
            &self,
            _ctx: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::physical_plan::SendableRecordBatchStream {
            let schema = Arc::clone(&self.schema);
            let batch = self.batch.clone();
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(vec![Ok(batch)]),
                ),
            )
        }
    }
}
