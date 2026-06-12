/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `approx_distinct_state(arg) -> VARBINARY` — the SHARD-side half of the
//! non-decomposable `APPROX_COUNT_DISTINCT` state/merge pair
//! (whole-plan-lowering-spec.md §4.2, D8).
//!
//! It is an ordinary aggregate that ingests the raw column (any type DataFusion's
//! `approx_distinct` accepts) and whose `evaluate()` returns the serialized HLL
//! accumulator **state** as a single `Binary` value — instead of the cardinality.
//! The reduce side ([`super::approx_distinct_merge`]) deserializes and merges
//! those blobs and evaluates the final count.
//!
//! Because the boundary column is declared `VARBINARY` truthfully by Calcite, the
//! whole distributed tree is honest relational algebra and the uniform D6 schema
//! assertion holds — no `reduce_eval` convention, no mode forcing.
//!
//! Implementation delegates to DataFusion's own `approx_distinct` accumulator and
//! serializes its `state()` (a single Binary value for HLL). A multi-field state
//! (e.g. a t-digest for a future percentile pair) would pack into one blob here.

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;

/// UDF name. Calcite's split rule emits this on the shard side via the
/// `StateMergeRegistry`; paired with `approx_distinct_merge` on the reduce side.
pub const APPROX_DISTINCT_STATE_NAME: &str = "approx_distinct_state";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxDistinctStateUdaf::new()));
}

pub struct ApproxDistinctStateUdaf {
    signature: Signature,
}

impl ApproxDistinctStateUdaf {
    pub fn new() -> Self {
        Self {
            // One argument: the raw column to count-distinct.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for ApproxDistinctStateUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ApproxDistinctStateUdaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinctStateUdaf").finish()
    }
}

impl PartialEq for ApproxDistinctStateUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ApproxDistinctStateUdaf {}
impl Hash for ApproxDistinctStateUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        APPROX_DISTINCT_STATE_NAME.hash(state);
    }
}

impl AggregateUDFImpl for ApproxDistinctStateUdaf {
    fn name(&self) -> &str {
        APPROX_DISTINCT_STATE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The shard-side output is the opaque serialized HLL state (D8 VARBINARY).
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Build the inner approx_distinct accumulator over the real argument so the
        // HLL ingests the actual column values. Reuse the call's own arg expr/field.
        let arg_field = acc_args
            .expr_fields
            .first()
            .cloned()
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "approx_distinct_state: missing argument field".into(),
            ))?;
        let arg_expr = acc_args
            .exprs
            .first()
            .cloned()
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "approx_distinct_state: missing argument expr".into(),
            ))?;
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let inner = approx_distinct_udaf().accumulator(AccumulatorArgs {
            return_field: ret_field,
            schema: acc_args.schema,
            ignore_nulls: acc_args.ignore_nulls,
            order_bys: &[],
            name: acc_args.name,
            is_distinct: false,
            exprs: &[arg_expr],
            expr_fields: &[arg_field],
            is_reversed: false,
        })?;
        Ok(Box::new(ApproxDistinctStateAccumulator { inner }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // D8: a single opaque binary state column.
        Ok(vec![Arc::new(Field::new(
            format!("{}[hll]", args.name),
            DataType::Binary,
            true,
        ))])
    }
}

/// Accumulator: ingests raw values into an inner `approx_distinct` accumulator,
/// then `evaluate()`s to that accumulator's serialized state (one Binary blob).
struct ApproxDistinctStateAccumulator {
    inner: Box<dyn Accumulator>,
}

impl Debug for ApproxDistinctStateAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinctStateAccumulator").finish()
    }
}

impl Accumulator for ApproxDistinctStateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Intra-stage partial→final pairing of the STATE aggregate itself merges
        // HLLs the same way the inner accumulator does.
        self.inner.merge_batch(states)
    }

    /// Return the serialized HLL state as one Binary value (the shard's output).
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let state = self.inner.state()?;
        match state.into_iter().next() {
            Some(sv @ ScalarValue::Binary(_)) => Ok(sv),
            Some(other) => {
                // approx_distinct's state is a single Binary; coerce defensively.
                match other {
                    ScalarValue::Binary(b) => Ok(ScalarValue::Binary(b)),
                    o => exec_err!("approx_distinct_state: unexpected inner state type {o:?}"),
                }
            }
            None => exec_err!("approx_distinct_state: inner accumulator produced no state"),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }

    /// State columns mirror `state_fields`: one Binary HLL blob.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Schema;

    fn state_accumulator() -> ApproxDistinctStateAccumulator {
        let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("x", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let inner = approx_distinct_udaf()
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
        ApproxDistinctStateAccumulator { inner }
    }

    /// The shard-side `evaluate()` returns a Binary HLL blob (not a count), and
    /// that blob, fed to the merge accumulator, yields the correct cardinality —
    /// i.e. the state/merge pair composes (D8 / §4.2).
    #[test]
    fn state_evaluate_returns_binary_that_merge_consumes() {
        use crate::udaf::approx_distinct_merge::ApproxDistinctMergeUdaf;
        use datafusion::logical_expr::function::AccumulatorArgs;

        // Two shards over disjoint-ish value sets.
        let mut s1 = state_accumulator();
        s1.update_batch(&[Arc::new(Int64Array::from((0..100).collect::<Vec<_>>())) as ArrayRef]).unwrap();
        let blob1 = s1.evaluate().unwrap();
        let mut s2 = state_accumulator();
        s2.update_batch(&[Arc::new(Int64Array::from((50..150).collect::<Vec<_>>())) as ArrayRef]).unwrap();
        let blob2 = s2.evaluate().unwrap();

        // Each shard's evaluate() must be a Binary HLL state.
        let (b1, b2) = match (blob1, blob2) {
            (ScalarValue::Binary(Some(a)), ScalarValue::Binary(Some(b))) => (a, b),
            other => panic!("expected Binary states, got {other:?}"),
        };

        // Merge side consumes the two blobs.
        let merge = AggregateUDF::from(ApproxDistinctMergeUdaf::new());
        let field: Arc<Field> = Arc::new(Field::new("hll", DataType::Binary, true));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("hll", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let mut macc = merge
            .accumulator(AccumulatorArgs {
                return_field: ret_field,
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                name: "hll",
                is_distinct: false,
                exprs: &[expr],
                expr_fields: &[field],
                is_reversed: false,
            })
            .unwrap();
        let states: ArrayRef = Arc::new(BinaryArray::from(vec![b1.as_slice(), b2.as_slice()]));
        macc.update_batch(&[states]).unwrap();
        let count = match macc.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("expected UInt64, got {o:?}"),
        };
        // Union of 0..150 ≈ 150 distinct (HLL approximate).
        assert!((140..=160).contains(&count), "state→merge cardinality {count} ≈ 150");
    }

    /// Parity with single-node approx_distinct over the SAME values.
    #[test]
    fn state_merge_matches_single_node_approx_distinct() {
        // Single-node: one approx_distinct over all 0..150.
        let mut single = state_accumulator();
        single.update_batch(&[Arc::new(Int64Array::from((0..150).collect::<Vec<_>>())) as ArrayRef]).unwrap();
        // single's inner evaluate would give the count; read it via the inner accumulator.
        let single_count = match single.inner.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("{o:?}"),
        };

        // Distributed: two shard states merged.
        use crate::udaf::approx_distinct_merge::ApproxDistinctMergeUdaf;
        use datafusion::logical_expr::function::AccumulatorArgs;
        let mut s1 = state_accumulator();
        s1.update_batch(&[Arc::new(Int64Array::from((0..75).collect::<Vec<_>>())) as ArrayRef]).unwrap();
        let mut s2 = state_accumulator();
        s2.update_batch(&[Arc::new(Int64Array::from((75..150).collect::<Vec<_>>())) as ArrayRef]).unwrap();
        let (b1, b2) = match (s1.evaluate().unwrap(), s2.evaluate().unwrap()) {
            (ScalarValue::Binary(Some(a)), ScalarValue::Binary(Some(b))) => (a, b),
            o => panic!("{o:?}"),
        };
        let merge = AggregateUDF::from(ApproxDistinctMergeUdaf::new());
        let field: Arc<Field> = Arc::new(Field::new("hll", DataType::Binary, true));
        let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("hll", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let mut macc = merge
            .accumulator(AccumulatorArgs {
                return_field: ret_field,
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                name: "hll",
                is_distinct: false,
                exprs: &[expr],
                expr_fields: &[field],
                is_reversed: false,
            })
            .unwrap();
        macc.update_batch(&[Arc::new(BinaryArray::from(vec![b1.as_slice(), b2.as_slice()])) as ArrayRef]).unwrap();
        let dist_count = match macc.evaluate().unwrap() {
            ScalarValue::UInt64(Some(v)) => v,
            o => panic!("{o:?}"),
        };
        // Same HLL math both ways → identical estimate.
        assert_eq!(single_count, dist_count, "distributed state/merge must equal single-node approx_distinct");
    }
}
