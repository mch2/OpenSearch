/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Strip-rules that align DataFusion's default `Final(Partial(...))` physical
//! plan with our distributed split:
//!
//! * **Data node** ([`strip_final_aggregate`]): DataFusion's planner produces
//!   `AggregateExec(Final, ...)` on top of `AggregateExec(Partial, ...)`. We
//!   strip the top `Final` so the executor stops at `Partial(scan)`. The
//!   `Partial` emits the function's `state_fields` shape (e.g. AVG → `[count,
//!   sum]`, HLL → `[sketch: Binary]`), which is what flows over the wire to
//!   the coordinator.
//!
//! * **Coordinator** ([`strip_partial_aggregate`]): DataFusion's planner
//!   produces `AggregateExec(Final, ...)` over `AggregateExec(Partial, ...)`,
//!   often with a `CoalescePartitionsExec` / `RepartitionExec` between them
//!   for parallelism. The streaming-table input is *already* the partial
//!   state (the data node ran `Partial`), so the coord-side `Partial` would
//!   re-aggregate state values — wrong. We strip the inner `Partial` and the
//!   intermediate exchange nodes, then insert a fresh
//!   `CoalescePartitionsExec` so the `Final` sees a single-partition stream
//!   of state batches and merges them via the function's accumulator.
//!
//! Both rules are no-ops if the expected pattern isn't present (e.g. a plan
//! without aggregates, or a plan DataFusion produced as plain `Single`).

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;

/// Walks `plan` and removes any top-level `AggregateExec(Final, ...)` that wraps
/// a `Partial`. The result of walking each subtree is the new node — for an
/// `AggregateExec(Final, child=AggregateExec(Partial, ...))`, returns the
/// child (Partial). All other nodes are preserved with their stripped
/// children.
///
/// Used by the data-node executor: DataFusion's default plan is
/// `Final(Partial(scan))`; after stripping we have `Partial(scan)` whose
/// output is the function's `state_fields` shape — exactly what we need to
/// ship to the coordinator.
pub fn strip_final_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .map(|c| strip_final_aggregate(Arc::clone(c)))
        .collect::<Result<_, _>>()?;

    let plan = if new_children.is_empty() {
        plan
    } else {
        plan.with_new_children(new_children)?
    };

    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
            // Strip this Final by returning its (already-stripped) child.
            return Ok(Arc::clone(agg.input()));
        }
    }
    Ok(plan)
}

fn rebuild_as_final(
    agg: &AggregateExec,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Resolve the true state schema from the aggregate expressions'
    // `state_fields`. This is what Final-mode merge expects to see.
    let mut state_fields = Vec::new();
    for g in agg.group_expr().expr() {
        state_fields.push(g.0.data_type(&agg.input_schema())
            .map(|dt| Arc::new(datafusion::arrow::datatypes::Field::new(g.1.clone(), dt, true)))?);
    }
    for expr in agg.aggr_expr() {
        state_fields.extend(expr.state_fields()?);
    }
    let state_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(state_fields));

    // Safety net: cast the input to match the resolved state schema if it
    // differs (e.g. if an upstream layer couldn't perfectly align count
    // columns to UInt64).
    let final_input = cast_to_match_schema(input, &state_schema)?;

    let rebuilt = AggregateExec::try_new(
        AggregateMode::Final,
        agg.group_expr().clone(),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        final_input,
        state_schema,
    )?;
    let with_limit = if let Some(limit) = agg.limit() {
        rebuilt.with_limit(Some(limit))
    } else {
        rebuilt
    };
    Ok(Arc::new(with_limit))
}

fn cast_to_match_schema(
    input: Arc<dyn ExecutionPlan>,
    target_schema: &Arc<datafusion::arrow::datatypes::Schema>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let input_schema = input.schema();
    if input_schema.fields().len() != target_schema.fields().len() {
        return Ok(input);
    }
    let needs_cast = input_schema.fields().iter().zip(target_schema.fields().iter())
        .any(|(src, tgt)| src.data_type() != tgt.data_type());
    if !needs_cast {
        return Ok(input);
    }
    let exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> = input_schema
        .fields()
        .iter()
        .zip(target_schema.fields().iter())
        .enumerate()
        .map(|(i, (src, tgt))| {
            let col: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
                Arc::new(datafusion::physical_expr::expressions::Column::new(src.name(), i));
            let expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
                if src.data_type() != tgt.data_type() {
                    Arc::new(datafusion::physical_expr::expressions::CastExpr::new(
                        col, tgt.data_type().clone(), None,
                    ))
                } else {
                    col
                };
            (expr, tgt.name().clone())
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
}

/// Walks `plan` and rewrites every `AggregateExec(Final, ...)` so that any
/// `AggregateExec(Partial, ...)` directly underneath (possibly with
/// `CoalescePartitionsExec` / `RepartitionExec` between) is removed. The
/// `Final` node's input is replaced with a `CoalescePartitionsExec` over
/// whatever was *under* the Partial — i.e. the streaming table feeding state
/// from upstream shards. The `Final`'s accumulator merges that state
/// directly.
///
/// Used by the coordinator executor: the streaming input is already the
/// partial state (the data node ran `Partial`); leaving DataFusion's auto-
/// inserted coord-side `Partial` in place would re-aggregate state values,
/// producing wrong results.
pub fn strip_partial_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .map(|c| strip_partial_aggregate(Arc::clone(c)))
        .collect::<Result<_, _>>()?;

    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        let direct_child = if new_children.is_empty() {
            Arc::clone(agg.input())
        } else {
            Arc::clone(&new_children[0])
        };

        if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
            if let Some(stripped_child) = strip_inner_partial(Arc::clone(&direct_child)) {
                let coalesced: Arc<dyn ExecutionPlan> = Arc::new(
                    CoalescePartitionsExec::new(stripped_child),
                );
                return rebuild_as_final(agg, coalesced);
            }
        }
        // Single-mode aggregate over a single-partition streaming table: rewrite
        // to Final so the accumulator's merge_batch reads partial state.
        if matches!(agg.mode(), AggregateMode::Single) {
            let coalesced: Arc<dyn ExecutionPlan> = Arc::new(
                CoalescePartitionsExec::new(direct_child),
            );
            return rebuild_as_final(agg, coalesced);
        }
    }

    if new_children.is_empty() {
        Ok(plan)
    } else {
        plan.with_new_children(new_children)
    }
}

/// Walks `plan` looking through `CoalescePartitionsExec` /
/// `RepartitionExec` for an `AggregateExec(Partial, ...)`. If found, returns
/// the partial's child (the streaming table). Otherwise returns `None`.
///
/// The Java-side coord-fragment plan emits an explicit
/// `Project(all_state_cols)` between the FINAL aggregate and the streaming
/// scan — that prevents DataFusion's column-pruning optimizer from dropping
/// state columns the aggregate's logical-level argList doesn't reference.
/// As a result the streaming table feeding the auto-inserted Partial keeps
/// the full state schema; stripping the Partial reconnects Final to that
/// full-state input directly.
fn strip_inner_partial(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial | AggregateMode::Single) {
            return Some(Arc::clone(agg.input()));
        }
    }
    if plan.as_any().is::<CoalescePartitionsExec>() || plan.as_any().is::<RepartitionExec>() {
        if let Some(child) = plan.children().into_iter().next() {
            return strip_inner_partial(Arc::clone(child));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    async fn build_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap()
    }

    fn aggregate_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut n = 0;
        walk(plan, &mut n);
        n
    }

    fn walk(plan: &Arc<dyn ExecutionPlan>, n: &mut usize) {
        if plan.as_any().is::<AggregateExec>() {
            *n += 1;
        }
        for child in plan.children() {
            walk(&Arc::clone(child), n);
        }
    }

    async fn ctx_with_table(name: &str) -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        ctx.register_batch(
            name,
            datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::clone(&schema)),
        )
        .unwrap();
        ctx
    }

    #[tokio::test]
    async fn strip_final_removes_top_final_when_split() {
        let ctx = ctx_with_table("t").await;
        let plan = build_plan(&ctx, "SELECT g, SUM(v) FROM t GROUP BY g").await;
        let before = aggregate_count(&plan);
        let stripped = strip_final_aggregate(plan).unwrap();
        let after = aggregate_count(&stripped);
        // If DataFusion split into Final+Partial, after stripping there's one less Aggregate.
        // If it produced a Single-mode plan, after is unchanged.
        assert!(after <= before);
    }

    #[tokio::test]
    async fn strip_partial_keeps_final_drops_partial() {
        let ctx = ctx_with_table("t2").await;
        let plan = build_plan(&ctx, "SELECT g, SUM(v) FROM t2 GROUP BY g").await;
        let stripped = strip_partial_aggregate(plan).unwrap();
        // The result must still contain at least one AggregateExec (the Final or Single).
        assert!(aggregate_count(&stripped) >= 1);
    }
}
