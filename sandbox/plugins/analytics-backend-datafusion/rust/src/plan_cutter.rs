/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The cut (whole-plan-lowering-spec.md D5/D6).
//!
//! After the whole Substrait plan is lowered into ONE DataFusion physical plan
//! (markers become [`crate::stage_boundary::StageBoundaryExec`] barriers), this
//! module walks the tree bottom-up and, at each barrier:
//!   - emits a [`CutStage`] whose plan is the barrier's input subtree, and
//!   - replaces the barrier in its parent with a
//!     [`crate::os_exec::StageReadExec`] stamped with the barrier-input schema.
//!
//! The remaining root tree is the coordinator stage (`boundary_id = ROOT`). The
//! cut performs no other tree surgery — boundary schemas are correct by
//! construction because they are read off the one tree at the exact cut point.
//!
//! D6: at each cut, the barrier-input schema is asserted against the boundary's
//! declared rowType (supplied by Java); nullable-widening is the only silent
//! acceptance. (The declared rowType is optional here; when absent the cut still
//! records the actual schema and Java performs the DAG-level cross-check.)

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{exec_datafusion_err, Result};
use datafusion::physical_plan::ExecutionPlan;

use crate::os_exec::StageReadExec;
use crate::stage_boundary::{StageBoundaryExec, ROOT_BOUNDARY_ID};

/// One stage produced by the cut: a self-contained physical plan plus the
/// boundary ids it reads from (its child stages).
pub struct CutStage {
    pub boundary_id: i32,
    pub plan: Arc<dyn ExecutionPlan>,
    pub output_schema: SchemaRef,
    /// Boundary ids this stage's plan reads (`StageReadExec` leaves) — the edges
    /// Java cross-checks against `DAGBuilder`'s own cut (D6).
    pub child_boundary_ids: Vec<i32>,
}

impl std::fmt::Debug for CutStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CutStage")
            .field("boundary_id", &self.boundary_id)
            .field("child_boundary_ids", &self.child_boundary_ids)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

/// Cut the whole physical plan at its `StageBoundaryExec` barriers.
///
/// Returns one [`CutStage`] per boundary plus the root (coordinator) stage, each
/// with no surviving `StageBoundaryExec`. `declared` optionally maps a
/// `boundary_id` to Calcite's declared rowType for the D6 schema assertion.
pub fn cut_plan(
    root: Arc<dyn ExecutionPlan>,
    declared: &HashMap<i32, SchemaRef>,
) -> Result<Vec<CutStage>> {
    let mut stages: Vec<CutStage> = Vec::new();
    let rewritten_root = cut_node(root, declared, &mut stages)?;
    // The root tree (after all barriers below it were cut out) is the coordinator stage.
    let child_boundary_ids = collect_stage_read_ids(&rewritten_root);
    let output_schema = rewritten_root.schema();
    stages.push(CutStage {
        boundary_id: ROOT_BOUNDARY_ID,
        plan: rewritten_root,
        output_schema,
        child_boundary_ids,
    });
    Ok(stages)
}

/// Recursively rewrite `node`: any `StageBoundaryExec` in the subtree is cut into
/// its own `CutStage` (pushed onto `stages`) and replaced by a `StageReadExec`.
/// Returns the rewritten subtree (barrier-free).
fn cut_node(
    node: Arc<dyn ExecutionPlan>,
    declared: &HashMap<i32, SchemaRef>,
    stages: &mut Vec<CutStage>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // First recurse into children so nested boundaries below this node are cut.
    let children = node.children();
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|c| cut_node(Arc::clone(c), declared, stages))
        .collect::<Result<_>>()?;
    let node = if new_children.is_empty() {
        node
    } else {
        node.with_new_children(new_children)?
    };

    // If this node IS a barrier, cut it: its (already-rewritten) input becomes a
    // stage, and the barrier is replaced by a StageReadExec.
    if let Some(barrier) = node.downcast_ref::<StageBoundaryExec>() {
        let boundary_id = barrier.boundary_id();
        let stage_plan = Arc::clone(barrier.input());
        let schema = stage_plan.schema();

        // D6: assert the barrier-input schema against Calcite's declared rowType.
        if let Some(decl) = declared.get(&boundary_id) {
            assert_boundary_schema(boundary_id, decl.as_ref(), schema.as_ref())?;
        }

        let child_boundary_ids = collect_stage_read_ids(&stage_plan);
        stages.push(CutStage {
            boundary_id,
            plan: stage_plan,
            output_schema: Arc::clone(&schema),
            child_boundary_ids,
        });
        return Ok(Arc::new(StageReadExec::new(boundary_id, schema)));
    }

    Ok(node)
}

/// Collect the `boundary_id`s of every `StageReadExec` leaf in `plan` (this
/// stage's inbound edges).
fn collect_stage_read_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<i32> {
    let mut ids = Vec::new();
    fn walk(plan: &Arc<dyn ExecutionPlan>, ids: &mut Vec<i32>) {
        if let Some(r) = plan.downcast_ref::<StageReadExec>() {
            ids.push(r.child_stage_id());
        }
        for c in plan.children() {
            walk(c, ids);
        }
    }
    walk(plan, &mut ids);
    ids
}

/// D6 boundary schema assertion: positions + types must match; nullable-widening
/// (declared nullable over a non-null actual field) is the only silent acceptance.
fn assert_boundary_schema(boundary_id: i32, declared: &Schema, actual: &Schema) -> Result<()> {
    if declared.fields().len() != actual.fields().len() {
        return Err(boundary_mismatch(boundary_id, declared, actual, "field count"));
    }
    for (decl, act) in declared.fields().iter().zip(actual.fields().iter()) {
        if decl.data_type() != act.data_type() {
            return Err(boundary_mismatch(boundary_id, declared, actual, "data type"));
        }
        // Allowed: declared nullable over non-null actual. Disallowed: the reverse.
        if !decl.is_nullable() && act.is_nullable() {
            return Err(boundary_mismatch(boundary_id, declared, actual, "nullability narrowing"));
        }
    }
    Ok(())
}

fn boundary_mismatch(boundary_id: i32, declared: &Schema, actual: &Schema, what: &str) -> datafusion::common::DataFusionError {
    exec_datafusion_err!(
        "stage-boundary schema mismatch ({what}) at boundary_id={boundary_id}:\n  \
         declared (Calcite): {declared:?}\n  actual (DataFusion): {actual:?}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage_boundary::{stage_boundary_logical, ExchangeType, StageBoundaryQueryPlanner, StageBoundarySerializerRegistry};
    use arrow::datatypes::{DataType, Field};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::*;

    fn has_barrier(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.downcast_ref::<StageBoundaryExec>().is_some() {
            return true;
        }
        plan.children().iter().any(|c| has_barrier(c))
    }

    async fn boundary_ctx() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_query_planner(Arc::new(StageBoundaryQueryPlanner))
            .with_serializer_registry(Arc::new(StageBoundarySerializerRegistry))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "a"])),
                Arc::new(arrow_array::Int64Array::from(vec![1i64, 2, 3])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    /// One boundary over `SUM(v) GROUP BY k`: the cut yields 2 stages (the agg
    /// subtree + the root reader), neither contains a barrier, and the root reads
    /// from the boundary.
    #[tokio::test]
    async fn cut_single_boundary_yields_two_barrier_free_stages() {
        let ctx = boundary_ctx().await;
        let agg = ctx.sql("SELECT k, SUM(v) AS s FROM t GROUP BY k").await.unwrap().logical_plan().clone();
        let wrapped = stage_boundary_logical(0, ExchangeType::Gather, agg);
        let physical = ctx.state().create_physical_plan(&wrapped).await.unwrap();

        let stages = cut_plan(physical, &HashMap::new()).unwrap();
        assert_eq!(stages.len(), 2, "expected agg stage + root stage");

        for s in &stages {
            assert!(
                !has_barrier(&s.plan),
                "stage {} still contains a barrier:\n{}",
                s.boundary_id,
                displayable(s.plan.as_ref()).indent(true)
            );
        }
        // The root stage reads from boundary 0.
        let root = stages.iter().find(|s| s.boundary_id == ROOT_BOUNDARY_ID).unwrap();
        assert_eq!(root.child_boundary_ids, vec![0]);
        // The cut-out stage (boundary 0) is the aggregate subtree.
        let b0 = stages.iter().find(|s| s.boundary_id == 0).unwrap();
        assert_eq!(b0.output_schema.fields().len(), 2, "k + s");
        assert!(b0.child_boundary_ids.is_empty(), "leaf stage has no inbound edges");
    }

    /// D6 fires when the declared boundary rowType lies about a type.
    #[tokio::test]
    async fn d6_fires_on_injected_rowtype_lie() {
        let ctx = boundary_ctx().await;
        let agg = ctx.sql("SELECT k, SUM(v) AS s FROM t GROUP BY k").await.unwrap().logical_plan().clone();
        let wrapped = stage_boundary_logical(0, ExchangeType::Gather, agg);
        let physical = ctx.state().create_physical_plan(&wrapped).await.unwrap();

        // Lie: declare boundary 0 as [k: Utf8, s: Float64] when the real s is Int64.
        let lie = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("s", DataType::Float64, true),
        ]));
        let mut declared = HashMap::new();
        declared.insert(0, lie);

        let err = cut_plan(physical, &declared).unwrap_err().to_string();
        assert!(err.contains("boundary_id=0"), "err: {err}");
        assert!(err.contains("data type"), "err: {err}");
    }

    /// Phase 0a end-to-end: cut `SUM(v) GROUP BY k`, execute the cut-out aggregate
    /// stage, feed its output through a partition stream into the root stage's
    /// `StageReadExec`, execute the root — result equals single-node execution.
    #[tokio::test]
    async fn cut_then_execute_equals_single_node() {
        use crate::session_context::StageInputRegistry;
        use datafusion::physical_plan::{collect, execute_stream};
        use futures::StreamExt;

        let ctx = boundary_ctx().await;
        let sql = "SELECT k, SUM(v) AS s FROM t GROUP BY k";

        // Single-node reference.
        let reference = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let expected = rows_sorted(&reference);
        assert_eq!(expected, vec![("a".into(), 4), ("b".into(), 2)]);

        // Whole-plan: wrap in a boundary, physical-plan, cut.
        let agg = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let wrapped = stage_boundary_logical(0, ExchangeType::Gather, agg);
        let physical = ctx.state().create_physical_plan(&wrapped).await.unwrap();
        let mut stages = cut_plan(physical, &HashMap::new()).unwrap();

        // Execute the cut-out aggregate stage (boundary 0) directly.
        let b0 = stages.iter().find(|s| s.boundary_id == 0).unwrap();
        let b0_schema = Arc::clone(&b0.output_schema);
        let b0_out = collect(Arc::clone(&b0.plan), ctx.task_ctx()).await.unwrap();
        assert!(!b0_out.is_empty());

        // Run the root stage with boundary 0's output registered as its input stream.
        // Build a session whose config carries the StageInputRegistry so StageReadExec
        // resolves boundary 0 from ctx.task_ctx().
        let registry = Arc::new(StageInputRegistry::new());
        registry.register(0, Arc::new(MemPartition::new(b0_schema, b0_out)));
        let exec_ctx = boundary_ctx_with_registry(registry).await;
        let root = stages.iter_mut().find(|s| s.boundary_id == ROOT_BOUNDARY_ID).unwrap();
        let mut stream = execute_stream(Arc::clone(&root.plan), exec_ctx.task_ctx()).unwrap();
        let mut result = Vec::new();
        while let Some(b) = stream.next().await {
            result.push(b.unwrap());
        }
        assert_eq!(rows_sorted(&result), expected, "whole-plan cut result must equal single-node");
    }

    /// §6 barrier hygiene (load-bearing). For a whole tree with a `Filter` ABOVE a
    /// boundary, after full physical optimization:
    ///   (a) the barrier survives (not eliminated),
    ///   (b) the filter stays ABOVE the barrier — not pushed through the fence,
    ///   (c) no `RepartitionExec`/`CoalescePartitionsExec` is wedged BELOW the
    ///       barrier (as its input child) — that would change the cut-out stage's
    ///       output partitioning out from under the network transport, which owns
    ///       the gather. (An exchange ABOVE the barrier is benign — it parallelizes
    ///       the consuming stage on the coordinator and is allowed.)
    ///   (d) post-cut, zero `StageBoundaryExec` remain and the barrier's input
    ///       schema equals the cut stage's output schema (passthrough).
    #[tokio::test]
    async fn barrier_is_an_optimization_fence() {
        use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};

        let ctx = boundary_ctx().await;

        // scan t → boundary(0) → filter(v > 1). The filter sits above the fence.
        let scan = ctx.sql("SELECT k, v FROM t").await.unwrap().logical_plan().clone();
        let wrapped = stage_boundary_logical(0, ExchangeType::Gather, scan);
        let filtered = LogicalPlanBuilder::from(wrapped)
            .filter(col("v").gt(lit(1i64)))
            .unwrap()
            .build()
            .unwrap();

        let physical = ctx.state().create_physical_plan(&filtered).await.unwrap();
        let rendered = displayable(physical.as_ref()).indent(true).to_string();

        // (a) barrier survives.
        assert!(has_barrier(&physical), "barrier eliminated by optimizer:\n{rendered}");

        // (b) the filter is an ANCESTOR of the barrier (above the fence), and no
        //     FilterExec appears in the barrier's input subtree (not pushed through).
        let barrier = find_barrier(&physical).expect("barrier present");
        assert!(
            !subtree_contains_name(barrier.input(), "FilterExec"),
            "filter was pushed BELOW the boundary fence:\n{rendered}"
        );
        assert!(
            ancestor_of_name_contains_barrier(&physical, "FilterExec"),
            "filter is not above the barrier:\n{rendered}"
        );

        // (c) no exchange wedged directly BELOW the barrier (its input child).
        assert!(
            !matches!(barrier.input().name(), "RepartitionExec" | "CoalescePartitionsExec"),
            "an exchange (Repartition/Coalesce) was inserted directly below the barrier — \
             it would re-partition the cut-out stage's output under the transport:\n{rendered}"
        );

        // (d) cut → no surviving barriers; passthrough schema preserved.
        let stages = cut_plan(physical, &HashMap::new()).unwrap();
        for s in &stages {
            assert!(!has_barrier(&s.plan), "post-cut barrier survived in stage {}", s.boundary_id);
        }
        let b0 = stages.iter().find(|s| s.boundary_id == 0).unwrap();
        assert_eq!(b0.output_schema.fields().len(), 2, "passthrough [k, v]");
    }

    fn find_barrier(plan: &Arc<dyn ExecutionPlan>) -> Option<&StageBoundaryExec> {
        if let Some(b) = plan.downcast_ref::<StageBoundaryExec>() {
            return Some(b);
        }
        for c in plan.children() {
            if let Some(b) = find_barrier(c) {
                return Some(b);
            }
        }
        None
    }

    fn subtree_contains_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name() == name {
            return true;
        }
        plan.children().iter().any(|c| subtree_contains_name(c, name))
    }

    /// True if some node named `name` has a `StageBoundaryExec` somewhere in its
    /// subtree (i.e. `name` is above the barrier).
    fn ancestor_of_name_contains_barrier(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name() == name && plan.children().iter().any(|c| subtree_contains_name(c, "StageBoundaryExec")) {
            return true;
        }
        plan.children().iter().any(|c| ancestor_of_name_contains_barrier(c, name))
    }

    fn rows_sorted(batches: &[arrow_array::RecordBatch]) -> Vec<(String, i64)> {
        use arrow_array::{Int64Array, StringArray};
        let mut out = Vec::new();
        for b in batches {
            let k = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((k.value(i).to_string(), s.value(i)));
            }
        }
        out.sort();
        out
    }

    /// Like `boundary_ctx`, but with the StageInputRegistry baked into the session
    /// config so a decoded `StageReadExec` resolves its stream from `ctx.task_ctx()`.
    async fn boundary_ctx_with_registry(
        registry: Arc<crate::session_context::StageInputRegistry>,
    ) -> SessionContext {
        let config = SessionConfig::new().with_extension(registry);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(Arc::new(StageBoundaryQueryPlanner))
            .with_serializer_registry(Arc::new(StageBoundarySerializerRegistry))
            .build();
        SessionContext::new_with_state(state)
    }

    #[derive(Debug)]
    struct MemPartition {
        schema: SchemaRef,
        batches: Vec<arrow_array::RecordBatch>,
    }
    impl MemPartition {
        fn new(schema: SchemaRef, batches: Vec<arrow_array::RecordBatch>) -> Self {
            Self { schema, batches }
        }
    }
    impl datafusion::physical_plan::streaming::PartitionStream for MemPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(
            &self,
            _ctx: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::physical_plan::SendableRecordBatchStream {
            let schema = Arc::clone(&self.schema);
            let batches = self.batches.clone();
            Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok)),
            ))
        }
    }
}
