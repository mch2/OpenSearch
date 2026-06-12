/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode stripping for distributed partial/final execution.
//!
//! Per df-proto spec D4, the mode-force code (`force_aggregate_mode` and its
//! helpers) has been **relocated** into [`crate::stage_finalizer`] (moved, not
//! rewritten). This module retains only `physical_optimizer_rules_without_combine`
//! — which is shared by every session builder, not just the finalizer — and
//! re-exports the relocated items so the legacy `prepare_partial_plan` /
//! `prepare_final_plan` paths keep compiling unchanged until Phase 4 deletes them.

use std::sync::Arc;

use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};

// Relocated to stage_finalizer (D4). Re-exported for the legacy execution paths.
pub(crate) use crate::stage_finalizer::{
    apply_aggregate_mode, partial_aggregate_schema, Mode,
};

/// Returns the default physical optimizer rules with `CombinePartialFinalAggregate` removed.
pub(crate) fn physical_optimizer_rules_without_combine(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::new()
        .rules
        .into_iter()
        .filter(|r| r.name() != combine_name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::*;

    /// Helper: SessionContext with CombinePartialFinalAggregate disabled, a
    /// memtable, and a physical plan for `SELECT SUM(x) FROM t`.
    async fn make_agg_plan() -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(SessionConfig::new())
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "x",
                arrow::datatypes::DataType::Int64,
                false,
            )])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.sql("SELECT SUM(x) FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn find_agg_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
        let mut modes = Vec::new();
        if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
            modes.push(*agg.mode());
        }
        for child in plan.children() {
            modes.extend(find_agg_modes(child));
        }
        modes
    }

    #[tokio::test]
    async fn test_strip_partial_over_scan() {
        let plan = make_agg_plan().await;
        let result = apply_aggregate_mode(plan, Mode::Partial).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Partial),
            "Should contain Partial: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Final),
            "Should NOT contain Final: {}",
            plan_string(&result)
        );
    }

    #[tokio::test]
    async fn test_strip_final_over_scan() {
        let plan = make_agg_plan().await;
        let result = apply_aggregate_mode(plan, Mode::Final).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Final),
            "Should contain Final: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Should NOT contain Partial: {}",
            plan_string(&result)
        );
    }

    #[test]
    fn test_combine_rule_absent() {
        let rules = physical_optimizer_rules_without_combine();
        let combine_name = CombinePartialFinalAggregate::new().name().to_string();
        assert!(
            !rules.iter().any(|r| r.name() == combine_name),
            "CombinePartialFinalAggregate should be filtered out"
        );
        assert!(!rules.is_empty(), "Should have other optimizer rules");
    }

    #[tokio::test]
    async fn test_apply_partial_strips_final() {
        let plan = make_agg_plan().await;
        let display_before = plan_string(&plan);
        assert!(display_before.contains("AggregateExec: mode=Final"), "expected Final in plan");
        assert!(display_before.contains("AggregateExec: mode=Partial"), "expected Partial in plan");

        let stripped = apply_aggregate_mode(plan, Mode::Partial).unwrap();
        let display_after = plan_string(&stripped);
        assert!(!display_after.contains("mode=Final"), "Final should be stripped");
        assert!(display_after.contains("mode=Partial"), "Partial should remain");
    }
}
