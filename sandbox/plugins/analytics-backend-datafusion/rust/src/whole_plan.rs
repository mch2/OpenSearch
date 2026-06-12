/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Whole-plan lowering entry point (whole-plan-lowering-spec.md §5, D12).
//!
//! The coordinator hands Rust ONE Substrait plan for the entire optimized
//! distributed tree (boundaries included, as `os_stage_boundary` extension
//! relations). [`plan_whole_query`]:
//!   1. builds a session wired with the stage-boundary query planner + serializer
//!      registry, and registers a pushdown-stub `TableProvider` for each scan,
//!   2. lowers the whole Substrait → one logical plan → one physical plan
//!      (markers become [`crate::stage_boundary::StageBoundaryExec`] barriers),
//!   3. swaps each real scan leaf to [`crate::os_exec::OpenSearchShardScanExec`],
//!   4. cuts the plan at the barriers ([`crate::plan_cutter`]),
//!   5. encodes each cut stage to `datafusion-proto` bytes (with a debug-build
//!      round-trip assertion), returning one entry per stage keyed by `boundary_id`.
//!
//! Nothing is re-derived, mode-forced, or schema-reconciled: boundary schemas are
//! read off the one tree at the cut point.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use serde::{Deserialize, Serialize};
use substrait::proto::Plan;

use crate::os_exec::{OpenSearchShardScanExec, PushdownStubProvider, ShardScanConfig};
use crate::plan_cutter::{cut_plan, CutStage};
use crate::stage_boundary::{StageBoundaryQueryPlanner, StageBoundarySerializerRegistry};

// ===========================================================================
// JSON metadata (D12) — Java↔Rust, in-process, low-volume
// ===========================================================================

/// One delegated predicate payload (Lucene QueryBuilder bytes), base64 in JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegatedJson {
    pub annotation_id: i32,
    pub backend_id: String,
    /// base64-encoded payload bytes.
    pub payload_b64: String,
}

/// Per-scan metadata. `delegated` is empty until Phase 3.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanJson {
    pub table: String,
    pub tree_shape: i32,
    pub requests_row_ids: bool,
    #[serde(default)]
    pub delegated: Vec<DelegatedJson>,
}

/// FFM input: the whole-query Substrait plan + per-scan metadata (D12).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPlanInput {
    pub query_id: String,
    /// base64-encoded whole-plan Substrait bytes.
    pub substrait_b64: String,
    pub scans: Vec<ScanJson>,
}

/// One finalized stage in the output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StageJson {
    pub boundary_id: i32,
    pub child_boundary_ids: Vec<i32>,
    /// base64-encoded datafusion-proto PhysicalPlanNode bytes.
    pub plan_bytes_b64: String,
    /// base64-encoded Arrow IPC schema bytes.
    pub output_schema_ipc_b64: String,
}

/// FFM output: one entry per cut stage (D12).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPlanOutput {
    pub stages: Vec<StageJson>,
}

// ===========================================================================
// Session
// ===========================================================================

/// Build a coordinator session for whole-plan lowering: the stage-boundary query
/// planner (markers → barriers), the serializer registry (extension-rel → node),
/// the combine rule kept enabled (D7 — intra-stage pairing only), and a
/// `StageInputRegistry` for any `StageReadExec` resolution at execute time.
fn whole_plan_session(runtime_env: &RuntimeEnv) -> SessionContext {
    let mut config = SessionConfig::new();
    config = config.with_extension(Arc::new(crate::session_context::StageInputRegistry::new()));
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime_env.clone()))
        .with_default_features()
        // D7: CombinePartialFinalAggregate stays ENABLED — each stage's aggregate
        // is an ordinary aggregate DF may pair-split intra-stage. No mode forcing.
        .with_query_planner(Arc::new(StageBoundaryQueryPlanner))
        .with_serializer_registry(Arc::new(StageBoundarySerializerRegistry))
        .build();
    let ctx = SessionContext::new_with_state(state);
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);
    crate::udwf::register_all(&ctx);
    ctx
}

// ===========================================================================
// Entry point
// ===========================================================================

/// Lower the whole-query Substrait, cut it at stage boundaries, and encode each
/// stage. Pure-Rust testable; the FFM wrapper (api.rs) decodes `QueryPlanInput`
/// JSON and re-encodes `QueryPlanOutput` JSON around this.
pub fn plan_whole_query(input: &QueryPlanInput, runtime_env: &RuntimeEnv) -> Result<QueryPlanOutput> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    let substrait_bytes = b64
        .decode(&input.substrait_b64)
        .map_err(|e| exec_datafusion_err!("plan_whole_query: base64 substrait: {e}"))?;

    let ctx = whole_plan_session(runtime_env);

    // 1. Register a pushdown-stub provider per scan so `from_substrait_plan` binds
    //    the real index tables without a live shard reader; the stub claims Exact
    //    pushdown so the whole filter routes into the scan (no FilterExec above).
    let scan_by_table: HashMap<&str, &ScanJson> =
        input.scans.iter().map(|s| (s.table.as_str(), s)).collect();
    let plan: Plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| exec_datafusion_err!("plan_whole_query: decode substrait: {e}"))?;
    register_scan_stubs(&ctx, &substrait_bytes)?;

    // 2. Lower whole Substrait → logical → physical (markers → barriers).
    let lowered = futures::executor::block_on(from_substrait_plan(&ctx.state(), &plan))
        .map_err(|e| exec_datafusion_err!("plan_whole_query: from_substrait_plan: {e}"))?;
    let physical = futures::executor::block_on(ctx.state().create_physical_plan(&lowered))
        .map_err(|e| exec_datafusion_err!("plan_whole_query: create_physical_plan: {e}"))?;

    // 3. Swap real scan leaves to OpenSearchShardScanExec (carrying per-scan config).
    let physical = swap_scan_leaves(physical, &scan_by_table)?;

    // 4. Cut at the barriers. (Declared rowTypes for D6 are carried by the markers
    //    themselves in the whole-plan design; the Java DAG cross-check is the
    //    primary D6 gate. No per-boundary declared map needed here.)
    let stages = cut_plan(physical, &HashMap::new())?;

    // 5. Encode each cut stage; debug-build codec round-trip assertion.
    let mut out = Vec::with_capacity(stages.len());
    for stage in &stages {
        let plan_bytes = crate::stage_finalizer::encode_stage_plan(&stage.plan)?;
        #[cfg(debug_assertions)]
        crate::stage_finalizer::assert_codec_round_trips(&stage.plan, ctx.task_ctx().as_ref())?;
        let schema_ipc = crate::schema_ipc::schema_to_ipc(stage.output_schema.as_ref())?;
        out.push(StageJson {
            boundary_id: stage.boundary_id,
            child_boundary_ids: stage.child_boundary_ids.clone(),
            plan_bytes_b64: b64.encode(&plan_bytes),
            output_schema_ipc_b64: b64.encode(&schema_ipc),
        });
    }
    Ok(QueryPlanOutput { stages: out })
}

/// Register a `PushdownStubProvider` for every NamedTable scan in the plan, using
/// the table's substrait `base_schema` as the provider schema.
fn register_scan_stubs(ctx: &SessionContext, substrait_bytes: &[u8]) -> Result<()> {
    for table in crate::api::named_table_names(substrait_bytes) {
        if let Some(schema) = crate::api::base_schema_to_arrow(substrait_bytes, &table, ctx) {
            // Last-write-wins; a plan may scan the same table twice.
            let provider = Arc::new(PushdownStubProvider::new(schema));
            let _ = ctx.deregister_table(&table);
            ctx.register_table(&table, provider)
                .map_err(|e| exec_datafusion_err!("register stub '{table}': {e}"))?;
        }
    }
    Ok(())
}

/// Swap each real scan leaf (the lowered pushdown-stub `EmptyExec`/`DataSourceExec`)
/// for an `OpenSearchShardScanExec` carrying that scan's delegation config.
fn swap_scan_leaves(
    plan: Arc<dyn ExecutionPlan>,
    scan_by_table: &HashMap<&str, &ScanJson>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // v1: a single scan kind per query is the common shape; we apply the first
    // scan's config to every stub leaf. (Multi-table joins are MPP-era, §8.)
    let cfg = scan_by_table.values().next().map(|s| build_scan_config(s));

    fn is_scan_leaf(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.children().is_empty()
            && matches!(plan.name(), "EmptyExec" | "DataSourceExec" | "MemoryExec")
    }
    fn rewrite(
        plan: Arc<dyn ExecutionPlan>,
        cfg: &Option<ShardScanConfig>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if is_scan_leaf(&plan) {
            if let Some(cfg) = cfg {
                return Ok(Arc::new(OpenSearchShardScanExec::new(cfg.clone(), plan.schema())));
            }
            return Ok(plan);
        }
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }
        let new_children: Vec<Arc<dyn ExecutionPlan>> =
            children.iter().map(|c| rewrite(Arc::clone(c), cfg)).collect::<Result<_>>()?;
        plan.with_new_children(new_children)
    }
    rewrite(plan, &cfg)
}

fn build_scan_config(scan: &ScanJson) -> ShardScanConfig {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    ShardScanConfig {
        filter_expr: Vec::new(), // embedded-filter is Phase 3
        tree_shape: scan.tree_shape,
        delegated: scan
            .delegated
            .iter()
            .map(|d| crate::os_exec::DelegatedExpr {
                annotation_id: d.annotation_id,
                backend_id: d.backend_id.clone(),
                payload: b64.decode(&d.payload_b64).unwrap_or_default(),
            })
            .collect(),
        requests_row_ids: scan.requests_row_ids,
        binding_key: scan.table.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn query_plan_input_json_round_trips() {
        let input = QueryPlanInput {
            query_id: "q1".into(),
            substrait_b64: "AAEC".into(),
            scans: vec![ScanJson {
                table: "idx".into(),
                tree_shape: 1,
                requests_row_ids: false,
                delegated: vec![],
            }],
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: QueryPlanInput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.query_id, "q1");
        assert_eq!(back.scans.len(), 1);
        assert_eq!(back.scans[0].table, "idx");
    }

    /// A single-stage whole plan (scan + filter + aggregate, NO boundary): the
    /// pipeline registers the scan stub, lowers, swaps the scan leaf to
    /// OpenSearchShardScanExec, cuts (one root stage), and encodes. Exercises every
    /// step except the multi-stage cut (which plan_cutter tests cover directly).
    #[tokio::test]
    async fn plan_whole_query_single_stage_scan_agg() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionContext;
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;
        use prost::Message as _;

        // Build whole-query Substrait for `SELECT k, SUM(v) FROM idx WHERE v > 0 GROUP BY k`
        // against a binding skeleton so the plan is portable onto the whole-plan session.
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let producer = SessionContext::new();
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
        producer.register_table("idx", Arc::new(table)).unwrap();
        let logical = producer
            .sql("SELECT k, SUM(v) AS s FROM idx WHERE v > 0 GROUP BY k")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let substrait = to_substrait_plan(&logical, &producer.state()).unwrap();
        let mut bytes = Vec::new();
        substrait.encode(&mut bytes).unwrap();

        let b64 = base64::engine::general_purpose::STANDARD;
        let input = QueryPlanInput {
            query_id: "q1".into(),
            substrait_b64: b64.encode(&bytes),
            scans: vec![ScanJson {
                table: "idx".into(),
                tree_shape: 0,
                requests_row_ids: false,
                delegated: vec![],
            }],
        };

        let env = RuntimeEnvBuilder::new().build().unwrap();
        let out = plan_whole_query(&input, &env).expect("whole-plan lowering");
        // No boundary → exactly one (root/coordinator) stage.
        assert_eq!(out.stages.len(), 1);
        assert_eq!(out.stages[0].boundary_id, crate::stage_boundary::ROOT_BOUNDARY_ID);
        assert!(!out.stages[0].plan_bytes_b64.is_empty());

        // The encoded stage must decode and contain the shard-scan leaf (scan swapped).
        let plan_bytes = b64.decode(&out.stages[0].plan_bytes_b64).unwrap();
        let exec_ctx = SessionContext::new();
        crate::udf::register_all(&exec_ctx);
        crate::udaf::register_all(&exec_ctx);
        let decoded = crate::stage_finalizer::decode_stage_plan(&plan_bytes, exec_ctx.task_ctx().as_ref()).unwrap();
        let rendered = datafusion::physical_plan::displayable(decoded.as_ref()).indent(true).to_string();
        assert!(
            rendered.contains("OpenSearchShardScanExec"),
            "scan leaf must be swapped:\n{rendered}"
        );
    }

    /// Inject an `os_stage_boundary` ExtensionSingleRel at the root of `plan`'s rel
    /// tree (wrapping the RelRoot's input). This is exactly the shape the Java
    /// convertor must emit: `ExtensionSingleRel { detail: Any{ type_url:
    /// "os_stage_boundary", value: JSON(StageBoundaryDetail) }, input: <subtree> }`.
    fn inject_root_boundary(plan: &mut Plan, boundary_id: i32) {
        use crate::stage_boundary::{ExchangeType, StageBoundaryDetail, STAGE_BOUNDARY_TYPE_URL};
        use substrait::proto::{rel, ExtensionSingleRel, Rel};

        let detail = StageBoundaryDetail { boundary_id, exchange_type: ExchangeType::Gather };
        let value = serde_json::to_vec(&detail).unwrap();
        let any = pbjson_types::Any {
            type_url: STAGE_BOUNDARY_TYPE_URL.to_string(),
            value: value.into(),
        };

        for plan_rel in &mut plan.relations {
            if let Some(substrait::proto::plan_rel::RelType::Root(root)) = &mut plan_rel.rel_type {
                let inner = root.input.take().expect("RelRoot has input");
                root.input = Some(Rel {
                    rel_type: Some(rel::RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                        common: None,
                        detail: Some(any.clone()),
                        input: Some(Box::new(inner)),
                    }))),
                });
            }
        }
    }

    /// Full multi-stage pipeline from real boundary-containing Substrait bytes:
    /// produce `SUM(v) GROUP BY k` Substrait, inject a root `os_stage_boundary`,
    /// then `plan_whole_query` consumes → barrier → cut → encode into TWO stages
    /// (the agg stage + the coordinator reader), each barrier-free and decodable.
    #[tokio::test]
    async fn plan_whole_query_two_stage_with_injected_boundary() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionContext;
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;
        use prost::Message as _;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let producer = SessionContext::new();
        producer
            .register_table("idx", Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap()))
            .unwrap();
        let logical = producer
            .sql("SELECT k, SUM(v) AS s FROM idx GROUP BY k")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let mut substrait = to_substrait_plan(&logical, &producer.state()).unwrap();
        inject_root_boundary(&mut substrait, 0);
        let mut bytes = Vec::new();
        substrait.encode(&mut bytes).unwrap();

        let b64 = base64::engine::general_purpose::STANDARD;
        let input = QueryPlanInput {
            query_id: "q2".into(),
            substrait_b64: b64.encode(&bytes),
            scans: vec![ScanJson {
                table: "idx".into(),
                tree_shape: 0,
                requests_row_ids: false,
                delegated: vec![],
            }],
        };

        let env = RuntimeEnvBuilder::new().build().unwrap();
        let out = plan_whole_query(&input, &env).expect("whole-plan lowering with boundary");

        // Two stages: boundary 0 (agg) + the coordinator root.
        assert_eq!(out.stages.len(), 2, "expected agg stage + root stage");
        let root = out
            .stages
            .iter()
            .find(|s| s.boundary_id == crate::stage_boundary::ROOT_BOUNDARY_ID)
            .expect("root stage");
        let b0 = out.stages.iter().find(|s| s.boundary_id == 0).expect("boundary 0 stage");

        // The root stage reads from boundary 0 (its only inbound edge).
        assert_eq!(root.child_boundary_ids, vec![0]);
        assert!(b0.child_boundary_ids.is_empty(), "agg leaf stage has no inbound edges");

        // Both stages decode cleanly and contain no surviving barrier; the agg
        // stage carries the swapped shard-scan leaf.
        let exec_ctx = SessionContext::new();
        crate::udf::register_all(&exec_ctx);
        crate::udaf::register_all(&exec_ctx);
        for s in [root, b0] {
            let pb = b64.decode(&s.plan_bytes_b64).unwrap();
            let decoded =
                crate::stage_finalizer::decode_stage_plan(&pb, exec_ctx.task_ctx().as_ref()).unwrap();
            let rendered =
                datafusion::physical_plan::displayable(decoded.as_ref()).indent(true).to_string();
            assert!(
                !rendered.contains("StageBoundaryExec"),
                "stage {} still has a barrier:\n{rendered}",
                s.boundary_id
            );
        }
        let b0_pb = b64.decode(&b0.plan_bytes_b64).unwrap();
        let b0_plan =
            crate::stage_finalizer::decode_stage_plan(&b0_pb, exec_ctx.task_ctx().as_ref()).unwrap();
        let b0_rendered =
            datafusion::physical_plan::displayable(b0_plan.as_ref()).indent(true).to_string();
        assert!(
            b0_rendered.contains("OpenSearchShardScanExec"),
            "agg stage must carry the swapped shard scan:\n{b0_rendered}"
        );
    }

    /// The exact JSON the Java `QueryPlanJson.encodeInput` emits must deserialize into
    /// `QueryPlanInput` (field-for-field contract, D12). Hand-written to match Java's output
    /// byte-shape: compact, `delegated` present-but-empty, snake_case keys.
    #[test]
    fn deserializes_java_shaped_input_json() {
        let java_json = r#"{"query_id":"q1","substrait_b64":"AQID","scans":[{"table":"http_logs","tree_shape":2,"requests_row_ids":false,"delegated":[]}]}"#;
        let input: QueryPlanInput = serde_json::from_str(java_json).expect("parse Java QueryPlanInput");
        assert_eq!(input.query_id, "q1");
        assert_eq!(input.scans.len(), 1);
        assert_eq!(input.scans[0].table, "http_logs");
        assert_eq!(input.scans[0].tree_shape, 2);
        assert!(!input.scans[0].requests_row_ids);
        assert!(input.scans[0].delegated.is_empty());
    }

    #[test]
    fn query_plan_output_json_round_trips() {
        let b64 = base64::engine::general_purpose::STANDARD;
        let out = QueryPlanOutput {
            stages: vec![StageJson {
                boundary_id: -1,
                child_boundary_ids: vec![0],
                plan_bytes_b64: b64.encode([1u8, 2, 3]),
                output_schema_ipc_b64: b64.encode([4u8, 5]),
            }],
        };
        let json = serde_json::to_string(&out).unwrap();
        let back: QueryPlanOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stages.len(), 1);
        assert_eq!(back.stages[0].child_boundary_ids, vec![0]);
    }
}
