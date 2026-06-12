/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage-boundary markers for whole-plan lowering
//! (whole-plan-lowering-spec.md D2/D3/D4).
//!
//! Calcite stamps each `OpenSearchExchangeReducer` with a `boundary_id` and the
//! convertor emits a Substrait **extension relation** named `os_stage_boundary`
//! in its place. On the Rust side:
//!
//! - [`StageBoundarySerializerRegistry`] maps the `os_stage_boundary` extension's
//!   `detail` payload (JSON `{boundary_id, exchange_type}`) to a
//!   [`StageBoundaryNode`] (`UserDefinedLogicalNodeCore`) — `DefaultSubstraitConsumer`
//!   routes `ExtensionSingleRel` through `serializer_registry().deserialize_logical_plan`.
//! - [`StageBoundaryExtensionPlanner`] lowers [`StageBoundaryNode`] to
//!   [`StageBoundaryExec`] — the barrier.
//!
//! The barrier is a single-child passthrough optimization **fence**: the physical
//! optimizer must not push operators through it or eliminate it, so that
//! [`crate::plan_cutter`] can cut the one whole physical plan at exactly the
//! points Calcite placed boundaries. `StageBoundaryExec::execute` is `unreachable!`
//! — barriers never run; they exist to be cut.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, internal_err, DFSchemaRef, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::registry::SerializerRegistry;
use serde::{Deserialize, Serialize};

/// The Substrait extension-relation type URL for a stage boundary. Both the Java
/// convertor (`detail.type_url`) and the registry below key on this exact string.
pub const STAGE_BOUNDARY_TYPE_URL: &str = "os_stage_boundary";

/// Sentinel boundary id for the coordinator (root) stage — the remaining tree
/// after all barriers are cut out.
pub const ROOT_BOUNDARY_ID: i32 = -1;

/// The boundary's exchange semantics. v1 is GATHER only; HASH/BROADCAST are the
/// MPP growth points (spec §8) and are deliberately not represented yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExchangeType {
    Gather,
}

/// JSON `detail` payload carried in the `os_stage_boundary` extension relation
/// (D2). Must match the Java convertor's emitted JSON field-for-field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageBoundaryDetail {
    pub boundary_id: i32,
    pub exchange_type: ExchangeType,
}

// ===========================================================================
// Logical node
// ===========================================================================

/// Logical marker for a distributed stage boundary. Single-input passthrough;
/// its schema is its input's schema. Carries the `boundary_id` Calcite assigned
/// so the cut can correlate with the Java-side stage DAG.
///
/// `UserDefinedLogicalNodeCore` requires `Eq + PartialOrd + Hash`; `DFSchemaRef`
/// and `LogicalPlan` don't all derive `PartialOrd`, so these are implemented by
/// hand over the identifying fields (`boundary_id`, `exchange_type`) plus the
/// input (via its display) — sufficient for the optimizer's equality/ordering use.
#[derive(Clone)]
pub struct StageBoundaryNode {
    pub boundary_id: i32,
    pub exchange_type: ExchangeType,
    input: LogicalPlan,
    /// Cached = input's schema (a boundary is a passthrough).
    schema: DFSchemaRef,
}

impl StageBoundaryNode {
    pub fn new(boundary_id: i32, exchange_type: ExchangeType, input: LogicalPlan) -> Self {
        let schema = Arc::clone(input.schema());
        Self { boundary_id, exchange_type, input, schema }
    }
}

impl PartialEq for StageBoundaryNode {
    fn eq(&self, other: &Self) -> bool {
        self.boundary_id == other.boundary_id
            && self.exchange_type == other.exchange_type
            && self.input == other.input
    }
}
impl Eq for StageBoundaryNode {}

impl PartialOrd for StageBoundaryNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.boundary_id.partial_cmp(&other.boundary_id) {
            Some(Ordering::Equal) => self.exchange_type.partial_cmp(&other.exchange_type),
            ord => ord,
        }
    }
}

impl Hash for StageBoundaryNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.boundary_id.hash(state);
        self.exchange_type.hash(state);
        self.input.hash(state);
    }
}

impl Debug for StageBoundaryNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StageBoundary: boundary_id={}, exchange_type={:?}",
            self.boundary_id, self.exchange_type
        )
    }
}

impl UserDefinedLogicalNodeCore for StageBoundaryNode {
    fn name(&self) -> &str {
        "StageBoundary"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    /// A boundary is an optimization fence (D4): no predicate may be pushed below it.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "StageBoundary: boundary_id={}, exchange_type={:?}",
            self.boundary_id, self.exchange_type
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return internal_err!("StageBoundaryNode takes no expressions, got {}", exprs.len());
        }
        if inputs.len() != 1 {
            return internal_err!("StageBoundaryNode takes exactly one input, got {}", inputs.len());
        }
        Ok(StageBoundaryNode::new(self.boundary_id, self.exchange_type, inputs.remove(0)))
    }
}

// ===========================================================================
// Serializer registry (Substrait extension `detail` → StageBoundaryNode)
// ===========================================================================

/// Maps the `os_stage_boundary` extension `detail` (JSON) to a
/// [`StageBoundaryNode`]. Registered on the session via
/// `SessionStateBuilder::with_serializer_registry`; `DefaultSubstraitConsumer`
/// invokes `deserialize_logical_plan(type_url, value)` for an `ExtensionSingleRel`.
///
/// Note: the consumer constructs the `Extension` node with the input attached
/// afterward (via `with_exprs_and_inputs`), so the node we return here is built
/// with an empty placeholder input that the consumer immediately replaces.
#[derive(Debug, Default)]
pub struct StageBoundarySerializerRegistry;

impl SerializerRegistry for StageBoundarySerializerRegistry {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        // Producing whole-plan Substrait from a DF plan is not part of this path
        // (Java is the producer); only deserialization is exercised. Provide a
        // symmetric impl for completeness / round-trip tests.
        let b = node
            .as_any()
            .downcast_ref::<StageBoundaryNode>()
            .ok_or_else(|| exec_datafusion_err!("serialize: not a StageBoundaryNode"))?;
        let detail = StageBoundaryDetail {
            boundary_id: b.boundary_id,
            exchange_type: b.exchange_type,
        };
        serde_json::to_vec(&detail)
            .map_err(|e| exec_datafusion_err!("serialize StageBoundaryDetail: {e}"))
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name != STAGE_BOUNDARY_TYPE_URL {
            return Err(exec_datafusion_err!(
                "StageBoundarySerializerRegistry: unknown extension type_url '{name}'"
            ));
        }
        let detail: StageBoundaryDetail = serde_json::from_slice(bytes)
            .map_err(|e| exec_datafusion_err!("decode StageBoundaryDetail: {e}"))?;
        // The consumer replaces this placeholder input with the real child via
        // `with_exprs_and_inputs`. Use an empty relation as the placeholder.
        let placeholder = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        Ok(Arc::new(StageBoundaryNode::new(
            detail.boundary_id,
            detail.exchange_type,
            placeholder,
        )))
    }
}

// ===========================================================================
// Physical barrier
// ===========================================================================

/// Physical barrier for a stage boundary (D4). Single-child passthrough; never
/// executes. Exists purely so [`crate::plan_cutter`] can locate the cut point in
/// the one whole physical plan.
pub struct StageBoundaryExec {
    boundary_id: i32,
    exchange_type: ExchangeType,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StageBoundaryExec {
    pub fn new(boundary_id: i32, exchange_type: ExchangeType, input: Arc<dyn ExecutionPlan>) -> Self {
        // Schema + partitioning verbatim from the child (D4; §8 makes partitioning
        // load-bearing under MPP — declare the child's true partitioning now).
        let schema: SchemaRef = input.schema();
        let props = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            props.output_partitioning().clone(),
            props.emission_type,
            props.boundedness,
        ));
        Self { boundary_id, exchange_type, input, properties }
    }

    pub fn boundary_id(&self) -> i32 {
        self.boundary_id
    }

    pub fn exchange_type(&self) -> ExchangeType {
        self.exchange_type
    }

    /// The subtree below the barrier — this boundary's stage plan (D5).
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl Debug for StageBoundaryExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageBoundaryExec")
            .field("boundary_id", &self.boundary_id)
            .field("exchange_type", &self.exchange_type)
            .finish()
    }
}

impl DisplayAs for StageBoundaryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "StageBoundaryExec: boundary_id={}, exchange_type={:?}",
                self.boundary_id, self.exchange_type
            ),
            DisplayFormatType::TreeRender => write!(f, "boundary_id={}", self.boundary_id),
        }
    }
}

impl ExecutionPlan for StageBoundaryExec {
    fn name(&self) -> &str {
        "StageBoundaryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "StageBoundaryExec takes exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(StageBoundaryExec::new(
            self.boundary_id,
            self.exchange_type,
            children.remove(0),
        )))
    }

    /// A barrier must never be optimized away or have operators pushed through it
    /// (D4); reporting that it does not benefit from input partitioning and
    /// behaving as a fence is enforced by the §6 hygiene test.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Barriers are cut out before execution (plan_cutter). Reaching here means
        // a barrier survived the cut — a hard bug, not a runtime condition.
        unreachable!(
            "StageBoundaryExec::execute called (boundary_id={}) — barriers must be cut before execution",
            self.boundary_id
        )
    }
}

// ===========================================================================
// Extension planner + query planner wiring
// ===========================================================================

/// Lowers [`StageBoundaryNode`] → [`StageBoundaryExec`] during physical planning.
#[derive(Debug, Default)]
pub struct StageBoundaryExtensionPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for StageBoundaryExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(b) = node.as_any().downcast_ref::<StageBoundaryNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return internal_err!(
                "StageBoundary expects exactly one physical input, got {}",
                physical_inputs.len()
            );
        }
        Ok(Some(Arc::new(StageBoundaryExec::new(
            b.boundary_id,
            b.exchange_type,
            Arc::clone(&physical_inputs[0]),
        ))))
    }
}

/// `QueryPlanner` that installs the [`StageBoundaryExtensionPlanner`]. Set on the
/// whole-plan session via `SessionStateBuilder::with_query_planner`.
#[derive(Debug, Default)]
pub struct StageBoundaryQueryPlanner;

#[async_trait::async_trait]
impl QueryPlanner for StageBoundaryQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            StageBoundaryExtensionPlanner,
        )]);
        planner.create_physical_plan(logical_plan, session_state).await
    }
}

/// Wrap a logical plan as a `StageBoundary` extension node (used by round-trip tests
/// and the producer-side symmetry).
pub fn stage_boundary_logical(
    boundary_id: i32,
    exchange_type: ExchangeType,
    input: LogicalPlan,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(StageBoundaryNode::new(boundary_id, exchange_type, input)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detail_json_round_trips() {
        let d = StageBoundaryDetail { boundary_id: 3, exchange_type: ExchangeType::Gather };
        let bytes = serde_json::to_vec(&d).unwrap();
        // JSON shape is the wire contract with the Java convertor.
        let s = String::from_utf8(bytes.clone()).unwrap();
        assert!(s.contains("\"boundary_id\":3"), "json: {s}");
        assert!(s.contains("\"exchange_type\":\"GATHER\""), "json: {s}");
        let back: StageBoundaryDetail = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn registry_deserializes_boundary() {
        let reg = StageBoundarySerializerRegistry;
        let json = br#"{"boundary_id":7,"exchange_type":"GATHER"}"#;
        let node = reg.deserialize_logical_plan(STAGE_BOUNDARY_TYPE_URL, json).unwrap();
        let b = node.as_any().downcast_ref::<StageBoundaryNode>().expect("StageBoundaryNode");
        assert_eq!(b.boundary_id, 7);
        assert_eq!(b.exchange_type, ExchangeType::Gather);
    }

    #[test]
    fn registry_rejects_unknown_type_url() {
        let reg = StageBoundarySerializerRegistry;
        assert!(reg.deserialize_logical_plan("not_a_boundary", b"{}").is_err());
    }

    /// A logical plan containing a `StageBoundaryNode` lowers, through a session
    /// wired with the StageBoundaryQueryPlanner, to a physical plan with a
    /// `StageBoundaryExec` barrier over the real aggregate subtree.
    #[tokio::test]
    async fn boundary_node_lowers_to_barrier_exec() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::SessionStateBuilder;
        use datafusion::physical_plan::displayable;
        use datafusion::prelude::*;

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

        // Build a logical aggregate, then wrap it in a stage boundary.
        let agg = ctx
            .sql("SELECT k, SUM(v) AS s FROM t GROUP BY k")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let wrapped = stage_boundary_logical(0, ExchangeType::Gather, agg);

        let physical = ctx.state().create_physical_plan(&wrapped).await.unwrap();
        let rendered = displayable(physical.as_ref()).indent(true).to_string();
        assert!(
            rendered.contains("StageBoundaryExec: boundary_id=0"),
            "physical plan must contain the barrier:\n{rendered}"
        );
        // The barrier is the root; its child is the aggregate subtree.
        let barrier = physical
            .downcast_ref::<StageBoundaryExec>()
            .expect("root is StageBoundaryExec");
        assert_eq!(barrier.boundary_id(), 0);
        assert!(barrier.input().schema().fields().len() == 2, "child = [k, s]");
    }
}
