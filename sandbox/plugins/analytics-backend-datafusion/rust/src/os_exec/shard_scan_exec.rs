/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `OpenSearchShardScanExec` — the shard-scan leaf that travels in a serialized
//! stage plan (df-proto spec §4).
//!
//! Carries exactly the union of today's `ShardScanWithDelegationInstructionNode`
//! + `DelegationDescriptor`:
//!   - the serialized filter expression (the bool-tree wire form: the decoded
//!     DataFusion `Expr` is reconstructed via the D13 classifier entry point),
//!   - `tree_shape` (`FilterTreeShape.ordinal()`),
//!   - `delegated` payloads (Lucene-owned QueryBuilder bytes),
//!   - `requests_row_ids`,
//!   - the index/binding key,
//!   - the projected output schema.
//!
//! `execute()` builds the indexed session via the same internals
//! `createSessionContextForIndexedExecution` uses today, sourcing the bool tree
//! from the embedded expression via the D13 entry point and resolving the shard
//! reader from the `ShardBindings` `TaskContext` extension. That body is wired in
//! Phase 2b; until `plan_format=full_proto` is live, this leaf is only ever
//! constructed inside finalizer/codec tests, so its `execute()` returns a typed
//! "not yet wired" error rather than silently producing wrong results.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{not_impl_err, DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// One delegated predicate payload carried on the scan node. Mirrors
/// `proto::DelegatedExpr`; kept as a plain struct so the node is independent of
/// the prost types at the execution layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegatedExpr {
    pub annotation_id: i32,
    pub backend_id: String,
    pub payload: Vec<u8>,
}

/// Configuration bundle for a shard-scan leaf — exactly the union of the legacy
/// `ShardScanWithDelegationInstructionNode` + `DelegationDescriptor`.
#[derive(Clone, Debug)]
pub struct ShardScanConfig {
    /// The serialized filter expression. Wire form is the substrait/Expr bytes
    /// the D13 classifier entry point reconstructs the `BoolNode` tree from. May
    /// be empty for an unfiltered scan.
    pub filter_expr: Vec<u8>,
    /// `FilterTreeShape.ordinal()`.
    pub tree_shape: i32,
    /// Lucene-owned delegated predicate payloads.
    pub delegated: Vec<DelegatedExpr>,
    /// QTF query phase: emit shard-global `__row_id__`.
    pub requests_row_ids: bool,
    /// Index / shard binding key — resolves the shard reader from the
    /// `ShardBindings` `TaskContext` extension at execute time.
    pub binding_key: String,
}

/// Shard-scan leaf in a finalized stage plan.
pub struct OpenSearchShardScanExec {
    config: ShardScanConfig,
    projected_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl OpenSearchShardScanExec {
    pub fn new(config: ShardScanConfig, projected_schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            // Partition count is resolved at execute time from the bound shard's
            // file groups. Until bound, advertise a single partition; the real
            // fan-out is internal to the indexed executor (matches the legacy
            // QueryShardExec contract where assignments drive partitions).
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            config,
            projected_schema,
            properties,
        }
    }

    pub fn config(&self) -> &ShardScanConfig {
        &self.config
    }

    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }
}

impl fmt::Debug for OpenSearchShardScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenSearchShardScanExec")
            .field("binding_key", &self.config.binding_key)
            .field("tree_shape", &self.config.tree_shape)
            .field("requests_row_ids", &self.config.requests_row_ids)
            .field("delegated", &self.config.delegated.len())
            .field("schema", &self.projected_schema)
            .finish()
    }
}

impl DisplayAs for OpenSearchShardScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "OpenSearchShardScanExec: binding={}, tree_shape={}, delegated={}, row_ids={}",
                self.config.binding_key,
                self.config.tree_shape,
                self.config.delegated.len(),
                self.config.requests_row_ids
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "binding={}", self.config.binding_key)
            }
        }
    }
}

impl ExecutionPlan for OpenSearchShardScanExec {
    fn name(&self) -> &str {
        "OpenSearchShardScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "OpenSearchShardScanExec is a leaf and takes no children, got {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Phase 2b wires this: build the indexed session via the same internals
        // `create_session_context_indexed` uses today, reconstruct the BoolNode
        // tree from `config.filter_expr` via the D13 classifier entry point, and
        // resolve the shard reader from the `ShardBindings` TaskContext extension.
        // Under `plan_format` < `full_proto` this leaf is never executed on a
        // data node (shard stages still run the legacy instruction path), so a
        // hard error here is the correct guard against accidental execution.
        not_impl_err!(
            "OpenSearchShardScanExec::execute is wired in Phase 2b (plan_format=full_proto); \
             binding_key={}",
            self.config.binding_key
        )
    }
}
