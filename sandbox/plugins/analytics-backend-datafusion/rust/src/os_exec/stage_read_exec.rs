/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `StageReadExec` — the leaf that reads a child stage's Arrow partition stream.
//!
//! Replaces the legacy `OpenSearchStageInputScan` → `input-<childStageId>`
//! `StreamingTable` read with an explicit physical leaf that travels in the
//! serialized stage plan. At the coordinator the finalizer stamps it with the
//! child stage's **actual** finalized `.schema()` (df-proto spec D5); on the data
//! node `execute()` resolves the registered partition stream for
//! `input-<child_stage_id>` and hands its batches up.
//!
//! The schema is the source of truth carried on the node — `execute()` asserts
//! the resolved stream's schema matches (D6 is enforced earlier, at finalize
//! time, against Calcite's declared rowType; here we only guard against a
//! registration/codec bug).

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use crate::session_context::stage_input_table_name;

/// Reads the partition stream produced by a finalized child stage.
pub struct StageReadExec {
    child_stage_id: i32,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StageReadExec {
    pub fn new(child_stage_id: i32, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            // Single partition: one child stage feeds one logical input stream.
            // The actual fan-in across shards already happened at the child.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child_stage_id,
            schema,
            properties,
        }
    }

    pub fn child_stage_id(&self) -> i32 {
        self.child_stage_id
    }
}

impl fmt::Debug for StageReadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageReadExec")
            .field("child_stage_id", &self.child_stage_id)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for StageReadExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StageReadExec: child_stage_id={}", self.child_stage_id)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "child_stage_id={}", self.child_stage_id)
            }
        }
    }
}

impl ExecutionPlan for StageReadExec {
    fn name(&self) -> &str {
        "StageReadExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
                "StageReadExec is a leaf and takes no children, got {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "StageReadExec only has partition 0, got {partition}"
            )));
        }
        // The finalizer registers the child stage's partition streams in a
        // `StageInputRegistry` extension on the session config before executing
        // the parent. Resolving it here hands back the child's single partition
        // stream for `input-<child_stage_id>`.
        let table_name = stage_input_table_name(self.child_stage_id);
        let registry = context
            .session_config()
            .get_extension::<crate::session_context::StageInputRegistry>()
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "StageReadExec: no StageInputRegistry on session (child_stage_id={})",
                    self.child_stage_id
                )
            })?;
        let provider = registry.get(self.child_stage_id).ok_or_else(|| {
            exec_datafusion_err!(
                "StageReadExec: no partition stream registered for {table_name} \
                 (child_stage_id={})",
                self.child_stage_id
            )
        })?;

        // Guard against a codec/registration mismatch. D6 already ran at
        // finalize time against Calcite's declared rowType; this is a
        // belt-and-suspenders check against the live stream's schema.
        if provider.schema().as_ref() != self.schema.as_ref() {
            return Err(exec_datafusion_err!(
                "StageReadExec schema mismatch for child_stage_id={}:\n  node:   {:?}\n  stream: {:?}",
                self.child_stage_id,
                self.schema,
                provider.schema()
            ));
        }
        Ok(provider.execute(context))
    }
}
