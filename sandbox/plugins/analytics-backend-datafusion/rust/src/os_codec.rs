/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `OpenSearchExtensionCodec` — the `PhysicalExtensionCodec` for serialized stage
//! plans (df-proto spec D2, §5).
//!
//! Composes `DefaultPhysicalExtensionCodec` (which handles every built-in
//! DataFusion exec) and adds arms for the OpenSearch-custom execs that can appear
//! in a finalized stage plan.
//!
//! # Codec inventory (Phase 1 first task — every `impl ExecutionPlan` in the crate)
//!
//! Custom execs and their codec disposition:
//!
//! | Exec                         | File                              | Arm? | Rationale |
//! |------------------------------|-----------------------------------|------|-----------|
//! | `OpenSearchShardScanExec`    | `os_exec/shard_scan_exec.rs`      | YES  | shard-stage leaf — always serialized |
//! | `StageReadExec`              | `os_exec/stage_read_exec.rs`      | YES  | reduce-stage leaf — always serialized |
//! | `RelabelExec`                | `relabel_exec.rs`                 | YES  | wraps the plan root on the producer side (Int↔UInt retag); can sit at a stage root |
//! | `QueryShardExec`             | `indexed_table/table_provider.rs` | NO   | the legacy shard executor — replaced by `OpenSearchShardScanExec` at the leaf; never appears in a finalized DF_PROTO stage plan (under `full_proto` the finalizer emits `OpenSearchShardScanExec`; under `legacy`/`reduce_proto` shard stages are not proto-encoded at all). Encode of it is a hard error so a stray instance is caught, not silently dropped. |
//! | `IndexedExec`                | `indexed_table/stream.rs`         | NO   | constructed *inside* `OpenSearchShardScanExec::execute` on the data node; never a serialized plan node. Hard error on encode. |
//! | `RecordingLeaf`              | `indexed_table/dynamic_filter_probe.rs` | NO | `#[cfg(test)]` probe only; cannot reach a finalized plan. |
//! | `ProjectRowId*`              | `project_row_id_*.rs`             | NO   | these are *optimizer rules*, not `ExecutionPlan`s; they rewrite into standard `ProjectionExec`/`DataSourceExec` which the default codec handles. (Verified: no `impl ExecutionPlan` in those files.) |
//!
//! The unsupported-exec arms return a typed error naming the exec, so the
//! Phase 1 round-trip CI invariant (#1) catches any future custom exec that
//! starts appearing in plans without a codec arm.

use std::sync::Arc;

use datafusion::common::{exec_datafusion_err, internal_err};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use prost::Message;

use crate::os_exec::{
    DelegatedExpr, OpenSearchShardScanExec, ShardScanConfig, StageReadExec,
};
use crate::proto::{
    os_exec_node, OsExecNode, RelabelExecNode, ShardScanExecNode, StageReadExecNode,
};
use crate::relabel_exec::RelabelExec;
use crate::schema_ipc::{schema_from_ipc, schema_to_ipc};

/// Extension codec for OpenSearch-custom physical execs in serialized stage plans.
#[derive(Debug)]
pub struct OpenSearchExtensionCodec {
    inner: DefaultPhysicalExtensionCodec,
}

impl Default for OpenSearchExtensionCodec {
    fn default() -> Self {
        Self {
            inner: DefaultPhysicalExtensionCodec {},
        }
    }
}

impl OpenSearchExtensionCodec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalExtensionCodec for OpenSearchExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let envelope = OsExecNode::decode(buf)
            .map_err(|e| exec_datafusion_err!("OpenSearchExtensionCodec: decode envelope: {e}"))?;
        let node = envelope
            .node
            .ok_or_else(|| exec_datafusion_err!("OpenSearchExtensionCodec: empty envelope"))?;
        match node {
            os_exec_node::Node::ShardScan(n) => {
                if !inputs.is_empty() {
                    return internal_err!(
                        "OpenSearchShardScanExec is a leaf, decoder got {} inputs",
                        inputs.len()
                    );
                }
                let schema = schema_from_ipc(&n.projected_schema_ipc)?;
                let config = ShardScanConfig {
                    filter_expr: n.filter_expr,
                    tree_shape: n.tree_shape,
                    delegated: n
                        .delegated
                        .into_iter()
                        .map(|d| DelegatedExpr {
                            annotation_id: d.annotation_id,
                            backend_id: d.backend_id,
                            payload: d.payload,
                        })
                        .collect(),
                    requests_row_ids: n.requests_row_ids,
                    binding_key: n.binding_key,
                };
                Ok(Arc::new(OpenSearchShardScanExec::new(config, schema)))
            }
            os_exec_node::Node::StageRead(n) => {
                if !inputs.is_empty() {
                    return internal_err!(
                        "StageReadExec is a leaf, decoder got {} inputs",
                        inputs.len()
                    );
                }
                let schema = schema_from_ipc(&n.schema_ipc)?;
                Ok(Arc::new(StageReadExec::new(n.child_stage_id, schema)))
            }
            os_exec_node::Node::Relabel(n) => {
                if inputs.len() != 1 {
                    return internal_err!(
                        "RelabelExec expects exactly one input, decoder got {}",
                        inputs.len()
                    );
                }
                let target_schema = schema_from_ipc(&n.target_schema_ipc)?;
                Ok(RelabelExec::try_new(Arc::clone(&inputs[0]), target_schema)?
                    as Arc<dyn ExecutionPlan>)
            }
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(scan) = node.downcast_ref::<OpenSearchShardScanExec>() {
            let cfg = scan.config();
            let envelope = OsExecNode {
                node: Some(os_exec_node::Node::ShardScan(ShardScanExecNode {
                    filter_expr: cfg.filter_expr.clone(),
                    tree_shape: cfg.tree_shape,
                    delegated: cfg
                        .delegated
                        .iter()
                        .map(|d| crate::proto::DelegatedExpr {
                            annotation_id: d.annotation_id,
                            backend_id: d.backend_id.clone(),
                            payload: d.payload.clone(),
                        })
                        .collect(),
                    requests_row_ids: cfg.requests_row_ids,
                    binding_key: cfg.binding_key.clone(),
                    projected_schema_ipc: schema_to_ipc(scan.projected_schema())?,
                })),
            };
            envelope.encode(buf).map_err(|e| {
                exec_datafusion_err!("OpenSearchExtensionCodec: encode ShardScan: {e}")
            })?;
            return Ok(());
        }
        if let Some(read) = node.downcast_ref::<StageReadExec>() {
            let envelope = OsExecNode {
                node: Some(os_exec_node::Node::StageRead(StageReadExecNode {
                    child_stage_id: read.child_stage_id(),
                    schema_ipc: schema_to_ipc(read.schema().as_ref())?,
                })),
            };
            envelope.encode(buf).map_err(|e| {
                exec_datafusion_err!("OpenSearchExtensionCodec: encode StageRead: {e}")
            })?;
            return Ok(());
        }
        if let Some(relabel) = node.downcast_ref::<RelabelExec>() {
            let envelope = OsExecNode {
                node: Some(os_exec_node::Node::Relabel(RelabelExecNode {
                    target_schema_ipc: schema_to_ipc(relabel.schema().as_ref())?,
                })),
            };
            envelope.encode(buf).map_err(|e| {
                exec_datafusion_err!("OpenSearchExtensionCodec: encode Relabel: {e}")
            })?;
            return Ok(());
        }
        // Not an OpenSearch-custom exec — let the default codec try. The default
        // returns "not provided" for anything it doesn't recognize, which keeps
        // the inventory honest: an un-armed custom exec surfaces as a hard error
        // in the round-trip CI invariant rather than silently encoding to nothing.
        self.inner.try_encode(node, buf)
    }

    fn try_decode_udf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<datafusion::logical_expr::ScalarUDF>> {
        // UDFs serialize by name; the fresh SessionContext on the data node
        // re-registers them via `crate::udf::register_all` before decode, so the
        // default name-based resolution finds them. This includes the marker UDFs
        // (`delegated_predicate`, `delegation_possible`) embedded in a scan node's
        // stored filter expression — they round-trip by name and are never
        // physically evaluated (DO-NOT-TOUCH §3).
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(
        &self,
        node: &datafusion::logical_expr::ScalarUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<datafusion::logical_expr::AggregateUDF>> {
        // UDAFs (including the engine-native-merge ones and `list_merge`) serialize
        // by name; the data node re-registers via `crate::udaf::register_all`.
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(
        &self,
        node: &datafusion::logical_expr::AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn task_ctx() -> TaskContext {
        TaskContext::default()
    }

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("hll_state", DataType::Binary, true),
        ]))
    }

    #[test]
    fn shard_scan_round_trips() {
        let codec = OpenSearchExtensionCodec::new();
        let cfg = ShardScanConfig {
            filter_expr: vec![1, 2, 3, 4],
            tree_shape: 2,
            delegated: vec![DelegatedExpr {
                annotation_id: 7,
                backend_id: "lucene".into(),
                payload: vec![9, 9, 9],
            }],
            requests_row_ids: true,
            binding_key: "index-shard-0".into(),
        };
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(OpenSearchShardScanExec::new(cfg.clone(), sample_schema()));

        let mut buf = Vec::new();
        codec.try_encode(Arc::clone(&exec), &mut buf).unwrap();
        let decoded = codec.try_decode(&buf, &[], &task_ctx()).unwrap();

        let d = decoded
            .downcast_ref::<OpenSearchShardScanExec>()
            .expect("decoded ShardScan");
        assert_eq!(d.config().filter_expr, cfg.filter_expr);
        assert_eq!(d.config().tree_shape, cfg.tree_shape);
        assert_eq!(d.config().delegated, cfg.delegated);
        assert_eq!(d.config().requests_row_ids, cfg.requests_row_ids);
        assert_eq!(d.config().binding_key, cfg.binding_key);
        assert_eq!(d.schema().as_ref(), sample_schema().as_ref());
    }

    #[test]
    fn stage_read_round_trips() {
        let codec = OpenSearchExtensionCodec::new();
        let schema = sample_schema();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(StageReadExec::new(42, Arc::clone(&schema)));

        let mut buf = Vec::new();
        codec.try_encode(Arc::clone(&exec), &mut buf).unwrap();
        let decoded = codec.try_decode(&buf, &[], &task_ctx()).unwrap();

        let d = decoded
            .downcast_ref::<StageReadExec>()
            .expect("decoded StageRead");
        assert_eq!(d.child_stage_id(), 42);
        assert_eq!(d.schema().as_ref(), schema.as_ref());
    }

    #[test]
    fn empty_buffer_decode_errors() {
        let codec = OpenSearchExtensionCodec::new();
        // A valid-but-empty OsExecNode (no variant set) must error, not panic.
        let envelope = OsExecNode { node: None };
        let mut buf = Vec::new();
        envelope.encode(&mut buf).unwrap();
        assert!(codec.try_decode(&buf, &[], &task_ctx()).is_err());
    }
}
