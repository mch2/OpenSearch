/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `PhysicalExtensionCodec` for [`ShardScanExec`]. The library serializes each stage's subplan as
//! datafusion-proto bytes; our custom leaf needs this codec on BOTH the coordinator (encode) and
//! every worker (decode). Only shard identity + schema cross the wire — never file names.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{exec_datafusion_err, internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use prost::Message;

use crate::distributed::shard_scan_exec::{DelegationDescriptor, ShardScanExec};

#[derive(Clone, PartialEq, ::prost::Message)]
struct ShardScanProto {
    #[prost(string, tag = "1")]
    table_name: String,
    #[prost(string, tag = "2")]
    index_uuid: String,
    /// The shard GROUP this task scans (>1 when shards > workers). `repeated` so no shard is dropped.
    #[prost(int32, repeated, tag = "3")]
    shard_ids: Vec<i32>,
    #[prost(message, optional, tag = "4")]
    schema: Option<protobuf::Schema>,
    /// Present iff the leaf carries predicate delegation (Phase 3).
    #[prost(message, optional, tag = "5")]
    delegation: Option<DelegationProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct DelegationProto {
    #[prost(bytes = "vec", tag = "1")]
    filter_tree: Vec<u8>,
    #[prost(int32, tag = "2")]
    tree_shape: i32,
    #[prost(int32, tag = "3")]
    delegated_predicate_count: i32,
    #[prost(bool, tag = "4")]
    requests_row_ids: bool,
    #[prost(bytes = "vec", tag = "5")]
    descriptor_bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct ShardScanCodec;

impl PhysicalExtensionCodec for ShardScanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!("ShardScanExec is a leaf, got {} inputs", inputs.len());
        }
        let proto = ShardScanProto::decode(buf)
            .map_err(|e| exec_datafusion_err!("failed to decode ShardScanExec: {e}"))?;
        let schema: Schema = proto
            .schema
            .as_ref()
            .map(|s| s.try_into())
            .ok_or_else(|| exec_datafusion_err!("ShardScanExec proto missing schema"))??;
        let delegation = proto.delegation.map(|d| DelegationDescriptor {
            filter_tree: d.filter_tree,
            tree_shape: d.tree_shape,
            delegated_predicate_count: d.delegated_predicate_count,
            requests_row_ids: d.requests_row_ids,
            descriptor_bytes: d.descriptor_bytes,
        });
        Ok(Arc::new(
            ShardScanExec::unassigned(proto.table_name, proto.index_uuid, Arc::new(schema))
                .with_shards(proto.shard_ids)
                .with_delegation(delegation),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let scan = node
            .downcast_ref::<ShardScanExec>()
            .ok_or_else(|| exec_datafusion_err!("expected ShardScanExec, got {}", node.name()))?;
        let proto = ShardScanProto {
            table_name: scan.table_name.clone(),
            index_uuid: scan.index_uuid.clone(),
            shard_ids: scan.shard_ids.clone(),
            schema: Some(scan.output_schema().as_ref().try_into()?),
            delegation: scan.delegation.as_ref().map(|d| DelegationProto {
                filter_tree: d.filter_tree.clone(),
                tree_shape: d.tree_shape,
                delegated_predicate_count: d.delegated_predicate_count,
                requests_row_ids: d.requests_row_ids,
                descriptor_bytes: d.descriptor_bytes.clone(),
            }),
        };
        proto
            .encode(buf)
            .map_err(|e| exec_datafusion_err!("failed to encode ShardScanExec: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    /// Codec round-trips a plain (non-delegated) ShardScanExec: identity fields + schema survive.
    #[test]
    fn roundtrip_plain() {
        let node: Arc<dyn ExecutionPlan> = Arc::new(
            ShardScanExec::unassigned("events".into(), "idx".into(), schema()).with_shards(vec![2, 5]),
        );
        let mut buf = Vec::new();
        ShardScanCodec.try_encode(Arc::clone(&node), &mut buf).unwrap();
        let decoded = ShardScanCodec
            .try_decode(&buf, &[], &TaskContext::default())
            .unwrap();
        let s = decoded.downcast_ref::<ShardScanExec>().unwrap();
        assert_eq!(s.table_name, "events");
        assert_eq!(s.index_uuid, "idx");
        assert_eq!(s.shard_ids, vec![2, 5], "the whole shard group must survive the wire");
        assert!(s.delegation.is_none());
        assert_eq!(s.output_schema().fields().len(), 2);
    }

    /// Phase 3: the delegation descriptor (filter tree + shape + flags) survives the codec, so it
    /// reaches the worker that will run the indexed/delegation branch.
    #[test]
    fn roundtrip_with_delegation() {
        let delegation = DelegationDescriptor {
            filter_tree: vec![1, 2, 3, 4, 5],
            tree_shape: 7,
            delegated_predicate_count: 3,
            requests_row_ids: true,
            descriptor_bytes: vec![9, 8, 7],
        };
        let node: Arc<dyn ExecutionPlan> = Arc::new(
            ShardScanExec::unassigned("events".into(), "idx".into(), schema())
                .with_shards(vec![1])
                .with_delegation(Some(delegation.clone())),
        );
        let mut buf = Vec::new();
        ShardScanCodec.try_encode(Arc::clone(&node), &mut buf).unwrap();
        let decoded = ShardScanCodec
            .try_decode(&buf, &[], &TaskContext::default())
            .unwrap();
        let s = decoded.downcast_ref::<ShardScanExec>().unwrap();
        assert_eq!(s.delegation.as_ref(), Some(&delegation), "delegation descriptor must survive the wire");
    }
}
