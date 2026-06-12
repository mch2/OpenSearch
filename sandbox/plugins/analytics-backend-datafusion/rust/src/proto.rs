/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Wire types for the datafusion-proto stage-boundary migration
//! (df-proto-migration-implementation-spec.md §5).
//!
//! These prost-annotated structs are hand-maintained to be byte-for-byte
//! compatible with what `prost-build` emits from `../proto/stage.proto`. The
//! Brazil build does not run `protoc`, so the canonical `.proto` is documentation
//! and these structs are the compiled artifact. The `tag` numbers, field order,
//! and types here MUST match the `.proto` exactly; the round-trip test at the
//! bottom of this file guards the encoding.
//!
//! `StageMeta` travels alongside each stage's Substrait fragment into
//! [`crate::stage_finalizer`]. It carries everything the finalizer needs to lower,
//! mode-force, fix-up, and leaf-rewrite a stage without the data node re-deriving
//! anything.

use prost::Message;

/// Per-stage metadata. See `proto/stage.proto`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageMeta {
    #[prost(int32, tag = "1")]
    pub stage_id: i32,
    #[prost(int32, repeated, tag = "2")]
    pub child_stage_ids: ::prost::alloc::vec::Vec<i32>,
    /// [`AggMode`] as i32.
    #[prost(enumeration = "AggMode", tag = "3")]
    pub agg_mode: i32,
    /// [`LeafKind`] as i32.
    #[prost(enumeration = "LeafKind", tag = "4")]
    pub leaf_kind: i32,
    /// `FilterTreeShape.ordinal()`.
    #[prost(int32, tag = "5")]
    pub tree_shape: i32,
    #[prost(bool, tag = "6")]
    pub requests_row_ids: bool,
    #[prost(message, repeated, tag = "7")]
    pub delegated: ::prost::alloc::vec::Vec<DelegatedExpr>,
    /// Calcite's declared input rowType per child edge — D6 assertion targets.
    #[prost(message, repeated, tag = "8")]
    pub declared_input_row_types: ::prost::alloc::vec::Vec<SerializedSchema>,
    /// Set iff `leaf_kind` references a late-materialization child (D10).
    #[prost(message, optional, tag = "9")]
    pub lm_output_row_type: ::core::option::Option<SerializedSchema>,
    /// Per-child partial-stage Substrait bytes (parallel to `child_stage_ids`),
    /// set when a child stage is legacy-format (Phase 2a reduce_proto): the
    /// finalizer lowers each child's partial Substrait coordinator-side via
    /// `derive_schema_from_partial_plan` to learn the child's ACTUAL physical
    /// output schema (D5) — the source of truth for the `StageReadExec` boundary,
    /// rather than Calcite's declared rowType. Empty bytes = no child substrait
    /// supplied (child finalized in-session, or non-agg gather).
    #[prost(bytes = "vec", repeated, tag = "10")]
    pub child_partial_substrait: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}

/// One delegated predicate payload (Lucene-owned QueryBuilder bytes).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelegatedExpr {
    #[prost(int32, tag = "1")]
    pub annotation_id: i32,
    #[prost(string, tag = "2")]
    pub backend_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: ::prost::alloc::vec::Vec<u8>,
}

/// Arrow IPC schema bytes (stream format: schema message + EOS).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SerializedSchema {
    #[prost(bytes = "vec", tag = "1")]
    pub ipc: ::prost::alloc::vec::Vec<u8>,
}

/// FFM request envelope for `finalize_query_plan`: all stages in one call so the
/// finalizer can order them child-first and thread child schemas / retained
/// Final halves into parents (df-proto spec §5, §4.1). One entry per stage.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizeRequest {
    #[prost(message, repeated, tag = "1")]
    pub stages: ::prost::alloc::vec::Vec<FinalizeStage>,
}

/// One stage in a [`FinalizeRequest`]: its whole-fragment Substrait bytes plus
/// its [`StageMeta`].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizeStage {
    #[prost(bytes = "vec", tag = "1")]
    pub substrait_bytes: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub meta: ::core::option::Option<StageMeta>,
}

/// FFM response envelope for `finalize_query_plan`: the finalized plan bytes per
/// stage. The natural cache value for the plan-cache workstream (D15).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizeResponse {
    #[prost(message, repeated, tag = "1")]
    pub plans: ::prost::alloc::vec::Vec<FinalizedStageProto>,
}

/// One finalized stage's shippable plan bytes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinalizedStageProto {
    #[prost(int32, tag = "1")]
    pub stage_id: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub plan_bytes: ::prost::alloc::vec::Vec<u8>,
}

/// Codec payload for [`crate::os_exec::OpenSearchShardScanExec`]. Encoded into
/// the `PhysicalExtensionNode` buffer by `OpenSearchExtensionCodec::try_encode`.
/// `projected_schema_ipc` carries the node's output schema as Arrow IPC bytes so
/// the leaf reconstructs without any external schema source.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardScanExecNode {
    #[prost(bytes = "vec", tag = "1")]
    pub filter_expr: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "2")]
    pub tree_shape: i32,
    #[prost(message, repeated, tag = "3")]
    pub delegated: ::prost::alloc::vec::Vec<DelegatedExpr>,
    #[prost(bool, tag = "4")]
    pub requests_row_ids: bool,
    #[prost(string, tag = "5")]
    pub binding_key: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "6")]
    pub projected_schema_ipc: ::prost::alloc::vec::Vec<u8>,
}

/// Codec payload for [`crate::os_exec::StageReadExec`].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageReadExecNode {
    #[prost(int32, tag = "1")]
    pub child_stage_id: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub schema_ipc: ::prost::alloc::vec::Vec<u8>,
}

/// Codec payload for [`crate::relabel_exec::RelabelExec`] — carries the target
/// (relabel) schema; the single child input is supplied by the proto framework.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelabelExecNode {
    #[prost(bytes = "vec", tag = "1")]
    pub target_schema_ipc: ::prost::alloc::vec::Vec<u8>,
}

/// Tagged envelope distinguishing which custom exec a `PhysicalExtensionNode`
/// buffer carries. The codec writes this so decode can dispatch by variant
/// rather than trial-parsing.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OsExecNode {
    #[prost(oneof = "os_exec_node::Node", tags = "1, 2, 3")]
    pub node: ::core::option::Option<os_exec_node::Node>,
}

pub mod os_exec_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "1")]
        ShardScan(super::ShardScanExecNode),
        #[prost(message, tag = "2")]
        StageRead(super::StageReadExecNode),
        #[prost(message, tag = "3")]
        Relabel(super::RelabelExecNode),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggMode {
    None = 0,
    Partial = 1,
    Final = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LeafKind {
    ShardScan = 0,
    StageInput = 1,
    Values = 2,
    LmOutput = 3,
}

impl StageMeta {
    /// Decode from protobuf bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, prost::DecodeError> {
        StageMeta::decode(buf)
    }

    /// Encode to protobuf bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    /// Typed accessor for the aggregate mode.
    pub fn agg_mode_enum(&self) -> AggMode {
        AggMode::try_from(self.agg_mode).unwrap_or(AggMode::None)
    }

    /// Typed accessor for the leaf kind.
    pub fn leaf_kind_enum(&self) -> LeafKind {
        LeafKind::try_from(self.leaf_kind).unwrap_or(LeafKind::StageInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_meta_round_trips() {
        let meta = StageMeta {
            stage_id: 7,
            child_stage_ids: vec![1, 2, 3],
            agg_mode: AggMode::Final as i32,
            leaf_kind: LeafKind::StageInput as i32,
            tree_shape: 2,
            requests_row_ids: true,
            delegated: vec![DelegatedExpr {
                annotation_id: 42,
                backend_id: "lucene".to_string(),
                payload: vec![0xde, 0xad, 0xbe, 0xef],
            }],
            declared_input_row_types: vec![SerializedSchema { ipc: vec![1, 2, 3] }],
            lm_output_row_type: Some(SerializedSchema { ipc: vec![9, 9] }),
            child_partial_substrait: vec![vec![0xaa, 0xbb], vec![], vec![0xcc]],
        };
        let bytes = meta.to_bytes();
        let decoded = StageMeta::from_bytes(&bytes).expect("decode");
        assert_eq!(meta, decoded);
        assert_eq!(decoded.agg_mode_enum(), AggMode::Final);
        assert_eq!(decoded.leaf_kind_enum(), LeafKind::StageInput);
    }

    #[test]
    fn defaults_decode_as_none_and_shard_scan() {
        let meta = StageMeta::default();
        assert_eq!(meta.agg_mode_enum(), AggMode::None);
        assert_eq!(meta.leaf_kind_enum(), LeafKind::ShardScan);
        // Empty message encodes to empty bytes and decodes back to default.
        let bytes = meta.to_bytes();
        assert!(bytes.is_empty());
        assert_eq!(StageMeta::from_bytes(&bytes).unwrap(), meta);
    }

    #[test]
    fn enum_values_match_proto() {
        // Wire numbers are part of the contract with the Java encoder.
        assert_eq!(AggMode::None as i32, 0);
        assert_eq!(AggMode::Partial as i32, 1);
        assert_eq!(AggMode::Final as i32, 2);
        assert_eq!(LeafKind::ShardScan as i32, 0);
        assert_eq!(LeafKind::StageInput as i32, 1);
        assert_eq!(LeafKind::Values as i32, 2);
        assert_eq!(LeafKind::LmOutput as i32, 3);
    }
}

