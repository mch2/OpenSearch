/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-local plan execution: parse Substrait, substitute stage inputs
//! with `StreamingTableExec` wrapping `FfiPartitionStream`, and execute.
//!
//! ## Substitution approach
//!
//! We register each `FfiPartitionStream` as a custom `TableProvider` on the
//! `SessionContext` before decoding the Substrait plan. When `datafusion-substrait`
//! resolves table references, it finds our provider and the physical planner
//! naturally produces a `StreamingTableExec`. This is simpler than a post-hoc
//! logical or physical plan rewrite and leverages DataFusion's built-in table
//! resolution.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionConfig;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::ffi_partition_stream::FfiPartitionStream;
use crate::session_registry;

/// Execute a coordinator-local Substrait plan.
///
/// 1. Build a fresh `SessionContext`.
/// 2. For each registered `FfiPartitionStream` in the session, register it as a
///    `StreamingTable` under the stage input ID name.
/// 3. Decode the Substrait plan — table references resolve to our streaming tables.
/// 4. Execute and return the output stream handle.
pub async fn execute_local_plan(
    session_handle: i64,
    substrait_bytes: &[u8],
) -> Result<i64, DataFusionError> {
    // Decode Substrait protobuf
    let substrait_plan = Plan::decode(substrait_bytes).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
    })?;

    // Build a SessionContext and register all partition streams as tables.
    // Honor ANALYTICS_DF_BATCH_SIZE env var (test-only) to force small batches
    // so multi-batch streaming through CrossRtStream/pushBatch/drain is exercised.
    let mut config = SessionConfig::new();
    if let Ok(s) = std::env::var("ANALYTICS_DF_BATCH_SIZE") {
        if let Ok(n) = s.parse::<usize>() {
            config.options_mut().execution.batch_size = n;
        }
    }
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // Collect partition streams from the session registry
    let streams: Vec<(String, Arc<FfiPartitionStream>)> =
        session_registry::with_session(session_handle, |session| {
            Ok(session
                .partition_streams
                .iter()
                .map(|(id, s)| (id.clone(), s.clone()))
                .collect())
        })?;

    // Register each FfiPartitionStream as a StreamingTable
    for (stage_input_id, partition_stream) in streams {
        let table = StreamingTable::try_new(
            partition_stream.schema().clone(),
            vec![partition_stream as Arc<dyn PartitionStream>],
        )?;
        ctx.register_table(&stage_input_id, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register streaming table '{}': {}",
                    stage_input_id, e
                ))
            })?;
    }

    // Decode Substrait → logical plan (table references resolve via ctx)
    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

    // Logical → physical → execute
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let output_stream =
        datafusion::physical_plan::execute_stream(physical_plan, ctx.task_ctx())?;

    // Wrap the output stream in CrossRtStream + RecordBatchStreamAdapter to match
    // the JNI bridge's expected pointer type. The JNI stream operations
    // (NativeBridge.streamGetSchema / streamNextBatch) cast the i64 handle to
    // `*mut RecordBatchStreamAdapter<CrossRtStream>` and dereference it. This
    // wrapping must produce that exact type so the JNI bridge can consume the
    // stream returned from the FFM execute path.
    //
    // The cpu_executor comes from the same RuntimeManager used by per-shard
    // queries — we share it so both paths offload CPU-bound DataFusion work to
    // the same DedicatedExecutor.
    let manager = crate::TOKIO_RUNTIME_MANAGER.get().ok_or_else(|| {
        DataFusionError::Execution(
            "Tokio runtime not initialized; call initTokioRuntimeManager first".to_string(),
        )
    })?;
    let cpu_executor = manager.cpu_executor();
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(output_stream, cpu_executor);
    let wrapped = RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream);

    // session_handle is intentionally unused after the partition-stream
    // collection above — the output stream is now an opaque heap pointer
    // owned by the caller (released via stream_close → Box::from_raw).
    let _ = session_handle;

    // Note: the boxed pointer is owned by the caller and must be released via
    // stream_close (existing JNI helper that does Box::from_raw). We do NOT
    // store the stream in session_registry.output_streams — that table is dead
    // code now and can be removed in a follow-up cleanup.
    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}
