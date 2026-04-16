/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Integration test: full end-to-end coordinator-local execution.
//!
//! Creates a session, registers a partition stream with `{a: Int64}` schema,
//! pushes 3 batches of values in-process, closes the sender, then executes
//! a Substrait plan for `SELECT sum(a) FROM __input__` and verifies the output.
//!
//! This test exercises the C ABI functions directly from Rust (without going
//! through FFI), validating the full pipeline:
//!   session_registry → FfiPartitionStream → StreamingTable → DataFusion plan → output

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// Import the library crate
use opensearch_datafusion_jni::ffi_partition_stream::FfiPartitionStream;
use opensearch_datafusion_jni::session_registry;

/// Test the FfiPartitionStream + session registry flow end-to-end
/// (without Substrait, using direct DataFusion API).
#[tokio::test]
async fn test_partition_stream_end_to_end_via_registry() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

    // 1. Create session
    let session_handle = session_registry::create_session();
    assert!(session_handle > 0);

    // 2. Create partition stream
    let sender_handle = session_registry::create_partition_stream(
        session_handle,
        "__stage_0_input__",
        schema.clone(),
    )
    .unwrap();
    assert!(sender_handle > 0);

    // 3. Push 3 batches: [10, 20], [30], [40, 50]
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![30]))],
    )
    .unwrap();
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![40, 50]))],
    )
    .unwrap();

    session_registry::push_batch(sender_handle, batch1).unwrap();
    session_registry::push_batch(sender_handle, batch2).unwrap();
    session_registry::push_batch(sender_handle, batch3).unwrap();

    // 4. Close sender (EOF)
    session_registry::close_sender(sender_handle);

    // 5. Verify the partition stream received all batches by executing
    //    a DataFusion plan directly (bypassing Substrait for this test).
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::streaming::PartitionStream;

    // Get the partition stream from the session
    let streams: Vec<(String, Arc<FfiPartitionStream>)> =
        session_registry::with_session(session_handle, |session| {
            Ok(session
                .partition_streams
                .iter()
                .map(|(id, s)| (id.clone(), s.clone()))
                .collect())
        })
        .unwrap();

    assert_eq!(streams.len(), 1);
    let (id, partition_stream) = &streams[0];
    assert_eq!(id, "__stage_0_input__");

    // Register as a StreamingTable and run a SUM query
    let ctx = SessionContext::new();
    let table = StreamingTable::try_new(
        partition_stream.schema().clone(),
        vec![partition_stream.clone() as Arc<dyn PartitionStream>],
    )
    .unwrap();
    ctx.register_table("__stage_0_input__", Arc::new(table))
        .unwrap();

    let df = ctx.sql("SELECT sum(a) as total FROM __stage_0_input__").await.unwrap();
    let batches = df.collect().await.unwrap();

    // Verify: sum(10 + 20 + 30 + 40 + 50) = 150
    assert_eq!(batches.len(), 1);
    let result_batch = &batches[0];
    assert_eq!(result_batch.num_rows(), 1);

    let total_col = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total_col.value(0), 150);

    // 6. Cleanup
    session_registry::drop_session(session_handle);
}

/// Test multiple partition streams in a single session.
#[tokio::test]
async fn test_multiple_partition_streams() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

    let session_handle = session_registry::create_session();

    let sender0 = session_registry::create_partition_stream(
        session_handle,
        "__stage_0_input__",
        schema.clone(),
    )
    .unwrap();
    let sender1 = session_registry::create_partition_stream(
        session_handle,
        "__stage_1_input__",
        schema.clone(),
    )
    .unwrap();

    assert_ne!(sender0, sender1);

    // Push to each stream
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![100]))],
    )
    .unwrap();
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![200]))],
    )
    .unwrap();

    session_registry::push_batch(sender0, batch0).unwrap();
    session_registry::push_batch(sender1, batch1).unwrap();

    // Close both
    session_registry::close_sender(sender0);
    session_registry::close_sender(sender1);

    // Verify both streams are registered
    session_registry::with_session(session_handle, |session| {
        assert_eq!(session.partition_streams.len(), 2);
        assert!(session.partition_streams.contains_key("__stage_0_input__"));
        assert!(session.partition_streams.contains_key("__stage_1_input__"));
        Ok(())
    })
    .unwrap();

    session_registry::drop_session(session_handle);
}

/// Test that pushing to a closed sender returns an error.
#[tokio::test]
async fn test_push_after_close_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

    let session_handle = session_registry::create_session();
    let sender_handle = session_registry::create_partition_stream(
        session_handle,
        "__stage_0_input__",
        schema.clone(),
    )
    .unwrap();

    // Close the sender
    session_registry::close_sender(sender_handle);

    // Push should fail (sender removed from registry)
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let result = session_registry::push_batch(sender_handle, batch);
    assert!(result.is_err());

    session_registry::drop_session(session_handle);
}
