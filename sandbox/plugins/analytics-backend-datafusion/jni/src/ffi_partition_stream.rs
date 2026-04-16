/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `FfiPartitionStream` — a DataFusion `PartitionStream` backed by a tokio mpsc channel.
//!
//! Java (via JDK FFM) pushes `RecordBatch`es through the sender side; DataFusion
//! pulls them through the receiver side as part of a `StreamingTableExec` leaf.
//!
//! No JNI types appear in this file — the `Ffi` prefix signals "fed via Foreign
//! Function Interface"; the struct has no knowledge that the consumer is Java.
//!
//! ## Bounded channel (backpressure)
//!
//! The channel is bounded with capacity 2 (double-buffering). When the buffer
//! is full, the producer blocks in `blocking_send` until DataFusion's poll loop
//! consumes a batch. This provides natural backpressure — the Java transport
//! thread is held, which holds the `PendingExecutions` permit, which prevents
//! the next shard request from dispatching.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Bounded channel capacity. Double-buffering: one batch being processed by
/// DataFusion, one queued and ready. Producers block when both slots are full.
const CHANNEL_CAPACITY: usize = 2;

/// A `PartitionStream` fed by a bounded mpsc channel.
///
/// Constructed via [`FfiPartitionStream::new`], which returns the stream and
/// the corresponding sender. The sender is stored separately in the session
/// registry and exposed to Java through a handle.
pub struct FfiPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<mpsc::Receiver<Result<RecordBatch, DataFusionError>>>>>,
}

impl FfiPartitionStream {
    /// Create a new partition stream and its corresponding sender.
    ///
    /// The sender is used to push batches into the stream. Dropping the sender
    /// signals EOF — the receiver stream will yield `None` after draining.
    pub fn new(
        schema: SchemaRef,
    ) -> (Self, mpsc::Sender<Result<RecordBatch, DataFusionError>>) {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let stream = Self {
            schema,
            receiver: Arc::new(Mutex::new(Some(rx))),
        };
        (stream, tx)
    }
}

impl fmt::Debug for FfiPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FfiPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for FfiPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("execute() called more than once on FfiPartitionStream");
        let stream = ReceiverStream::new(receiver);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Int64Array;
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn test_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// Task 16.1: create, push 3 batches via sender, iterate stream → 3 batches.
    /// Producer and consumer run concurrently since the channel is bounded.
    #[tokio::test]
    async fn test_push_and_consume() {
        let schema = test_schema();
        let (stream, tx) = FfiPartitionStream::new(schema.clone());

        // Start consuming in a separate task (bounded channel requires concurrent drain)
        let ctx = Arc::new(TaskContext::default());
        let mut output = stream.execute(ctx);

        let producer_schema = schema.clone();
        let producer = tokio::spawn(async move {
            tx.send(Ok(test_batch(&producer_schema, &[1, 2]))).await.unwrap();
            tx.send(Ok(test_batch(&producer_schema, &[3, 4]))).await.unwrap();
            tx.send(Ok(test_batch(&producer_schema, &[5]))).await.unwrap();
            drop(tx); // signal EOF
        });

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(result) = output.next().await {
            let batch = result.unwrap();
            batch_count += 1;
            total_rows += batch.num_rows();
        }
        producer.await.unwrap();
        assert_eq!(batch_count, 3);
        assert_eq!(total_rows, 5);
    }

    /// Task 16.2: drop sender → stream yields None
    #[tokio::test]
    async fn test_close_signals_eof() {
        let schema = test_schema();
        let (stream, tx) = FfiPartitionStream::new(schema.clone());

        // Drop sender immediately — no batches pushed
        drop(tx);

        let ctx = Arc::new(TaskContext::default());
        let mut output = stream.execute(ctx);

        // Stream should yield None immediately
        assert!(output.next().await.is_none());
    }

    /// Task 16.3: schema at construction matches stream.schema()
    #[tokio::test]
    async fn test_schema_preserved() {
        let schema = test_schema();
        let (stream, _tx) = FfiPartitionStream::new(schema.clone());

        assert_eq!(stream.schema(), &schema);
    }

    /// Task 16.4: second execute() call panics
    #[tokio::test]
    #[should_panic(expected = "execute() called more than once")]
    async fn test_execute_once_only() {
        let schema = test_schema();
        let (stream, _tx) = FfiPartitionStream::new(schema.clone());

        let ctx = Arc::new(TaskContext::default());
        let _first = stream.execute(ctx.clone());
        let _second = stream.execute(ctx); // should panic
    }
}
