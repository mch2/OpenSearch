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
//! ## Unbounded channel (MVP limitation)
//!
//! The channel is unbounded (`tokio::sync::mpsc::unbounded_channel`). Fast producers
//! can outrun the consumer and grow coordinator memory without bound. Bounded
//! backpressure is future work.

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
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A `PartitionStream` fed by an mpsc channel.
///
/// Constructed via [`FfiPartitionStream::new`], which returns the stream and
/// the corresponding sender. The sender is stored separately in the session
/// registry and exposed to Java through a handle.
pub struct FfiPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<mpsc::UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>,
}

impl FfiPartitionStream {
    /// Create a new partition stream and its corresponding sender.
    ///
    /// The sender is used to push batches into the stream. Dropping the sender
    /// signals EOF — the receiver stream will yield `None` after draining.
    pub fn new(
        schema: SchemaRef,
    ) -> (Self, mpsc::UnboundedSender<Result<RecordBatch, DataFusionError>>) {
        let (tx, rx) = mpsc::unbounded_channel();
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
        let stream = UnboundedReceiverStream::new(receiver);
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

    /// Task 16.1: create, push 3 batches via sender, iterate stream → 3 batches
    #[tokio::test]
    async fn test_push_and_consume() {
        let schema = test_schema();
        let (stream, tx) = FfiPartitionStream::new(schema.clone());

        // Push 3 batches
        tx.send(Ok(test_batch(&schema, &[1, 2]))).unwrap();
        tx.send(Ok(test_batch(&schema, &[3, 4]))).unwrap();
        tx.send(Ok(test_batch(&schema, &[5]))).unwrap();
        drop(tx); // signal EOF

        let ctx = Arc::new(TaskContext::default());
        let mut output = stream.execute(ctx);

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(result) = output.next().await {
            let batch = result.unwrap();
            batch_count += 1;
            total_rows += batch.num_rows();
        }
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
