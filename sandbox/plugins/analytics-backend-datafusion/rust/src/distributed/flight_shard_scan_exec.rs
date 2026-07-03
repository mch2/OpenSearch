/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Model A leaf: a custom `ExecutionPlan` that IS a Rust Arrow Flight client.
//!
//! Unlike `ShardScanExec` (Model B — scans parquet locally on a data-node Rust Worker),
//! `FlightShardScanExec` runs on the COORDINATOR / an MPP worker and fetches a shard's rows by
//! `do_get`-ing the data node hosting that shard, directly over Arrow Flight (rust→java). The data
//! node exposes a plain-Flight shard endpoint whose ticket is a simple descriptor (NOT OpenSearch
//! transport framing) — see the Java `DistributedShardFlightProducer`. Because the coordinator
//! already knows shard→node from `DfShardRouting`, the leaf constructs the ticket locally and goes
//! straight to `do_get` (no `getFlightInfo` hop, no Java upcall).
//!
//! This keeps the data-node shard SCAN path unchanged and uses datafusion-distributed only for the
//! shuffle/reduce/MPP layer above the leaf. One variant per shard is produced by the TaskEstimator;
//! each carries its target node's Flight URL + the shard descriptor.

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::{FlightClient, Ticket};
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, exec_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;

use crate::distributed::shard_scan_exec::UNASSIGNED_SHARD;

/// The ticket payload a `FlightShardScanExec` sends to the data node's plain-Flight shard endpoint.
/// A simple prost message (NOT OpenSearch transport framing) the Java endpoint decodes directly.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardScanTicket {
    #[prost(int64, tag = "1")]
    pub query_id: i64,
    #[prost(string, tag = "2")]
    pub index_uuid: String,
    #[prost(int32, tag = "3")]
    pub shard_id: i32,
    /// The shard-local Substrait plan to execute (projection/filter/partial-agg pushdown). Empty =
    /// full scan of the leaf's columns.
    #[prost(bytes = "vec", tag = "4")]
    pub plan: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FlightShardScanExec {
    pub index_uuid: String,
    pub shard_id: i32,
    /// Flight URL of the data node hosting this shard (e.g. "http://host:9450"). Empty until the
    /// estimator binds a concrete shard+node variant.
    pub node_url: String,
    pub query_id: i64,
    pub plan_bytes: Vec<u8>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl FlightShardScanExec {
    pub fn new(index_uuid: String, shard_id: i32, node_url: String, query_id: i64, plan_bytes: Vec<u8>, schema: SchemaRef) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { index_uuid, shard_id, node_url, query_id, plan_bytes, schema, props }
    }

    /// Bind this placeholder to a concrete shard + its data-node Flight URL (per-task variant).
    pub fn with_target(&self, shard_id: i32, node_url: String) -> Self {
        Self::new(
            self.index_uuid.clone(),
            shard_id,
            node_url,
            self.query_id,
            self.plan_bytes.clone(),
            Arc::clone(&self.schema),
        )
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl DisplayAs for FlightShardScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FlightShardScanExec: shard_id={}, node={}", self.shard_id, self.node_url)
    }
}

impl ExecutionPlan for FlightShardScanExec {
    fn name(&self) -> &str {
        "FlightShardScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if self.shard_id == UNASSIGNED_SHARD || self.node_url.is_empty() {
            return exec_err!("FlightShardScanExec executed while unassigned (shard_id={}, node_url='{}')", self.shard_id, self.node_url);
        }

        let url = self.node_url.clone();
        let ticket = Ticket {
            ticket: Bytes::from(
                ShardScanTicket {
                    query_id: self.query_id,
                    index_uuid: self.index_uuid.clone(),
                    shard_id: self.shard_id,
                    plan: self.plan_bytes.clone(),
                }
                .encode_to_vec(),
            ),
        };
        let schema = Arc::clone(&self.schema);

        // do_get is async; produce a stream that lazily connects + fetches. We adapt the
        // FlightRecordBatchStream into a DataFusion RecordBatchStream. Errors map to DataFusionError.
        let fut = async move {
            let channel = tonic::transport::Channel::from_shared(url.clone())
                .map_err(|e| exec_datafusion_err!("invalid Flight URL '{url}': {e}"))?
                .connect()
                .await
                .map_err(|e| exec_datafusion_err!("Flight connect to '{url}' failed: {e}"))?;
            let mut client = FlightClient::new(channel);
            let stream = client
                .do_get(ticket)
                .await
                .map_err(|e| exec_datafusion_err!("Flight do_get to '{url}' failed: {e}"))?;
            Ok::<_, datafusion::common::DataFusionError>(stream)
        };

        // Flatten the connect-future into a batch stream: yield connect errors as the first item.
        let batch_stream = futures::stream::once(fut)
            .map(|res| res.map(|s| s.map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))))
            .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batch_stream)))
    }
}
