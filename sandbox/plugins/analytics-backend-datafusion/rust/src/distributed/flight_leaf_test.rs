/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Model A test: proves `FlightShardScanExec` (the Rust Arrow Flight client leaf) fetches a shard's
//! rows directly from a data-node Flight server over real TCP, decoding our `ShardScanTicket`.
//!
//! The server here is a MINIMAL Rust stand-in for the Java `DistributedShardFlightProducer`: it
//! decodes the ticket, and returns a deterministic batch keyed by shard_id (mimicking "scan shard
//! N"). This validates the entire Rust client path — ticket encode → do_get → FlightData decode →
//! RecordBatch — independent of the JVM. The Java endpoint must honor the same ticket contract.

#![cfg(all(test, feature = "spike_integration"))]

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, Streaming};

use crate::distributed::flight_shard_scan_exec::{FlightShardScanExec, ShardScanTicket};

fn out_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

/// Minimal Flight server mimicking the data-node plain-Flight shard endpoint: decode the
/// ShardScanTicket, return one batch of {shard_id*10, shard_id*10+1} so the test can verify the
/// fetched rows correspond to the requested shard.
#[derive(Clone)]
struct StubShardFlight;

#[tonic::async_trait]
impl FlightService for StubShardFlight {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let parsed = ShardScanTicket::decode(ticket.ticket).map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;
        let base = (parsed.shard_id as i64) * 10;
        let batch = RecordBatch::try_new(out_schema(), vec![Arc::new(Int64Array::from(vec![base, base + 1]))])
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(out_schema())
            .build(futures::stream::iter(vec![Ok(batch)]))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    // Unused verbs.
    async fn handshake(&self, _: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(&self, _: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_action(&self, _: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
    async fn do_exchange(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_leaf_fetches_shard_directly_over_tcp() -> Result<(), Box<dyn std::error::Error>> {
    // Stand up the stub data-node Flight server on a loopback port.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(StubShardFlight))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let url = format!("http://127.0.0.1:{port}");

    // The leaf for shard 3 fetches directly from the (stub) data node. shard 3 -> rows {30,31}.
    let leaf: Arc<dyn ExecutionPlan> = Arc::new(FlightShardScanExec::new(
        "idx-uuid".to_string(),
        3,
        url,
        4242,
        Vec::new(),
        out_schema(),
    ));

    let ctx = SessionContext::new();
    let batches = execute_stream(leaf, ctx.task_ctx())?.try_collect::<Vec<_>>().await?;
    let vals: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec())
        .collect();
    assert_eq!(vals, vec![30, 31], "Rust Flight leaf must fetch shard 3's rows directly from the data node");
    Ok(())
}
