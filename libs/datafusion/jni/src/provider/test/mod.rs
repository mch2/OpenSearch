use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use arrow::ipc::convert::IpcSchemaEncoder;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::util::pretty::print_batches;
use datafusion::functions_aggregate::count::count;

use crate::provider;
use arrow::array::{DictionaryArray, Int32Array, StringArray, StructArray};
use arrow::datatypes::Int32Type;
use arrow::ipc;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Location, PollInfo, PutResult, SchemaResult, Ticket
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Array, Float32Array, Int64Array, Int8Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{col, DataFrame, SessionConfig, SessionContext};
use datafusion_table_providers::flight::sql::FlightSqlDriver;
use datafusion_table_providers::flight::{FlightProperties, FlightTableFactory};
use futures::{stream, Stream, TryStreamExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::http::HeaderMap;
use tonic::codegen::tokio_stream;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Server};
use tonic::{Extensions, Request, Response, Status, Streaming};
use datafusion::logical_expr::cast;

struct TestFlightService {
    flight_info: FlightInfo,
    partition_data: RecordBatch,
    shutdown_sender: Option<Sender<()>>,
}

impl TestFlightService {
    async fn run_in_background(self, rx: Receiver<()>) -> SocketAddr {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8815));
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = FlightServiceServer::new(self);
        #[allow(clippy::disallowed_methods)] // spawn allowed only in tests
        tokio::spawn(async move {
            Server::builder()
                .timeout(Duration::from_secs(1))
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    rx.await.ok();
                })
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        addr
    }
}

impl Drop for TestFlightService {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_sender.take() {
            tx.send(()).ok();
        }
    }
}

#[async_trait]
impl FlightService for TestFlightService {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("TEST SERVER: get_flight_info called");
        println!("TEST SERVER: descriptor: {:?}", request.get_ref());

        // Accept any ticket and return the flight info
        Ok(Response::new(self.flight_info.clone()))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("TEST SERVER: do_get called");
        println!("TEST SERVER: ticket: {:?}", request.get_ref());

        let data = self.partition_data.clone();
        let rb = async move { Ok(data) };
        let stream = FlightDataEncoderBuilder::default()
            .with_schema(self.partition_data.schema())
            .build(stream::once(rb))
            .map_err(|e| Status::internal(e.to_string()));

        Ok(Response::new(Box::pin(stream)))
    }

    // Implement other required methods with default behaviors
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        println!("TEST SERVER: get_schema called");
        println!("TEST SERVER: descriptor: {:?}", request.get_ref());
        // let schema = self.partition_data.schema();
        // let schema_bytes = ipc::writer::serialize_schema(schema.as_ref())
        //     .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;
        Ok(Response::new(SchemaResult {
            schema: self.flight_info.schema.clone(),
        }))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        println!("TEST SERVER: poll_flight_info called");
        println!("TEST SERVER: descriptor: {:?}", request.get_ref());

        Ok(Response::new(PollInfo {
            info: Some(__self.flight_info.clone()),
            expiration_time: std::option::Option::None,
            flight_descriptor: todo!(),
            progress: todo!(),
        }))
    }
}

#[tokio::test]
async fn test_read_aggs() -> datafusion::common::Result<()> {
    let schema = Schema::new([
        Arc::new(Field::new("ord", DataType::Utf8, false)),
        Arc::new(Field::new("count", DataType::Int64, false)),
    ]);
    
    let partition_data: RecordBatch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    // Simple schema encoding
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    let schema_bytes = &fb.finished_data();
        // Add IPC message framing
        let mut message = Vec::new();
        message.extend_from_slice(&(schema_bytes.len() as i32).to_le_bytes());
        message.extend_from_slice(schema_bytes);
    
    // Set up flight endpoint
    let endpoint = FlightEndpoint::default().with_ticket(Ticket::new("bytes".as_bytes()));
    let flight_info = FlightInfo::default()
        .try_with_schema(partition_data.schema().as_ref())?
        .with_endpoint(endpoint);

    // Set up test service
    let (tx, rx) = channel();
    let service = TestFlightService {
        flight_info,
        partition_data,
        shutdown_sender: Some(tx),
    };

    let port = service.run_in_background(rx).await.port();
    println!("TEST: Server started on port {}", port);
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", port))
        .unwrap()
        .connect()
        .await;

    println!("TEST: Channel connection result: {:?}", channel);
    // Set up session context
    let config = SessionConfig::new().with_batch_size(1);
    let ctx = SessionContext::new_with_config(config);
    // let props_template = FlightProperties::new().with_reusable_flight_info(true);
    // let driver = FlightSqlDriver::new().with_properties_template(props_template);
    // ctx.state_ref().write().table_factories_mut().insert(
    //     "FLIGHT_SQL".into(),
    //     Arc::new(FlightTableFactory::new(Arc::new(driver))),
    // );

    // Create test bytes for ticket
    let test_bytes = Bytes::from("test_ticket");

    let endpoint = FlightEndpoint {
        ticket: Some(Ticket { ticket: Bytes::from(test_bytes) }),
        location: vec![Location { uri: "http://localhost:8815".to_string() }],
        app_metadata: Bytes::new(),
        expiration_time: None,
    };
    // let schema_bytes = schema_bytes;

    // Execute read_aggs
    let result_df = provider::read_aggs_with_endpoints(
        ctx.clone(),
        vec![endpoint],
        "http://localhost:8815".to_owned(),
        500,
        Bytes::from(schema_bytes.clone())
    )
    .await
    .map_err(|e| {
        println!("TEST: Error in read_aggs: {:?}", e);
        e
    })?;

    let result = result_df.execute_stream().await?;
    let mut stream = result;
    while let Some(batch) = stream.try_next().await? {
        println!("\nRecord batch:");
        println!("Number of rows: {}", batch.num_rows());
        println!("Number of columns: {}", batch.num_columns());

        let ord_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray for ord column");

        println!("ord_array contents:");
        for i in 0..ord_array.len() {
            println!("  index {}: {:?}", i, ord_array.value(i));
        }

        // let count_array = batch
        //     .column(1)
        //     .downcast_ref::<Int64Array>()
        //     .expect("Expected Int64Array for count column");

        // println!("count_array contents:");
        // for i in 0..count_array.len() {
        //     println!("  index {}: {:?}", i, count_array.value(i));
        // }
    }
    Ok(())
}

#[tokio::test]
async fn test_dict_encoding() -> datafusion::common::Result<()> {
    let values = StringArray::from(vec!["a", "b", "c"]);
    let keys = Int32Array::from(vec![0, 1, 2, 0, 1, 2, 1, 2, 2]);
    let dict_array: DictionaryArray<Int32Type> =
        DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

    // Create a record batch with dictionary-encoded column
    let schema = Schema::new(vec![Field::new(
        "dict_col",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict_array)]).unwrap();

    println!("{}", batch.schema());
    let ctx = SessionContext::new();
//     let df = ctx.read_batch(batch)?
//     .aggregate(vec![col("dict_col").alias("ord")], vec![count(col("dict_col")).alias("count")]);
// df.unwrap().show().await?;
// let df = ctx.read_batch(batch)?
//     .with_column(
//         "decoded_col",
//         cast(col("dict_col"), DataType::Utf8)
//     )?;
//     df.show().await;
let result = ctx.read_batch(batch)?.collect().await?;
for batch in &result {
    if let Some(dict_array) = batch.column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>() 
    {
        let values = dict_array.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
            
            for i in 0..dict_array.len() {
                if let Some(key) = dict_array.key(i) {
                    println!("Row {}: key {} -> value {:?}", 
                        i, 
                        key, 
                        values.value(key as usize)
                    );
                } else {
                    println!("Row {}: NULL", i);
                }
            }
    }
}
    Ok(())
}
