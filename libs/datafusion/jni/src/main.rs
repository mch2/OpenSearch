use std::io::Cursor;
use std::{collections::HashMap, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow_flight::FlightData;
use datafusion::logical_expr::expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{col, lit};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use datafusion_table_providers::flight::{FlightDriver, FlightMetadata, FlightTableFactory};
use futures::{StreamExt, TryStreamExt};
use tonic::{async_trait, transport::Channel, Request};
use arrow_flight::{error::Result, flight_service_client::FlightServiceClient, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_flight::utils::flight_data_to_arrow_batch;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let ctx: SessionContext = SessionContext::new();

    // let mut map: HashMap<&str, &str> = HashMap::new();
    // map.insert("s0", "8081");
    // map.insert("s1", "8082");

    // for key in map.keys() {
    //     if let Some(value) = map.get(key) {
    //         println!("Registering Table: {}, at port: {}", key, value);
    //         let table_factory = FlightTableFactory::new(Arc::new(OpenSearchFlightDriver::default()));
    //         let table = table_factory
    //             .open_table(
    //                 format!("http://localhost:{}", value),
    //                 HashMap::new(),
    //             )
    //             .await?;
    //         ctx.register_table(key.to_string(), Arc::new(table))?;
    //     }
    // }

    // // fan out
    // ctx.sql("select * from s0 UNION select * from s1 ORDER BY score DESC LIMIT 20").await?.show().await?;


    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(TicketedFlightDriver::default()));
    let table = table_factory
        .open_table(
            format!("http://localhost:{}", "8815"),
            HashMap::new(),
        )
        .await?;
    // ctx.register_table("shards", Arc::new(table))?;

    let data = ctx.read_table(Arc::new(table))?;

    
    println!("{:?}", data.schema().columns());
    data.clone().show().await?;

    // data.clone().filter(col(r#""docID""#).lt_eq(lit(1)))?.show().await?;
    // data.clone().show().await?;

    // let stream = data.execute_stream().await?;

    // // print batches
    // let batches: Vec<RecordBatch> = stream.try_collect().await?;
    // println!("{:?}", batches);
    // Ok(())
    // ctx.sql("select * from shards").await?.show().await?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct OpenSearchFlightDriver {}

#[async_trait]
impl FlightDriver for OpenSearchFlightDriver {
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> Result<FlightMetadata> {
        let mut client: FlightServiceClient<Channel> = FlightServiceClient::new(channel);

        // let descriptor = FlightDescriptor::new_cmd(options["flight.command"].clone());

        // let descriptor: FlightDescriptor = FlightDescriptor::new_path(vec!["get_table_data".to_string()]);
        let cmd = FlightDescriptor::new_cmd("wtf".as_bytes());
    
        let response: std::result::Result<tonic::Response<FlightInfo>, tonic::Status> = client.get_flight_info(tonic::Request::new(cmd)).await;
        match response {
            Ok(info) => {
                println!("Received info: {:?}", info);      
                let info: arrow_flight::FlightInfo = info.into_inner();
                FlightMetadata::try_new(info, HashMap::default())
            }
            Err(status) => {
                println!("Error details: {:?}", status);
                return Err(status.into());
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TicketedFlightDriver {}

#[async_trait]
impl FlightDriver for TicketedFlightDriver {
    // this doesn't work - we don't have schema data...
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> Result<FlightMetadata> {


       let schema = Schema::new(vec![
         Field::new("docID", DataType::Int32, false),
         Field::new("score", DataType::Int32, false),
         Field::new("shardID", DataType::Utf8, false),
         Field::new("nodeID", DataType::Utf8, false)
       ]);

       let endpoint: FlightEndpoint = FlightEndpoint::new()
       .with_ticket(Ticket::new("36c94e1f-9b41-429a-8e9b-a89962610834"))
       .with_location("http://localhost:8815");

       let endpoint2: FlightEndpoint = FlightEndpoint::new()
       .with_ticket(Ticket::new("ce1b4e1b-5cf3-484a-afdd-076452b56be5"))
       .with_location("http://localhost:8815");
               
       let info = FlightInfo::new()
           .with_endpoint(endpoint)
           .with_endpoint(endpoint2)
           .try_with_schema(&schema)
           .expect("encoding failed");
      return FlightMetadata::try_new(info, HashMap::default());

      // this shit doesn't work bc the stream gets consumed.. if we read the schema here we have to pass the stream down
      // to the table impl so it can be read only once.

        // // with this driver, we are supplied the ticket Id already, so we'll construct the
        // // FlightInfo manually... might be better to just have the server return the entire info and push the query here?
        // let mut client: FlightServiceClient<Channel> = FlightServiceClient::new(channel);
        // let ticket: Ticket = Ticket::new("77e745bd-af92-42cf-b30e-2247b8bbf71a");
        // let response = client.do_get(ticket).await;

        // match response {
        //     Ok(s) => {
        //         let mut stream = s.into_inner();
        //         let flight_data = stream.message().await?.unwrap();
        //         // convert FlightData to a stream
        //         let schema = Arc::new(Schema::try_from(&flight_data)?);
        //         println!("Schema: {schema:?}");
        //         let endpoint = FlightEndpoint::new()
        //         .with_ticket(Ticket::new("77e745bd-af92-42cf-b30e-2247b8bbf71a"))
        //         .with_location("http://localhost:8815");
                        
        //         let info = FlightInfo::new()
        //             .with_endpoint(endpoint)
        //             .try_with_schema(&schema)
        //             .expect("encoding failed");
        //        return FlightMetadata::try_new(info, HashMap::default());
        //     }
        //     Err(status) => {
        //         println!("Error details: {:?}", status);
        //         return Err(status.into());
        //     }
        // }
    
    }

    
}