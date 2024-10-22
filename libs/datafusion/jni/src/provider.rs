use std::io::{Cursor};
use std::{collections::HashMap, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow_flight::FlightData;
use datafusion::logical_expr::expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{col, lit, DataFrame};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use datafusion_table_providers::flight::{FlightDriver, FlightMetadata, FlightTableFactory};
use futures::{StreamExt, TryStreamExt};
use tonic::{async_trait, transport::Channel, Request};
use arrow_flight::{error::Result, flight_service_client::FlightServiceClient, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_flight::utils::flight_data_to_arrow_batch;
use bytes::Bytes;

pub async fn collect(ctx: SessionContext, tickets: Vec<Bytes>) -> datafusion::common::Result<DataFrame> {

    // register "tables" for all of the given tickets
    let driver: TicketedFlightDriver = TicketedFlightDriver {
        tickets
    };
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    let table = table_factory
        .open_table(
            format!("http://localhost:{}", "8815"),
            HashMap::new(),
        ).await?;

    ctx.register_table("theIndex",  Arc::new(table))?;  
    let plan = ctx.state().create_logical_plan("Select * FROM theIndex ORDER BY score desc").await?;
    let data = ctx.execute_logical_plan(plan).await?;
    // execute the query producing the final stream

    // query a single table on the fly, no registration
    // let data = ctx.read_table(Arc::new(table))?;

    // return the docId set
    Ok(data)
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
pub struct TicketedFlightDriver {
    tickets: Vec<Bytes>
}

#[async_trait]
impl FlightDriver for TicketedFlightDriver {
    // this doesn't work - we don't have schema data...
    async fn metadata(
        &self,
        _channel: Channel,
        _options: &HashMap<String, String>,
    ) -> Result<FlightMetadata> {


        let schema = Schema::new(vec![
            Field::new("docID", DataType::Int32, false),
            Field::new("score", DataType::Float32, false),
            Field::new("shardID", DataType::Utf8, false),
            Field::new("nodeID", DataType::Utf8, false)
          ]);

       let mut info: FlightInfo = FlightInfo::new();

       for entry in &self.tickets {
        println!("Processing ticket: {:?}", entry);
        let endpoint: FlightEndpoint = FlightEndpoint::new()
        .with_ticket(Ticket::new(entry.clone()))
        .with_location("http://localhost:8815");
        info = info.with_endpoint(endpoint);
       }
       let info = info.try_with_schema(&schema)
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