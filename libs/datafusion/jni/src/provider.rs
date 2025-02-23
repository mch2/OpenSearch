use arrow_flight::FlightEndpoint;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightDescriptor};
use bytes::Bytes;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::expr_fn::sum;
use futures::TryFutureExt;
use std::backtrace::Backtrace;
use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, DataFrame};
use datafusion_table_providers::flight::{
    FlightDriver, FlightMetadata, FlightProperties, FlightTableFactory, FlightTable
};
use tonic::async_trait;
use tonic::transport::Channel;
mod test;

// Returns a DataFrame that represents a query on a single index.
// Each ticket should correlate to a single shard so that this executes shard fan-out
pub async fn query(
    ctx: SessionContext,
    ticket: Bytes,
    entry_point: String
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket, entry_point).await?;

    df.sort(vec![col("score").sort(false, true)])
        .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}

pub async fn read_aggs(
    ctx: SessionContext,
    ticket: Bytes,
    entry_point: String,
    size: usize
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket, entry_point).await?;
    df.filter(col("ord").is_not_null())?
    .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])?
    .sort(vec![col("count").sort(false, true)])?  // Sort by count descending
    .limit(0, Some(size)) // Get top 500 results
}

pub async fn read_aggs_with_endpoints(
    ctx: SessionContext,
    endpoints: Vec<FlightEndpoint>,
    entry_point: String,
    size: usize,
    schema_bytes: Bytes
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index_with_endpoints(&ctx, "theIndex".to_owned(), endpoints, entry_point, schema_bytes).await?;
    df.filter(col("ord").is_not_null())?
    .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])?
    .sort(vec![col("count").sort(false, true)])?  // Sort by count descending
    .limit(0, Some(size)) // Get top results
}

async fn dataframe_for_index(
    ctx: &SessionContext,
    prefix: String,
    ticket: Bytes,
    entry_point: String
) -> Result<DataFrame> {
     let table_name = format!("{}-s", prefix);
     get_dataframe_for_tickets(ctx, table_name, ticket.clone(), entry_point.clone()).await
}

async fn dataframe_for_index_with_endpoints(
    ctx: &SessionContext,
    prefix: String,
    endpoints: Vec<FlightEndpoint>,
    entry_point: String,
    schema_bytes: Bytes
) -> Result<DataFrame> {
     let table_name = format!("{}-s", prefix);
     get_dataframe_for_endpoints(ctx, table_name, endpoints, entry_point, schema_bytes).await
}

// registers a single table from the list of given tickets, then reads it immediately returning a dataframe.
// intended to be used to register and get a df for a single shard.
async fn get_dataframe_for_tickets(
    ctx: &SessionContext,
    name: String,
    ticket: Bytes,
    entry_point: String
) -> Result<DataFrame> {
    register_table(ctx, name.clone(), ticket, entry_point.clone())
        .and_then(|_| ctx.table(&name))
        .await
}

async fn get_dataframe_for_endpoints(
    ctx: &SessionContext,
    name: String,
    endpoints: Vec<FlightEndpoint>,
    entry_point: String,
    schema_bytes: Bytes,
) -> Result<DataFrame> {
    register_table_with_endpoints(ctx, name.clone(), endpoints, entry_point, schema_bytes)
        .and_then(|_| ctx.table(&name))
        .await
}

// registers a single table with datafusion using DataFusion TableProviders.
// Uses a TicketedFlightDriver to register the table with the list of given tickets.
async fn register_table(ctx: &SessionContext, name: String, ticket: Bytes, entry_point: String) -> Result<()> {
    println!("Registring table!");
    let driver: TicketedFlightDriver = TicketedFlightDriver { ticket };
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    let table: FlightTable = table_factory
    .open_table(entry_point, HashMap::new())
    .await
    .map_err(|e| {
        let bt = Backtrace::capture();
        println!("Error backtrace:\n{:?}", bt);
        DataFusionError::Execution(format!("Error creating table: {}", e))
    })?;
    println!("Table created but not reg?");
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| DataFusionError::Execution(format!("Error registering table: {}", e)))?;
    Ok(())
}

// registers a single table with datafusion using LocalFlightDriver with provided endpoints
async fn register_table_with_endpoints(
    ctx: &SessionContext,
    name: String,
    endpoints: Vec<FlightEndpoint>,
    entry_point: String,
    schema_bytes: Bytes,
) -> Result<()> {
    println!("EP: {:?}", endpoints);
    let driver: LocalFlightDriver = LocalFlightDriver { endpoints, schema_bytes };
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    let table: FlightTable = table_factory
    .open_table(entry_point, HashMap::new())
    .await
    .map_err(|e| {
        let bt = Backtrace::capture();
        println!("Error backtrace:\n{:?}", bt);
        DataFusionError::Execution(format!("Error creating table: {}", e))
    })?;
    println!("Table created but not reg?");
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| DataFusionError::Execution(format!("Error registering table: {}", e)))?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct TicketedFlightDriver {
    ticket: Bytes,
}

#[async_trait]
impl FlightDriver for TicketedFlightDriver {
    async fn metadata(
        &self,
        channel: Channel,
        _options: &HashMap<String, String>,
    ) -> arrow_flight::error::Result<FlightMetadata> {
        let mut client: FlightServiceClient<Channel> = FlightServiceClient::new(channel.clone());
        let descriptor = FlightDescriptor::new_cmd(self.ticket.clone());
        let request = tonic::Request::new(descriptor);

        match client.get_flight_info(request).await {
            Ok(info) => {
                let info = info.into_inner();
                FlightMetadata::try_new(info, FlightProperties::default())
            }
            Err(status) => {
                Err(status.into())
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalFlightDriver {
    endpoints: Vec<FlightEndpoint>,
    schema_bytes: Bytes
}

#[async_trait]
impl FlightDriver for LocalFlightDriver {
    async fn metadata(
        &self,
        _channel: Channel,
        _options: &HashMap<String, String>,
    ) -> arrow_flight::error::Result<FlightMetadata> {
        // Create metadata directly from stored endpoints
        FlightMetadata::try_new(
            arrow_flight::FlightInfo {
                schema: self.schema_bytes.clone(), // Schema will be determined when actually reading the data
                flight_descriptor: None,
                endpoint: self.endpoints.clone(),
                total_records: -1,
                total_bytes: -1,
                app_metadata: Bytes::new(),
                ordered: false,
            },
            FlightProperties::default(),
        )
    }
}