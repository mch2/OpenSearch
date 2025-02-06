use arrow_flight::{flight_service_client::FlightServiceClient, FlightDescriptor};
use bytes::Bytes;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::expr_fn::sum;
use futures::TryFutureExt;
use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, DataFrame};
use datafusion_table_providers::flight::{
    FlightDriver, FlightMetadata, FlightProperties, FlightTableFactory, FlightTable
};
use datafusion::logical_expr::test::function_stub::count;
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

pub async fn aggregate(
    ctx: SessionContext,
    ticket: Bytes,
    entry_point: String
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket,     entry_point).await?;
    df.aggregate(vec![col("")], vec![count(col("a"))])
     .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
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
// registers a single table with datafusion using DataFusion TableProviders.
// Uses a TicketedFlightDriver to register the table with the list of given tickets.
async fn register_table(ctx: &SessionContext, name: String, ticket: Bytes, entry_point: String) -> Result<()> {
    let driver: TicketedFlightDriver = TicketedFlightDriver { ticket };
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    let table: FlightTable = table_factory
        .open_table(entry_point, HashMap::new())
        .await
        .map_err(|e| DataFusionError::Execution(format!("Error creating table: {}", e)))?;
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
    // this doesn't work - we don't have schema data...
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