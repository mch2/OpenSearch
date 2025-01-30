use anyhow::Context;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightDescriptor};
use bytes::Bytes;
use datafusion::common::JoinType;
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
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket, "http://localhost:9400".to_owned()).await?;

    df.sort(vec![col("score").sort(false, true)])
        .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}

pub async fn read_aggs(
    ctx: SessionContext,
    ticket: Bytes,
    size: usize
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket, "http://localhost:9450".to_owned()).await?;
    // Ok(df)
    // df.clone().explain(true, true)?.collect().await?;
    df.filter(col("ord").is_not_null())?
    .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])?
    .sort(vec![col("count").sort(false, true)])?  // Sort by count descending
    .limit(0, Some(500)) // Get top 500 results
}

// inner join two tables together, returning a single DataFrame that can be consumed
// represents a join query against two indices
pub async fn join(
    ctx: SessionContext,
    left: Bytes,
    right: Bytes,
    join_field: String,
) -> datafusion::common::Result<DataFrame> {

    let select_cols = vec![col(r#""docId""#), col(r#""shardId""#), col(&join_field)];

    let left_df = dataframe_for_index(&ctx, "left".to_owned(), left, "http://localhost:9400".to_owned()).await?.select( select_cols.clone())?;

    let alias = format!("right.{}", &join_field);
    let right_df = dataframe_for_index(&ctx, "right".to_owned(), right, "http://localhost:9400".to_owned()).await?.select(
        vec![col(r#""docId""#).alias("right_docId"), col(r#""shardId""#).alias("right_shardId"), col("instance_id").alias("right_instance_id")]
    )?;

    println!("LEFT FRAME:");
    // left_df.clone().show().await;
    println!("RIGHT FRAME:");

    return left_df
        .join(
            right_df,
            JoinType::Inner,
            &[&join_field],
            &["right_instance_id"],
            None,
        )
        .map_err(|e| DataFusionError::Execution(format!("Join operation failed: {}", e)));

    // let df2 = frame.as_ref().unwrap().clone().show().await;
    // return frame;
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
    // println!("Registering table {:?}", table);
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| DataFusionError::Execution(format!("Error registering table: {}", e)))?;
    // let df = ctx.sql("SHOW TABLES").await?;
    // df.show().await?;
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
        // println!("DRIVER: options: {:?}", _options);
        
        let mut client: FlightServiceClient<Channel> = FlightServiceClient::new(channel.clone());
        
        // println!("DRIVER: Using ticket: {:?}", self.ticket);
        
        let descriptor = FlightDescriptor::new_cmd(self.ticket.clone());
        // println!("DRIVER: Created descriptor: {:?}", descriptor);
        
        let request = tonic::Request::new(descriptor);
        // println!("DRIVER: Sending get_flight_info request");
        
        match client.get_flight_info(request).await {
            Ok(info) => {
                // println!("DRIVER: Received flight info response");
                let info = info.into_inner();
                // println!("DRIVER: Flight info: {:?}", info);
                FlightMetadata::try_new(info, FlightProperties::default())
            }
            Err(status) => {
                // println!("DRIVER: Error getting flight info: {:?}", status);
                Err(status.into())
            }
        }
    }
}
