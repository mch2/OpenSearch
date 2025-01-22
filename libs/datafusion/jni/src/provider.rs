use arrow_flight::{flight_service_client::FlightServiceClient, FlightDescriptor, FlightInfo};
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::common::JoinType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::prelude::Expr;
use futures::TryFutureExt;
use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, DataFrame};
use datafusion_table_providers::flight::{
    FlightDriver, FlightMetadata, FlightProperties, FlightTableFactory, FlightTable
};
use datafusion::logical_expr::test::function_stub::count;
use futures::future::try_join_all;
use tonic::async_trait;
use tonic::transport::Channel;
mod test;

// Returns a DataFrame that represents a query on a single index.
// Each ticket should correlate to a single shard so that this executes shard fan-out
pub async fn query(
    ctx: SessionContext,
    ticket: Bytes,
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket).await?;

    df.sort(vec![col("score").sort(false, true)])
        .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}

pub async fn read_aggs(
    ctx: SessionContext,
    ticket: Bytes,
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket).await?;
    df.filter(col("ord").is_not_null())?
    .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])
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

    let left_df = dataframe_for_index(&ctx, "left".to_owned(), left).await?.select( select_cols.clone())?;

    let alias = format!("right.{}", &join_field);
    let right_df = dataframe_for_index(&ctx, "right".to_owned(), right).await?.select(
        vec![col(r#""docId""#).alias("right_docId"), col(r#""shardId""#).alias("right_shardId"), col("instance_id").alias("right_instance_id")]
    )?;

    println!("LEFT FRAME:");
    // left_df.clone().show().await;
    println!("RIGHT FRAME:");
    // right_df.clone().show().await;

    // // select all the cols returned by right but alias the join field
    // let select_cols: Vec<Expr> = right_df.schema()
    // .fields()
    // .iter()
    // .map(|field| {
    //     if field.name() == &join_field {
    //         col(&join_field).alias("right.join_field")
    //     } else {
    //         col(field.name())
    //     }
    // })
    // .collect();

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
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), ticket).await?;
    df.aggregate(vec![col("")], vec![count(col("a"))])
     .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}


async fn dataframe_for_index(
    ctx: &SessionContext,
    prefix: String,
    ticket: Bytes
) -> Result<DataFrame> {
     let table_name = format!("{}-s", prefix);
     get_dataframe_for_tickets(ctx, table_name, ticket.clone()).await
}

// Return a single dataframe for an entire index.
// Each ticket in tickets represents a single shard.
// async fn dataframe_for_index(
//     ctx: &SessionContext,
//     prefix: String,
//     tickets: Vec<Bytes>,
// ) -> Result<DataFrame> {
//     println!("UNION");
//     let inner_futures = tickets
//         .into_iter()
//         .enumerate()
//         .map(|(j, bytes)| {
//             let table_name = format!("{}-s-{}", prefix, j);
//             get_dataframe_for_tickets(ctx, table_name, vec![bytes.clone()])
//         })
//         .collect::<Vec<_>>();

//     let frames = try_join_all(inner_futures)
//         .await
//         .map_err(|e| DataFusionError::Execution(format!("Failed to join futures: {}", e)))?;
//     frames[0].clone().show().await?;
//     union_df(frames)
// }

// // Union a list of DataFrames
// fn union_df(frames: Vec<DataFrame>) -> Result<DataFrame> {
//     Ok(frames
//         .into_iter()
//         .reduce(|acc, df| match acc.union(df) {
//             Ok(unioned_df) => unioned_df,
//             Err(e) => panic!("Failed to union DataFrames: {}", e),
//         })
//         .ok_or_else(|| DataFusionError::Execution("No frames to union".to_string()))?)
// }

// registers a single table from the list of given tickets, then reads it immediately returning a dataframe.
// intended to be used to register and get a df for a single shard.
async fn get_dataframe_for_tickets(
    ctx: &SessionContext,
    name: String,
    ticket: Bytes,
) -> Result<DataFrame> {
    println!("Register");
    register_table(ctx, name.clone(), ticket)
        .and_then(|_| ctx.table(&name))
        .await
}
// registers a single table with datafusion using DataFusion TableProviders.
// Uses a TicketedFlightDriver to register the table with the list of given tickets.
async fn register_table(ctx: &SessionContext, name: String, ticket: Bytes) -> Result<()> {
    println!("start");
    let driver: TicketedFlightDriver = TicketedFlightDriver { ticket };
    println!("1");
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    println!("2");
    let table: FlightTable = table_factory
        .open_table(format!("http://localhost:{}", "8815"), HashMap::new())
        .await
        .map_err(|e| DataFusionError::Execution(format!("Error creating table: {}", e)))?;
    println!("Registering table {}", name);
    println!("Registering table {:?}", table);
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
        println!("DRIVER: metadata called");
        println!("DRIVER: options: {:?}", _options);
        
        let mut client: FlightServiceClient<Channel> = FlightServiceClient::new(channel.clone());
        
        println!("DRIVER: Using ticket: {:?}", self.ticket);
        
        let descriptor = FlightDescriptor::new_cmd(self.ticket.clone());
        println!("DRIVER: Created descriptor: {:?}", descriptor);
        
        let request = tonic::Request::new(descriptor);
        println!("DRIVER: Sending get_flight_info request");
        
        match client.get_flight_info(request).await {
            Ok(info) => {
                println!("DRIVER: Received flight info response");
                let info = info.into_inner();
                println!("DRIVER: Flight info: {:?}", info);
                FlightMetadata::try_new(info, FlightProperties::default())
            }
            Err(status) => {
                println!("DRIVER: Error getting flight info: {:?}", status);
                Err(status.into())
            }
        }
    }
}
