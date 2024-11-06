use arrow_flight::{flight_service_client::FlightServiceClient, FlightDescriptor, FlightInfo};
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::common::JoinType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use futures::TryFutureExt;
use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, DataFrame};
use datafusion_table_providers::flight::{
    FlightDriver, FlightMetadata, FlightProperties, FlightTableFactory,
};
use datafusion::logical_expr::test::function_stub::count;
use futures::future::try_join_all;
use tonic::async_trait;
use tonic::transport::Channel;

// Returns a DataFrame that represents a query on a single index.
// Each ticket should correlate to a single shard so that this executes shard fan-out
pub async fn query(
    ctx: SessionContext,
    tickets: Vec<Bytes>,
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), tickets).await?;

    df.sort(vec![col("score").sort(false, true)])
        .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}

// inner join two tables together, returning a single DataFrame that can be consumed
// represents a join query against two indices
pub async fn join(
    ctx: SessionContext,
    left: Vec<Bytes>,
    right: Vec<Bytes>,
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
    tickets: Vec<Bytes>,
) -> datafusion::common::Result<DataFrame> {
    let df = dataframe_for_index(&ctx, "theIndex".to_owned(), tickets).await?;
    df.aggregate(vec![col("")], vec![count(col("a"))])
     .map_err(|e| DataFusionError::Execution(format!("Failed to sort DataFrame: {}", e)))
}

// Return a single dataframe for an entire index.
// Each ticket in tickets represents a single shard.
async fn dataframe_for_index(
    ctx: &SessionContext,
    prefix: String,
    tickets: Vec<Bytes>,
) -> Result<DataFrame> {
    let inner_futures = tickets
        .into_iter()
        .enumerate()
        .map(|(j, bytes)| {
            let table_name = format!("{}-s-{}", prefix, j);
            get_dataframe_for_tickets(ctx, table_name, vec![bytes.clone()])
        })
        .collect::<Vec<_>>();

    let frames = try_join_all(inner_futures)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to join futures: {}", e)))?;

    union_df(frames)
}

// Union a list of DataFrames
fn union_df(frames: Vec<DataFrame>) -> Result<DataFrame> {
    Ok(frames
        .into_iter()
        .reduce(|acc, df| match acc.union(df) {
            Ok(unioned_df) => unioned_df,
            Err(e) => panic!("Failed to union DataFrames: {}", e),
        })
        .ok_or_else(|| DataFusionError::Execution("No frames to union".to_string()))?)
}

// registers a single table from the list of given tickets, then reads it immediately returning a dataframe.
// intended to be used to register and get a df for a single shard.
async fn get_dataframe_for_tickets(
    ctx: &SessionContext,
    name: String,
    tickets: Vec<Bytes>,
) -> Result<DataFrame> {
    register_table(ctx, name.clone(), tickets)
        .and_then(|_| ctx.table(&name))
        .await
}
// registers a single table with datafusion using DataFusion TableProviders.
// Uses a TicketedFlightDriver to register the table with the list of given tickets.
async fn register_table(ctx: &SessionContext, name: String, tickets: Vec<Bytes>) -> Result<()> {
    let driver: TicketedFlightDriver = TicketedFlightDriver { tickets };
    let table_factory: FlightTableFactory = FlightTableFactory::new(Arc::new(driver));
    let table = table_factory
        .open_table(format!("http://localhost:{}", "8815"), HashMap::new())
        .await
        .map_err(|e| DataFusionError::Execution(format!("Error creating table: {}", e)))?;
    println!("Registering table {}", name);
    println!("Registering table {:?}", table);
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| DataFusionError::Execution(format!("Error registering table: {}", e)))?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct TicketedFlightDriver {
    tickets: Vec<Bytes>,
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

        // TODO: validate tickets vec on way in

        let cmd = FlightDescriptor::new_cmd(self.tickets.get(0).expect("No ticket found").clone());
        let response: std::result::Result<tonic::Response<FlightInfo>, tonic::Status> =
            client.get_flight_info(tonic::Request::new(cmd)).await;
        match response {
            Ok(info) => {
                println!("Received info: {:?}", info);
                let info: arrow_flight::FlightInfo = info.into_inner();
                return FlightMetadata::try_new(info, FlightProperties::default());
            }
            Err(status) => {
                println!("Error details: {:?}", status);
                return Err(status.into());
            }
        }
    }
}
