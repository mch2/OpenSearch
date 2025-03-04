use arrow::array::RecordBatch;
use datafusion::{error::DataFusionError, functions_aggregate::{count::count, sum::sum}, prelude::{col, DataFrame, SessionContext}};

pub struct DataFusionAggregator {
    context: SessionContext,
    current_aggregation: Option<DataFrame>,
    term_column: String,
}

impl DataFusionAggregator {
    pub fn new(context: SessionContext, term_column: String) -> Self {
        DataFusionAggregator {
            context,
            current_aggregation: None,
            term_column,
        }
    }

    pub async fn push_batch(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        // agg and collect the new batch immediately, we need to pre-aggregate the incoming batch
        // so that union works, otherwise we have 2 cols union with 1.
        let aggregated = self
            .context
            .read_batch(batch)?
            .filter(col(&self.term_column).is_not_null())?
            .aggregate(
                vec![col(&self.term_column).alias("ord")],
                vec![count(col(&self.term_column)).alias("count")],
            )?
            .collect()
            .await?;

        // Convert collected batches back to DataFrame
        let incoming_df = self.context.read_batches(aggregated)?;

        // Merge with existing aggregation if we have one
        self.current_aggregation = match self.current_aggregation.take() {
            Some(existing) => {
                let merged = existing.union(incoming_df)?.collect().await?;
                Some(self.context.read_batches(merged)?)
            }
            None => Some(incoming_df),
        };

        Ok(())
    }

    pub fn get_results(&self) -> Option<DataFrame> {
        self.current_aggregation.as_ref().and_then(|df| {
            df.clone()
                .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])
                .unwrap()
                .sort(vec![col("ord").sort(false, false)])
                .ok()
        })
    }

    pub fn get_results_with_limit(&self, limit: i32) -> Option<DataFrame> {
        self.current_aggregation.as_ref().and_then(|df| {
            if limit > 0 {
                df.clone()
                    .limit(0, Some(limit as usize))
                    .and_then(|limited| limited.sort(vec![col("ord").sort(false, false)]))
                    .ok()
            } else {
                Some(df.clone())
            }
        })
    }

    pub fn take_results(&mut self) -> Option<DataFrame> {
        self.current_aggregation.take()
    }
}