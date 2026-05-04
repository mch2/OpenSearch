/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `last_day(ts)` — date of the last day of `ts`'s calendar month.
//!
//! Rationale (UDF over decomposition): operator decomposition (go to next month,
//! subtract one day via epoch-seconds) rounds through float/long arithmetic, is
//! awkward around December→January wraparound, and loses type identity (returns
//! timestamp, needs re-cast to date). chrono's `checked_add_months` +
//! `pred_opt` / `with_day0(0) on month+1` does it in two direct calls.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Months, NaiveDate, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::DataType;
#[cfg(test)]
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(LastDayUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LastDayUdf {
    signature: Signature,
}

impl LastDayUdf {
    pub fn new() -> Self {
        // PPL emits `last_day(<expr>)` where <expr> may be a Utf8 literal or a
        // Timestamp with any precision/tz. The old Signature::one_of variant
        // list rejected everything but the exact Timestamp(Ms)/Date32 pair.
        // With user_defined + coerce_types we normalize all accepted inputs to
        // Timestamp(Ms,None) and rely on invoke_with_args to handle that type.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for LastDayUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for LastDayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "last_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("last_day expects 1 argument, got {}", arg_types.len());
        }
        Ok(DataType::Date32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args("last_day", arg_types, &[CoerceMode::TimestampMs])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("last_day expects 1 argument, got {}", args.args.len());
        }
        let arr = args.args[0].clone().into_array(args.number_rows)?;
        let out = match arr.data_type() {
            DataType::Timestamp(_, _) => {
                let ts = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "last_day: expected TimestampMillisecond, got {:?}",
                            arr.data_type()
                        ))
                    })?;
                let mut b = Date32Builder::with_capacity(ts.len());
                for i in 0..ts.len() {
                    if ts.is_null(i) {
                        b.append_null();
                    } else {
                        let d = DateTime::<Utc>::from_timestamp_millis(ts.value(i))
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "last_day: unrepresentable ts millis {}",
                                    ts.value(i)
                                ))
                            })?
                            .date_naive();
                        b.append_value(naive_date_to_days(last_day_of_month(&d)?));
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }
            DataType::Date32 => {
                let d = arr
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "last_day: expected Date32, got {:?}",
                            arr.data_type()
                        ))
                    })?;
                let mut b = Date32Builder::with_capacity(d.len());
                for i in 0..d.len() {
                    if d.is_null(i) {
                        b.append_null();
                    } else {
                        let nd = days_to_naive_date(d.value(i))?;
                        b.append_value(naive_date_to_days(last_day_of_month(&nd)?));
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }
            other => {
                return plan_err!("last_day: unsupported field type {:?}", other);
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// Last day of `d`'s calendar month, via +1 month then subtract one day.
fn last_day_of_month(d: &NaiveDate) -> Result<NaiveDate> {
    // first of next month = first day of this month + 1 calendar month
    let first_this = NaiveDate::from_ymd_opt(d.year(), d.month(), 1).ok_or_else(|| {
        DataFusionError::Internal(format!("last_day: unrepresentable date for {:?}", d))
    })?;
    let first_next = first_this.checked_add_months(Months::new(1)).ok_or_else(|| {
        DataFusionError::Internal(format!("last_day: month overflow for {:?}", d))
    })?;
    first_next.pred_opt().ok_or_else(|| {
        DataFusionError::Internal(format!("last_day: pred underflow for {:?}", first_next))
    })
}

fn days_to_naive_date(days: i32) -> Result<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days + 719163).ok_or_else(|| {
        DataFusionError::Internal(format!("last_day: unrepresentable epoch day {}", days))
    })
}

fn naive_date_to_days(d: NaiveDate) -> i32 {
    d.num_days_from_ce() - 719163
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    #[test]
    fn last_day_31day_month() {
        // 2024-01-15 → 2024-01-31
        assert_eq!(last_day_of_month(&date(2024, 1, 15)).unwrap(), date(2024, 1, 31));
    }

    #[test]
    fn last_day_leap_february() {
        assert_eq!(last_day_of_month(&date(2024, 2, 10)).unwrap(), date(2024, 2, 29));
        assert_eq!(last_day_of_month(&date(2023, 2, 10)).unwrap(), date(2023, 2, 28));
    }

    #[test]
    fn last_day_december_wraps_year() {
        assert_eq!(last_day_of_month(&date(2024, 12, 7)).unwrap(), date(2024, 12, 31));
    }

    #[test]
    fn last_day_30day_month() {
        assert_eq!(last_day_of_month(&date(2024, 6, 1)).unwrap(), date(2024, 6, 30));
        assert_eq!(last_day_of_month(&date(2024, 4, 30)).unwrap(), date(2024, 4, 30));
    }

    #[test]
    fn invoke_date32_null_propagation() {
        let udf = LastDayUdf::new();
        // 2024-02-10 = 19763; 2024-02-29 = 19782.
        let feb10 = 19763_i32;
        let feb29 = 19782_i32;
        let arr = Date32Array::from(vec![Some(feb10), None]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            number_rows: 2,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Date32,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(arr.value(0), feb29);
        assert!(arr.is_null(1));
    }

    // Coercion: PPL may emit last_day with a Utf8 literal (`last_day('2024-02-15')`)
    // or a Timestamp of varying precision. coerce_types must normalize all
    // temporal/string inputs to Timestamp(Ms,None) — the Date32 branch at
    // invoke time remains callable when the coordinator already emits Date32.
    #[test]
    fn coerce_types_accepts_utf8() {
        let udf = LastDayUdf::new();
        let out = udf.coerce_types(&[DataType::Utf8]).unwrap();
        assert_eq!(out, vec![DataType::Timestamp(TimeUnit::Millisecond, None)]);
    }

    #[test]
    fn coerce_types_passes_date32_through_as_timestamp() {
        // Our shared coerce_slot maps Date32 → Timestamp(Ms,None) for the
        // TimestampMs mode. Invoke_with_args still handles Timestamp at runtime,
        // so the existing Date32 branch there becomes dead for planner-emitted
        // calls — but remains correct if ever invoked.
        let udf = LastDayUdf::new();
        let out = udf.coerce_types(&[DataType::Date32]).unwrap();
        assert_eq!(out, vec![DataType::Timestamp(TimeUnit::Millisecond, None)]);
    }

    #[test]
    fn coerce_types_rejects_non_temporal() {
        let udf = LastDayUdf::new();
        let err = udf.coerce_types(&[DataType::Int64]).unwrap_err();
        assert!(format!("{err}").contains("last_day"));
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = LastDayUdf::new();
        assert!(udf.coerce_types(&[]).is_err());
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }

    #[test]
    fn invoke_timestamp_returns_date() {
        let udf = LastDayUdf::new();
        // 2024-02-15 00:00:00 UTC = 1707955200000
        let arr = TimestampMillisecondArray::from(vec![Some(1_707_955_200_000_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Date32,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        // 2024-02-29 = 19782.
        assert_eq!(arr.value(0), 19782);
    }
}
