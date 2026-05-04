/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `timestampadd_calendar(unit, n, ts)` — add `n` calendar units (MONTH / QUARTER / YEAR) to `ts`.
//!
//! Rationale (UDF over decomposition): calendar months have variable length; adding
//! "one month" to Jan-31 lands on Feb-28/29, not Feb-31. `chrono::Months::checked_add`
//! implements the rule correctly. Operator decomposition via fixed-seconds would
//! silently land on the wrong day.
//!
//! Companion to `timestampdiff_calendar` — the Stream 2 SPI adapter forwards
//! calendar-unit TIMESTAMPADD calls to this UDF; fixed-unit adds still take the
//! adapter's epoch-arithmetic path.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Months, NaiveDateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TimestampAddCalendarUdf::new()));
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CalendarUnit {
    Month,
    Quarter,
    Year,
}

impl CalendarUnit {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "MONTH" | "M" => Some(Self::Month),
            "QUARTER" | "Q" => Some(Self::Quarter),
            "YEAR" | "Y" => Some(Self::Year),
            _ => None,
        }
    }
    fn months_per_unit(self) -> i64 {
        match self {
            Self::Month => 1,
            Self::Quarter => 3,
            Self::Year => 12,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimestampAddCalendarUdf {
    signature: Signature,
}

impl TimestampAddCalendarUdf {
    pub fn new() -> Self {
        // PPL emits n as any integer type (Int32/Int64), ts as Utf8/Date/Timestamp.
        // Exact signature rejected the common Int32 case. user_defined + coerce
        // normalizes to (Utf8 unit, Int64 n, Timestamp(Ms, None) ts).
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for TimestampAddCalendarUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for TimestampAddCalendarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "timestampadd_calendar"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!(
                "timestampadd_calendar expects 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "timestampadd_calendar",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Int64, CoerceMode::TimestampMs],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!(
                "timestampadd_calendar expects 3 arguments, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;
        let unit_arr = args.args[0].clone().into_array(n)?;
        let amount_arr = args.args[1].clone().into_array(n)?;
        let ts_arr = args.args[2].clone().into_array(n)?;

        let unit_arr = unit_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampadd_calendar: unit expected Utf8, got {:?}",
                    unit_arr.data_type()
                ))
            })?;
        let amount_arr = amount_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampadd_calendar: n expected Int64, got {:?}",
                    amount_arr.data_type()
                ))
            })?;
        let ts_arr = ts_arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampadd_calendar: ts expected TimestampMillisecond, got {:?}",
                    ts_arr.data_type()
                ))
            })?;

        let mut builder = TimestampMillisecondBuilder::with_capacity(n);
        for i in 0..n {
            if unit_arr.is_null(i) || amount_arr.is_null(i) || ts_arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let unit = CalendarUnit::parse(unit_arr.value(i)).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "timestampadd_calendar: unit must be MONTH/QUARTER/YEAR, got {:?}. \
                     Fixed-length units (SECOND..WEEK) are handled by the SPI adapter \
                     path — do not call this UDF with them.",
                    unit_arr.value(i)
                ))
            })?;
            let total_months = amount_arr.value(i).saturating_mul(unit.months_per_unit());
            let ts = timestamp_to_naive(ts_arr.value(i))?;
            let shifted = add_calendar_months(ts, total_months).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "timestampadd_calendar: month add overflow (n={}, unit={:?})",
                    amount_arr.value(i),
                    unit
                ))
            })?;
            builder.append_value(shifted.and_utc().timestamp_millis());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn timestamp_to_naive(ts_millis: i64) -> Result<NaiveDateTime> {
    Ok(DateTime::<Utc>::from_timestamp_millis(ts_millis)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "timestampadd_calendar: unrepresentable millis {}",
                ts_millis
            ))
        })?
        .naive_utc())
}

/// Add `total_months` calendar months to `ts`; negative = subtract. chrono's
/// `Months::new` only takes u32, so for negatives we use `checked_sub_months`.
fn add_calendar_months(ts: NaiveDateTime, total_months: i64) -> Option<NaiveDateTime> {
    if total_months >= 0 {
        ts.checked_add_months(Months::new(total_months.try_into().ok()?))
    } else {
        let abs: u32 = (-total_months).try_into().ok()?;
        ts.checked_sub_months(Months::new(abs))
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ndt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(h, mi, s).unwrap()
    }

    #[test]
    fn add_one_month_trivial() {
        let ts = ndt(2024, 1, 15, 12, 30, 0);
        assert_eq!(add_calendar_months(ts, 1).unwrap(), ndt(2024, 2, 15, 12, 30, 0));
    }

    #[test]
    fn add_month_to_jan_31_clamps_to_feb_end() {
        // Jan-31 + 1 month in chrono → Feb-29 (leap) / Feb-28 (non-leap). Matches MySQL.
        let ts = ndt(2024, 1, 31, 0, 0, 0);
        assert_eq!(add_calendar_months(ts, 1).unwrap(), ndt(2024, 2, 29, 0, 0, 0));
        let ts = ndt(2023, 1, 31, 0, 0, 0);
        assert_eq!(add_calendar_months(ts, 1).unwrap(), ndt(2023, 2, 28, 0, 0, 0));
    }

    #[test]
    fn subtract_months_via_negative() {
        let ts = ndt(2024, 3, 15, 0, 0, 0);
        assert_eq!(add_calendar_months(ts, -2).unwrap(), ndt(2024, 1, 15, 0, 0, 0));
    }

    #[test]
    fn add_year_wraps_month() {
        let ts = ndt(2024, 6, 15, 0, 0, 0);
        assert_eq!(add_calendar_months(ts, 12).unwrap(), ndt(2025, 6, 15, 0, 0, 0));
    }

    #[test]
    fn invoke_happy_path_quarter() {
        let udf = TimestampAddCalendarUdf::new();
        // 2024-01-15 + 2 quarters = 2024-07-15
        let units = StringArray::from(vec![Some("QUARTER")]);
        let amount = Int64Array::from(vec![Some(2_i64)]);
        // 2024-01-15 00:00:00 UTC = 1705276800000
        let ts = TimestampMillisecondArray::from(vec![Some(1_705_276_800_000_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(units)),
                ColumnarValue::Array(Arc::new(amount)),
                ColumnarValue::Array(Arc::new(ts)),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        // 2024-07-15 00:00:00 UTC = 1721001600000
        assert_eq!(arr.value(0), 1_721_001_600_000);
    }

    #[test]
    fn invoke_null_propagation() {
        let udf = TimestampAddCalendarUdf::new();
        let units = StringArray::from(vec![Some("MONTH"), None, Some("MONTH")]);
        let amount = Int64Array::from(vec![Some(1_i64), Some(1_i64), None]);
        let ts = TimestampMillisecondArray::from(vec![None, Some(0_i64), Some(0_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(units)),
                ColumnarValue::Array(Arc::new(amount)),
                ColumnarValue::Array(Arc::new(ts)),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(arr.is_null(0) && arr.is_null(1) && arr.is_null(2));
    }

    // Coercion: unit arrives as Utf8 (literal), n may be any integer/float type,
    // ts may be Utf8 / Date32 / Timestamp(any).
    #[test]
    fn coerce_types_normalizes_args() {
        let udf = TimestampAddCalendarUdf::new();
        let out = udf
            .coerce_types(&[DataType::Utf8, DataType::Int32, DataType::Utf8])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_non_numeric_n() {
        let udf = TimestampAddCalendarUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = TimestampAddCalendarUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Int64])
            .is_err());
    }

    #[test]
    fn invoke_rejects_fixed_unit() {
        let udf = TimestampAddCalendarUdf::new();
        let units = StringArray::from(vec![Some("HOUR")]);
        let amount = Int64Array::from(vec![Some(1_i64)]);
        let ts = TimestampMillisecondArray::from(vec![Some(0_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(units)),
                ColumnarValue::Array(Arc::new(amount)),
                ColumnarValue::Array(Arc::new(ts)),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        assert!(udf.invoke_with_args(args).is_err());
    }
}
