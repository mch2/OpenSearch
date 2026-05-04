/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `timestampdiff_calendar(unit, a, b)` — calendar-unit diff (MONTH / QUARTER / YEAR).
//!
//! Rationale (UDF over decomposition): months are variable-length (28–31 days),
//! so `(b-a) / unit_seconds` has no fixed divisor. SQL semantics require
//! counting whole calendar months crossed, which needs `chrono`'s
//! calendar-aware operations. The Stream 2 SPI adapter handles fixed-length
//! units (SECOND..WEEK) via epoch arithmetic and rewrites the calendar-unit
//! RexCall to target this UDF.
//!
//! Result = whole calendar units between `a` and `b`, counted as "b - a". Matches
//! MySQL semantics: returns an integer, rounded toward zero.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Builder, StringArray, TimestampMillisecondArray,
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
    ctx.register_udf(ScalarUDF::from(TimestampDiffCalendarUdf::new()));
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CalendarUnit {
    Month,
    Quarter,
    Year,
}

impl CalendarUnit {
    /// Accept both long and short forms — the sql plugin upper-cases but
    /// tolerate case-sensitive variants too.
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "MONTH" | "M" => Some(Self::Month),
            "QUARTER" | "Q" => Some(Self::Quarter),
            "YEAR" | "Y" => Some(Self::Year),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimestampDiffCalendarUdf {
    signature: Signature,
}

impl TimestampDiffCalendarUdf {
    pub fn new() -> Self {
        // PPL emits either datetime arg as Utf8/Date/Timestamp of varying
        // precision. user_defined + coerce normalizes both to
        // Timestamp(Ms, None) before invoke_with_args runs.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for TimestampDiffCalendarUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for TimestampDiffCalendarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "timestampdiff_calendar"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!(
                "timestampdiff_calendar expects 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "timestampdiff_calendar",
            arg_types,
            &[
                CoerceMode::Utf8,
                CoerceMode::TimestampMs,
                CoerceMode::TimestampMs,
            ],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!(
                "timestampdiff_calendar expects 3 arguments, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;
        let unit_arr = args.args[0].clone().into_array(n)?;
        let a_arr = args.args[1].clone().into_array(n)?;
        let b_arr = args.args[2].clone().into_array(n)?;

        let unit_arr = unit_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampdiff_calendar: unit expected Utf8, got {:?}",
                    unit_arr.data_type()
                ))
            })?;
        let a_arr = a_arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampdiff_calendar: a expected TimestampMillisecond, got {:?}",
                    a_arr.data_type()
                ))
            })?;
        let b_arr = b_arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "timestampdiff_calendar: b expected TimestampMillisecond, got {:?}",
                    b_arr.data_type()
                ))
            })?;

        let mut builder = Int64Builder::with_capacity(n);
        for i in 0..n {
            if unit_arr.is_null(i) || a_arr.is_null(i) || b_arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let unit = CalendarUnit::parse(unit_arr.value(i)).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "timestampdiff_calendar: unit must be MONTH/QUARTER/YEAR, got {:?}. \
                     Fixed-length units (SECOND..WEEK) are handled by the SPI adapter \
                     path — do not call this UDF with them.",
                    unit_arr.value(i)
                ))
            })?;
            let a = timestamp_to_naive(a_arr.value(i))?;
            let b = timestamp_to_naive(b_arr.value(i))?;
            builder.append_value(diff_in_unit(&a, &b, unit));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn timestamp_to_naive(ts_millis: i64) -> Result<NaiveDateTime> {
    Ok(DateTime::<Utc>::from_timestamp_millis(ts_millis)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "timestampdiff_calendar: unrepresentable millis {}",
                ts_millis
            ))
        })?
        .naive_utc())
}

/// Whole-units difference `b - a`, floor-toward-zero, using calendar
/// arithmetic. Matches MySQL `TIMESTAMPDIFF`:
/// * MONTH: count of whole months crossed (same-day-of-month threshold).
/// * YEAR: count of whole years crossed (same anniversary threshold).
/// * QUARTER: whole months divided by 3 (truncating toward zero).
fn diff_in_unit(a: &NaiveDateTime, b: &NaiveDateTime, unit: CalendarUnit) -> i64 {
    let whole_months = whole_months_between(a, b);
    match unit {
        CalendarUnit::Month => whole_months,
        // Quarter: whole months divided by 3, truncated toward zero.
        CalendarUnit::Quarter => whole_months / 3,
        // Year: same algorithm but with a year granularity — don't just
        // divide months by 12, because Feb-28 → Feb-28 next year is a full year
        // and the month-count algorithm already accounts for that.
        CalendarUnit::Year => whole_years_between(a, b),
    }
}

/// Number of whole calendar months `b` is after `a`. Negative if `b < a`.
/// Matches MySQL: counts months only once `b`'s day+time ≥ `a`'s day+time.
fn whole_months_between(a: &NaiveDateTime, b: &NaiveDateTime) -> i64 {
    let (y1, m1, d1) = (a.year() as i64, a.month() as i64, a.day() as i64);
    let (y2, m2, d2) = (b.year() as i64, b.month() as i64, b.day() as i64);
    let mut months = (y2 - y1) * 12 + (m2 - m1);
    // If b hasn't yet reached a's day-of-month (with ties broken by time),
    // the final month isn't complete.
    let day_cmp = d2.cmp(&d1).then_with(|| a_time(a).cmp(&a_time(b)).reverse());
    if months > 0 && day_cmp == std::cmp::Ordering::Less {
        months -= 1;
    } else if months < 0 && day_cmp == std::cmp::Ordering::Greater {
        months += 1;
    }
    months
}

fn whole_years_between(a: &NaiveDateTime, b: &NaiveDateTime) -> i64 {
    let mut years = (b.year() - a.year()) as i64;
    // Same anniversary threshold: b has to have reached a's (month, day, time).
    let month_day_a = (a.month(), a.day(), a_time(a));
    let month_day_b = (b.month(), b.day(), a_time(b));
    if years > 0 && month_day_b < month_day_a {
        years -= 1;
    } else if years < 0 && month_day_b > month_day_a {
        years += 1;
    }
    years
}

/// Pack time-of-day to an orderable integer (nanos-of-day) for tie-breaking.
fn a_time(t: &NaiveDateTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
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
    fn parse_unit_accepts_full_and_short_names() {
        assert_eq!(CalendarUnit::parse("MONTH"), Some(CalendarUnit::Month));
        assert_eq!(CalendarUnit::parse("month"), Some(CalendarUnit::Month));
        assert_eq!(CalendarUnit::parse("QUARTER"), Some(CalendarUnit::Quarter));
        assert_eq!(CalendarUnit::parse("YEAR"), Some(CalendarUnit::Year));
        assert_eq!(CalendarUnit::parse("y"), Some(CalendarUnit::Year));
        assert_eq!(CalendarUnit::parse("SECOND"), None);
        assert_eq!(CalendarUnit::parse(""), None);
    }

    #[test]
    fn diff_months_basic() {
        let a = ndt(2024, 1, 15, 12, 0, 0);
        let b = ndt(2024, 3, 15, 12, 0, 0);
        assert_eq!(diff_in_unit(&a, &b, CalendarUnit::Month), 2);
        assert_eq!(diff_in_unit(&b, &a, CalendarUnit::Month), -2);
    }

    #[test]
    fn diff_months_threshold_not_reached() {
        // Jan 15 → Feb 10: less than one full month. MySQL returns 0.
        let a = ndt(2024, 1, 15, 0, 0, 0);
        let b = ndt(2024, 2, 10, 0, 0, 0);
        assert_eq!(diff_in_unit(&a, &b, CalendarUnit::Month), 0);
    }

    #[test]
    fn diff_months_threshold_exact() {
        let a = ndt(2024, 1, 15, 12, 0, 0);
        let b = ndt(2024, 2, 15, 12, 0, 0);
        assert_eq!(diff_in_unit(&a, &b, CalendarUnit::Month), 1);
        // Same day but earlier time → threshold not yet reached.
        let b2 = ndt(2024, 2, 15, 11, 59, 59);
        assert_eq!(diff_in_unit(&a, &b2, CalendarUnit::Month), 0);
    }

    #[test]
    fn diff_years() {
        // Same anniversary → 1 year
        let a = ndt(2023, 5, 15, 12, 0, 0);
        let b = ndt(2024, 5, 15, 12, 0, 0);
        assert_eq!(diff_in_unit(&a, &b, CalendarUnit::Year), 1);
        // Anniversary not yet reached
        let b2 = ndt(2024, 5, 14, 23, 59, 59);
        assert_eq!(diff_in_unit(&a, &b2, CalendarUnit::Year), 0);
        // Negative direction
        assert_eq!(diff_in_unit(&b, &a, CalendarUnit::Year), -1);
    }

    #[test]
    fn diff_quarter_is_months_div_3() {
        let a = ndt(2024, 1, 1, 0, 0, 0);
        let b = ndt(2024, 10, 1, 0, 0, 0);
        // 9 months → 3 quarters.
        assert_eq!(diff_in_unit(&a, &b, CalendarUnit::Quarter), 3);
        // 4 months → 1 quarter (truncation)
        let b2 = ndt(2024, 5, 1, 0, 0, 0);
        assert_eq!(diff_in_unit(&a, &b2, CalendarUnit::Quarter), 1);
    }

    // Coercion: unit is Utf8, both datetime args may be Utf8/Date32/Timestamp
    // of any precision. Template normalizes both to Timestamp(Ms,None).
    #[test]
    fn coerce_types_normalizes_mixed_datetime_args() {
        let udf = TimestampDiffCalendarUdf::new();
        let out = udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Date32])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_non_temporal_operand() {
        let udf = TimestampDiffCalendarUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Int64, DataType::Utf8])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = TimestampDiffCalendarUdf::new();
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }

    #[test]
    fn invoke_rejects_fixed_unit() {
        let udf = TimestampDiffCalendarUdf::new();
        let units = StringArray::from(vec![Some("SECOND")]);
        let a = TimestampMillisecondArray::from(vec![Some(0_i64)]);
        let b = TimestampMillisecondArray::from(vec![Some(3_600_000_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(units)),
                ColumnarValue::Array(Arc::new(a)),
                ColumnarValue::Array(Arc::new(b)),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Int64,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let err = udf.invoke_with_args(args).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("MONTH/QUARTER/YEAR"), "unexpected: {msg}");
    }

    #[test]
    fn invoke_happy_path() {
        let udf = TimestampDiffCalendarUdf::new();
        // 2024-01-15 → 2024-05-15 = 4 months
        let units = StringArray::from(vec![Some("MONTH")]);
        let a = TimestampMillisecondArray::from(vec![Some(1_705_320_000_000_i64)]);
        let b = TimestampMillisecondArray::from(vec![Some(1_715_774_400_000_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(units)),
                ColumnarValue::Array(Arc::new(a)),
                ColumnarValue::Array(Arc::new(b)),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Int64,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
        assert_eq!(arr.value(0), 4);
    }
}
