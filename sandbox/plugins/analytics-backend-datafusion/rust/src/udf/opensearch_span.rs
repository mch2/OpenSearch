/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `opensearch_span(field, n, unit)` — polymorphic span/bucket UDF for PPL.
//!
//! Replaces the per-input-type Java decomposition previously in
//! `PPLFuncImpTable.registerSpanFunction` (which rewrote SPAN into a mix of
//! `FROM_UNIXTIME`, `DATE_TRUNC`, and `MAKE_TIME`). That decomposition fell
//! through three separate downstream gaps:
//!
//! 1. substrait's `date_trunc(string, date) → timestamp` lost DATE type info,
//! 2. custom-pattern date fields weren't narrowed to DATE/TIME at schema time,
//! 3. calendar units with n>1 had no primitive to lower to.
//!
//! One polymorphic UDF that returns the SAME type as the input field bypasses
//! all three. Overloads: Timestamp(ms), Date32, Time32(ms), Int64, Float64.
//!
//! Call shape: `opensearch_span(field, n_literal_i64, unit_literal_utf8)`.
//! For numeric fields the unit string is ignored (typically null / empty).

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, Time32MillisecondArray, Time32MillisecondBuilder, TimestampMillisecondArray,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(OpenSearchSpanUdf::new()));
}

/// Unit codes emitted by PPL's `SpanUnit.getName()`.
///
/// `us` / `ms` / `s` / `m` / `h` / `d` / `w` / `M` / `q` / `y`. The visitor emits
/// `constantNull()` for NONE/UNKNOWN (numeric span), so a null unit also reaches
/// this kernel.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SpanUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    /// No unit (numeric span, or absent unit literal). Caller treats this as
    /// "no time-dimension rounding" — numeric overloads use n directly.
    None,
}

impl SpanUnit {
    fn parse(s: Option<&str>) -> Option<Self> {
        match s {
            None => Some(SpanUnit::None),
            Some("") => Some(SpanUnit::None),
            Some("us") => Some(SpanUnit::Microsecond),
            Some("ms") => Some(SpanUnit::Millisecond),
            Some("s") => Some(SpanUnit::Second),
            Some("m") => Some(SpanUnit::Minute),
            Some("h") => Some(SpanUnit::Hour),
            Some("d") => Some(SpanUnit::Day),
            Some("w") => Some(SpanUnit::Week),
            Some("M") | Some("month") | Some("MONTH") => Some(SpanUnit::Month),
            Some("q") | Some("quarter") | Some("QUARTER") => Some(SpanUnit::Quarter),
            Some("y") | Some("year") | Some("YEAR") => Some(SpanUnit::Year),
            Some(_) => None,
        }
    }

    /// Fixed-length units can be bucketed in integer milliseconds-of-day or
    /// epoch-millis without calendar math. Calendar units (month / quarter /
    /// year) need chrono because their length in days varies.
    fn fixed_millis(self) -> Option<i64> {
        match self {
            SpanUnit::Microsecond => Some(1), // round to ms granularity; sub-ms clamped
            SpanUnit::Millisecond => Some(1),
            SpanUnit::Second => Some(1_000),
            SpanUnit::Minute => Some(60_000),
            SpanUnit::Hour => Some(3_600_000),
            SpanUnit::Day => Some(86_400_000),
            SpanUnit::Week => Some(604_800_000),
            _ => None,
        }
    }

}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct OpenSearchSpanUdf {
    signature: Signature,
}

impl OpenSearchSpanUdf {
    pub fn new() -> Self {
        // Each overload: [field, n (i64), unit (utf8 or null)]. We accept Null
        // for the unit to match the numeric-span call shape where PPL passes
        // `constantNull()`. Null dtype in DF is `DataType::Null`.
        let field_types = [
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date32,
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Int64,
            DataType::Float64,
        ];
        let unit_types = [DataType::Utf8, DataType::Null];
        let mut variants: Vec<TypeSignature> = Vec::new();
        for f in &field_types {
            for u in &unit_types {
                variants.push(TypeSignature::Exact(vec![
                    f.clone(),
                    DataType::Int64,
                    u.clone(),
                ]));
            }
        }
        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

impl Default for OpenSearchSpanUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for OpenSearchSpanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "opensearch_span"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("opensearch_span expects 3 arguments, got {}", arg_types.len());
        }
        Ok(match &arg_types[0] {
            // Normalize timestamp precision: PPL / parquet hold Millisecond,
            // but isthmus sometimes plans Microsecond. We bucket in millis
            // either way; returning Millisecond keeps the arrow batch layout
            // stable for the response serializer.
            DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
            DataType::Date32 => DataType::Date32,
            DataType::Time32(unit) => DataType::Time32(*unit),
            DataType::Int64 => DataType::Int64,
            DataType::Float64 => DataType::Float64,
            other => {
                return plan_err!(
                    "opensearch_span: unsupported field type {:?}",
                    other
                )
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!(
                "opensearch_span expects 3 arguments, got {}",
                args.args.len()
            );
        }
        let n = extract_scalar_i64(&args.args[1]).ok_or_else(|| {
            DataFusionError::Plan(
                "opensearch_span: second argument (n) must be a non-null i64 literal".into(),
            )
        })?;
        if n <= 0 {
            return plan_err!("opensearch_span: n must be positive, got {}", n);
        }
        let unit_str = extract_scalar_str(&args.args[2])?;
        let unit = SpanUnit::parse(unit_str.as_deref()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "opensearch_span: unknown unit {:?}",
                unit_str
            ))
        })?;

        // Resolve the field as a full array — handles ColumnarValue::Scalar,
        // single-row broadcast, etc. in one line.
        let field_cv = args.args[0].clone();
        let field_arr = field_cv.into_array(args.number_rows)?;

        match field_arr.data_type() {
            DataType::Timestamp(_, _) => {
                let ts = field_arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: expected TimestampMillisecond, got {:?}",
                            field_arr.data_type()
                        ))
                    })?;
                Ok(ColumnarValue::Array(bucket_timestamp(ts, n, unit)?))
            }
            DataType::Date32 => {
                let d = field_arr
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: expected Date32, got {:?}",
                            field_arr.data_type()
                        ))
                    })?;
                Ok(ColumnarValue::Array(bucket_date(d, n, unit)?))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let t = field_arr
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: expected Time32(Millisecond), got {:?}",
                            field_arr.data_type()
                        ))
                    })?;
                Ok(ColumnarValue::Array(bucket_time_millis(t, n, unit)?))
            }
            DataType::Int64 => {
                let i = field_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: expected Int64, got {:?}",
                            field_arr.data_type()
                        ))
                    })?;
                if matches!(unit, SpanUnit::None) {
                    // Fine — numeric span doesn't care about a unit string.
                } else {
                    // Caller passed a unit on a numeric field. PPL normally emits
                    // null for the unit in this case; non-null means someone wrote
                    // a confused query. Bucket anyway — ignore the unit.
                    log::trace!(
                        "opensearch_span: ignoring unit {:?} on Int64 field",
                        unit
                    );
                }
                Ok(ColumnarValue::Array(bucket_int64(i, n)))
            }
            DataType::Float64 => {
                let f = field_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: expected Float64, got {:?}",
                            field_arr.data_type()
                        ))
                    })?;
                if !matches!(unit, SpanUnit::None) {
                    log::trace!(
                        "opensearch_span: ignoring unit {:?} on Float64 field",
                        unit
                    );
                }
                Ok(ColumnarValue::Array(bucket_float64(f, n)))
            }
            other => plan_err!("opensearch_span: unsupported field type {:?}", other),
        }
    }
}

// ─── scalar-literal extraction ──────────────────────────────────────────────

fn extract_scalar_i64(cv: &ColumnarValue) -> Option<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some(*v),
        // The planner sometimes materializes a literal as a single-row array.
        ColumnarValue::Array(arr) => {
            let arr = arr.as_any().downcast_ref::<Int64Array>()?;
            if arr.len() != 1 || arr.is_null(0) {
                return None;
            }
            Some(arr.value(0))
        }
        _ => None,
    }
}

/// Returns `Ok(None)` for a null/absent unit literal (numeric span); `Ok(Some(s))`
/// for a Utf8 literal; error if the unit is a non-literal column expression.
fn extract_scalar_str(cv: &ColumnarValue) -> Result<Option<String>> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        ColumnarValue::Array(arr) => {
            // Broadcasted null unit comes through as a single-row NullArray.
            if matches!(arr.data_type(), DataType::Null) {
                return Ok(None);
            }
            plan_err!(
                "opensearch_span: unit argument must be a literal, got array of type {:?}",
                arr.data_type()
            )
        }
        _ => plan_err!(
            "opensearch_span: unit argument must be a string or null literal"
        ),
    }
}

// ─── numeric kernels ────────────────────────────────────────────────────────

fn bucket_int64(arr: &Int64Array, n: i64) -> ArrayRef {
    let mut b = Int64Builder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
        } else {
            b.append_value(floor_div_mul_i64(arr.value(i), n));
        }
    }
    Arc::new(b.finish())
}

fn bucket_float64(arr: &Float64Array, n: i64) -> ArrayRef {
    let n = n as f64;
    let mut b = Float64Builder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
        } else {
            let v = arr.value(i);
            // (v / n).floor() * n — keeps negative numbers bucketed toward -inf,
            // matching the Java Expressions.multiply(FLOOR(divide(...)), ...)
            // semantics.
            b.append_value((v / n).floor() * n);
        }
    }
    Arc::new(b.finish())
}

/// Floored division, multiplied back, matching Java's `(v/n)*n` under floor
/// semantics — so negative values bucket downward.
fn floor_div_mul_i64(v: i64, n: i64) -> i64 {
    let q = if (v < 0) && (v % n != 0) {
        v / n - 1
    } else {
        v / n
    };
    q * n
}

// ─── timestamp kernel (millis epoch) ────────────────────────────────────────

fn bucket_timestamp(
    arr: &TimestampMillisecondArray,
    n: i64,
    unit: SpanUnit,
) -> Result<ArrayRef> {
    let mut b = TimestampMillisecondBuilder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
            continue;
        }
        let ts = arr.value(i);
        let bucketed = match unit {
            SpanUnit::None => {
                return plan_err!(
                    "opensearch_span: timestamp field requires a time unit (got none)"
                );
            }
            u if u.fixed_millis().is_some() => {
                let size = u.fixed_millis().unwrap().saturating_mul(n);
                if size == 0 {
                    return plan_err!("opensearch_span: bucket size must be > 0");
                }
                floor_div_mul_i64(ts, size)
            }
            SpanUnit::Month | SpanUnit::Quarter | SpanUnit::Year => {
                let ndt: NaiveDateTime = DateTime::from_timestamp_millis(ts)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "opensearch_span: unrepresentable timestamp millis {}",
                            ts
                        ))
                    })?
                    .naive_utc();
                let truncated = truncate_datetime_calendar(&ndt, n, unit)?;
                truncated.and_utc().timestamp_millis()
            }
            _ => unreachable!("all SpanUnit variants handled"),
        };
        b.append_value(bucketed);
    }
    Ok(Arc::new(b.finish()))
}

// ─── date kernel (days since epoch) ─────────────────────────────────────────

fn bucket_date(arr: &Date32Array, n: i64, unit: SpanUnit) -> Result<ArrayRef> {
    let mut b = Date32Builder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
            continue;
        }
        let days = arr.value(i) as i64;
        let bucketed = match unit {
            SpanUnit::None => {
                return plan_err!(
                    "opensearch_span: date field requires a time unit (got none)"
                );
            }
            SpanUnit::Day => floor_div_mul_i64(days, n),
            SpanUnit::Week => {
                // ISO weeks start Monday. Epoch day 0 was Thursday 1970-01-01,
                // so the Monday that opened that ISO week was day -3. Shift by
                // +3 before flooring so bucket boundaries fall on Mondays, then
                // subtract the shift back out.
                let shifted = days + 3;
                let bucket = floor_div_mul_i64(shifted, 7 * n);
                bucket - 3
            }
            SpanUnit::Month | SpanUnit::Quarter | SpanUnit::Year => {
                let nd = date32_to_naive_date(days as i32)?;
                let truncated = truncate_date_calendar(&nd, n, unit)?;
                naive_date_to_date32(&truncated)
            }
            u if u.fixed_millis().is_some() => {
                // Sub-day fixed units on a DATE field are semantically a no-op —
                // DATE has no intra-day component. Return the day as-is; matches
                // the prior Java fallback which cast through TIMESTAMP and back.
                days
            }
            _ => unreachable!("all SpanUnit variants handled"),
        };
        b.append_value(bucketed as i32);
    }
    Ok(Arc::new(b.finish()))
}

// ─── time kernel (millis-of-day) ────────────────────────────────────────────

fn bucket_time_millis(
    arr: &Time32MillisecondArray,
    n: i64,
    unit: SpanUnit,
) -> Result<ArrayRef> {
    let mut b = Time32MillisecondBuilder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
            continue;
        }
        let millis_of_day = arr.value(i) as i64;
        let bucketed = match unit {
            SpanUnit::None => {
                return plan_err!(
                    "opensearch_span: time field requires a time unit (got none)"
                );
            }
            SpanUnit::Month | SpanUnit::Quarter | SpanUnit::Year => {
                // Calendar units are meaningless on a TIME column. PPL doesn't
                // emit this shape; defensive reject so a confused query gets a
                // clear error instead of silently returning the input unchanged.
                return plan_err!(
                    "opensearch_span: calendar unit {:?} not applicable to time field",
                    unit
                );
            }
            u if u.fixed_millis().is_some() => {
                let size = u.fixed_millis().unwrap().saturating_mul(n);
                if size == 0 {
                    return plan_err!("opensearch_span: bucket size must be > 0");
                }
                // millis-of-day is always non-negative for a valid Time32, so
                // integer division is already floor.
                (millis_of_day / size) * size
            }
            _ => unreachable!("all SpanUnit variants handled"),
        };
        b.append_value(bucketed as i32);
    }
    Ok(Arc::new(b.finish()))
}

// ─── calendar truncation helpers (chrono) ───────────────────────────────────

fn truncate_datetime_calendar(
    dt: &NaiveDateTime,
    n: i64,
    unit: SpanUnit,
) -> Result<NaiveDateTime> {
    let date = truncate_date_calendar(&dt.date(), n, unit)?;
    Ok(date.and_hms_opt(0, 0, 0).expect("midnight always valid"))
}

fn truncate_date_calendar(d: &NaiveDate, n: i64, unit: SpanUnit) -> Result<NaiveDate> {
    let n = n as i32;
    if n <= 0 {
        return plan_err!("opensearch_span: n must be positive");
    }
    let year = d.year();
    let month0 = d.month0() as i32; // 0..=11
    match unit {
        SpanUnit::Year => {
            // Bucket year floor — e.g. n=3 groups {0..2, 3..5, ...}. For
            // negative years we still floor via Euclidean division.
            let bucket_year = year.div_euclid(n) * n;
            NaiveDate::from_ymd_opt(bucket_year, 1, 1).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "opensearch_span: unrepresentable year {}",
                    bucket_year
                ))
            })
        }
        SpanUnit::Quarter => {
            // Quarter is just months grouped in 3s with bucket counter = quarter.
            let quarter = month0 / 3; // 0..=3
            let total_quarters = year * 4 + quarter;
            let bucket = total_quarters.div_euclid(n) * n;
            let ry = bucket.div_euclid(4);
            let rq = bucket.rem_euclid(4);
            NaiveDate::from_ymd_opt(ry, (rq * 3 + 1) as u32, 1).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "opensearch_span: unrepresentable quarter {}-{}",
                    ry, rq
                ))
            })
        }
        SpanUnit::Month => {
            // Same counter trick but in months — handles n>1 (e.g. 3 gives
            // Jan/Apr/Jul/Oct buckets of width 3).
            let total_months = year * 12 + month0;
            let bucket = total_months.div_euclid(n) * n;
            let ry = bucket.div_euclid(12);
            let rm = bucket.rem_euclid(12);
            NaiveDate::from_ymd_opt(ry, (rm + 1) as u32, 1).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "opensearch_span: unrepresentable month {}-{}",
                    ry, rm
                ))
            })
        }
        _ => plan_err!(
            "opensearch_span: {:?} is not a calendar unit",
            unit
        ),
    }
}

fn date32_to_naive_date(days: i32) -> Result<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days + 719163) // epoch = 719163 days from 0001-01-01
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "opensearch_span: unrepresentable epoch day {}",
                days
            ))
        })
}

fn naive_date_to_date32(d: &NaiveDate) -> i64 {
    (d.num_days_from_ce() - 719163) as i64
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Unit parsing handles every PPL-emitted code plus variants.
    #[test]
    fn unit_parse_covers_pgl_codes() {
        for code in ["us", "ms", "s", "m", "h", "d", "w", "M", "q", "y"] {
            assert!(SpanUnit::parse(Some(code)).is_some(), "missed {code}");
        }
        assert_eq!(SpanUnit::parse(None), Some(SpanUnit::None));
        assert_eq!(SpanUnit::parse(Some("")), Some(SpanUnit::None));
        assert_eq!(SpanUnit::parse(Some("bogus")), None);
    }

    // ─── numeric ────────────────────────────────────────────────────────────
    #[test]
    fn int64_positive_buckets_floor() {
        let arr = Int64Array::from(vec![Some(0), Some(4), Some(5), Some(9), Some(10), None]);
        let out = bucket_int64(&arr, 5);
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 0);
        assert_eq!(out.value(1), 0);
        assert_eq!(out.value(2), 5);
        assert_eq!(out.value(3), 5);
        assert_eq!(out.value(4), 10);
        assert!(out.is_null(5));
    }

    #[test]
    fn int64_negative_buckets_floor_down() {
        let arr = Int64Array::from(vec![Some(-1), Some(-5), Some(-6)]);
        let out = bucket_int64(&arr, 5);
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), -5);
        assert_eq!(out.value(1), -5);
        assert_eq!(out.value(2), -10);
    }

    #[test]
    fn float64_buckets_floor() {
        let arr = Float64Array::from(vec![Some(0.0), Some(2.5), Some(4.9), Some(5.0)]);
        let out = bucket_float64(&arr, 5);
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 0.0);
        assert_eq!(out.value(1), 0.0);
        assert_eq!(out.value(2), 0.0);
        assert_eq!(out.value(3), 5.0);
    }

    // ─── date (days) ────────────────────────────────────────────────────────
    #[test]
    fn date_day_unit_preserves_date() {
        // 2024-01-05 = epoch day 19727
        let arr = Date32Array::from(vec![Some(19727)]);
        let out = bucket_date(&arr, 1, SpanUnit::Day).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(out.value(0), 19727);
    }

    #[test]
    fn date_week_aligns_to_monday() {
        // 2024-01-05 is a Friday — Monday of that week is 2024-01-01 (epoch day 19723).
        let arr = Date32Array::from(vec![Some(19727)]);
        let out = bucket_date(&arr, 1, SpanUnit::Week).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(out.value(0), 19723);
    }

    #[test]
    fn date_year_truncates() {
        // 2024-05-15 = epoch day 19858. Year bucket (n=1) → 2024-01-01 = 19723.
        let arr = Date32Array::from(vec![Some(19858)]);
        let out = bucket_date(&arr, 1, SpanUnit::Year).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        // 2024-01-01 = 19723
        assert_eq!(out.value(0), 19723);
    }

    #[test]
    fn date_3month_bucket() {
        // 2024-05-15 → 3-month buckets Jan/Apr/Jul/Oct. 2024-05 lands in April.
        // 2024-04-01 = epoch day 19814.
        let arr = Date32Array::from(vec![Some(19858)]);
        let out = bucket_date(&arr, 3, SpanUnit::Month).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(out.value(0), 19814);
    }

    #[test]
    fn date_null_passthrough() {
        let arr = Date32Array::from(vec![None]);
        let out = bucket_date(&arr, 1, SpanUnit::Day).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert!(out.is_null(0));
    }

    // ─── time (millis-of-day) ───────────────────────────────────────────────
    #[test]
    fn time_minute_bucket() {
        // 09:07:30.125 = 32_850_125 ms. 1-minute bucket = 32_820_000 (09:07:00).
        let arr = Time32MillisecondArray::from(vec![Some(32_850_125)]);
        let out = bucket_time_millis(&arr, 1, SpanUnit::Minute).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 32_820_000);
    }

    #[test]
    fn time_15minute_bucket() {
        // 09:17:30 (33_450_000ms). 15-minute buckets: 00, 15, 30, 45 → 09:15:00 = 33_300_000.
        let arr = Time32MillisecondArray::from(vec![Some(33_450_000)]);
        let out = bucket_time_millis(&arr, 15, SpanUnit::Minute).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 33_300_000);
    }

    #[test]
    fn time_hour_bucket() {
        // 06:30:00 = 23_400_000ms. 6-hour bucket: 00, 06, 12, 18 → 06:00:00 = 21_600_000.
        let arr = Time32MillisecondArray::from(vec![Some(23_400_000)]);
        let out = bucket_time_millis(&arr, 6, SpanUnit::Hour).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 21_600_000);
    }

    #[test]
    fn time_calendar_unit_errors() {
        let arr = Time32MillisecondArray::from(vec![Some(0)]);
        let err = bucket_time_millis(&arr, 1, SpanUnit::Month).unwrap_err();
        assert!(
            format!("{err}").contains("not applicable to time"),
            "unexpected error: {err}"
        );
    }

    // ─── timestamp (epoch millis) ───────────────────────────────────────────
    #[test]
    fn timestamp_day_bucket() {
        // 2024-01-05 12:34:56 UTC → 1704458096000. Day bucket = 1704412800000 (2024-01-05 00:00).
        let arr = TimestampMillisecondArray::from(vec![Some(1_704_458_096_000)]);
        let out = bucket_timestamp(&arr, 1, SpanUnit::Day).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 1_704_412_800_000);
    }

    #[test]
    fn timestamp_3month_bucket() {
        // 2024-05-15 00:00:00 UTC = 1715731200000. 3-month bucket → 2024-04-01 UTC = 1711929600000.
        let arr = TimestampMillisecondArray::from(vec![Some(1_715_731_200_000)]);
        let out = bucket_timestamp(&arr, 3, SpanUnit::Month).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 1_711_929_600_000);
    }

    #[test]
    fn timestamp_year_bucket() {
        // 2024-05-15 UTC → year bucket = 2024-01-01 = 1704067200000.
        let arr = TimestampMillisecondArray::from(vec![Some(1_715_731_200_000)]);
        let out = bucket_timestamp(&arr, 1, SpanUnit::Year).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 1_704_067_200_000);
    }

    #[test]
    fn timestamp_null_passthrough() {
        let arr = TimestampMillisecondArray::from(vec![None]);
        let out = bucket_timestamp(&arr, 1, SpanUnit::Day).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(out.is_null(0));
    }

    // ─── return-type signalling ─────────────────────────────────────────────
    #[test]
    fn return_type_identity_per_overload() {
        let udf = OpenSearchSpanUdf::new();
        assert_eq!(
            udf.return_type(&[DataType::Date32, DataType::Int64, DataType::Utf8])
                .unwrap(),
            DataType::Date32
        );
        assert_eq!(
            udf.return_type(&[
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Int64,
                DataType::Utf8
            ])
            .unwrap(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            udf.return_type(&[
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Int64,
                DataType::Utf8
            ])
            .unwrap(),
            // Normalized to Millisecond, as declared in the substrait catalog.
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            udf.return_type(&[DataType::Int64, DataType::Int64, DataType::Null])
                .unwrap(),
            DataType::Int64
        );
        assert_eq!(
            udf.return_type(&[DataType::Float64, DataType::Int64, DataType::Null])
                .unwrap(),
            DataType::Float64
        );
    }
}
