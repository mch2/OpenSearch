/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `span_bucket(field, span)` — label a numeric value by its fixed-width bucket
//! as `"start-end"`. Mirrors sql-repo
//! `org.opensearch.sql.expression.function.udf.binning.SpanBucketFunction`
//! including integer/float formatting and the non-positive-span → null rule.
//!
//! Rationale (UDF over decomposition): the SPI adapter previously shipped here
//! (SpanBucketAdapter) only handled integer-span, integer-value cases; floating-
//! point spans produced non-bit-exact output. Matching the Java UDF's decimal-
//! places policy inside a Rex tree would mean wrapping `to_char` calls with
//! per-span-value format strings — not expressible without a dispatch at runtime.
//! Doing it in Rust is straightforward.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SpanBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SpanBucketUdf {
    signature: Signature,
}

impl SpanBucketUdf {
    pub fn new() -> Self {
        // PPL emits span_bucket with arbitrary numeric pairs (Int32, Float32).
        // Exact one_of only covered {Int64, Float64} pairs — Int32 failed
        // planning. user_defined + coerce normalizes both to Float64.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SpanBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SpanBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "span_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("span_bucket expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "span_bucket",
            arg_types,
            &[CoerceMode::Float64, CoerceMode::Float64],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("span_bucket expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let field = args.args[0].clone().into_array(n)?;
        let span = args.args[1].clone().into_array(n)?;
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            let field_null = field.is_null(i);
            let span_null = span.is_null(i);
            if field_null || span_null {
                builder.append_null();
                continue;
            }
            let v = as_f64(&field, i)?;
            let s = as_f64(&span, i)?;
            match calculate_span_bucket(v, s) {
                Some(label) => builder.append_value(label),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Mirrors `SpanBucketFunction.calculateSpanBucket` in sql-repo.
fn calculate_span_bucket(value: f64, span: f64) -> Option<String> {
    if !span.is_finite() || span <= 0.0 {
        return None;
    }
    let bin_start = (value / span).floor() * span;
    let bin_end = bin_start + span;
    Some(format_range(bin_start, bin_end, span))
}

/// Mirrors `SpanBucketFunction.formatRange`.
fn format_range(bin_start: f64, bin_end: f64, span: f64) -> String {
    if is_integer_span(span) && is_integer_value(bin_start) && is_integer_value(bin_end) {
        format!("{}-{}", bin_start as i64, bin_end as i64)
    } else {
        let places = appropriate_decimal_places(span);
        format!("{:.*}-{:.*}", places, bin_start, places, bin_end)
    }
}

fn is_integer_span(span: f64) -> bool {
    span.is_finite() && span == span.floor()
}

fn is_integer_value(v: f64) -> bool {
    (v - v.round()).abs() < 1e-10
}

fn appropriate_decimal_places(span: f64) -> usize {
    if span >= 1.0 {
        1
    } else if span >= 0.1 {
        2
    } else if span >= 0.01 {
        3
    } else {
        4
    }
}

fn as_f64(arr: &ArrayRef, i: usize) -> Result<f64> {
    match arr.data_type() {
        DataType::Int64 => Ok(arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("span_bucket: i64 cast".into()))?
            .value(i) as f64),
        DataType::Float64 => Ok(arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("span_bucket: f64 cast".into()))?
            .value(i)),
        other => Err(DataFusionError::Internal(format!(
            "span_bucket: unsupported type {:?}",
            other
        ))),
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Integer span + integer value → "start-end" with no decimals.
    #[test]
    fn integer_span_integer_value() {
        assert_eq!(
            calculate_span_bucket(39225.0, 10000.0).as_deref(),
            Some("30000-40000")
        );
        assert_eq!(
            calculate_span_bucket(5.0, 5.0).as_deref(),
            Some("5-10")
        );
        assert_eq!(
            calculate_span_bucket(-1.0, 5.0).as_deref(),
            Some("-5-0")
        );
    }

    #[test]
    fn float_span_picks_decimal_places() {
        // span 0.5 → 2 decimals (>=0.1 bucket); both bounds non-integer, so float form.
        assert_eq!(
            calculate_span_bucket(1.25, 0.5).as_deref(),
            Some("1.00-1.50")
        );
        // Float span 2.5 with value 3.2: binStart = floor(3.2/2.5)*2.5 = 2.5, binEnd=5.0.
        // span is non-integer → float form, 1 decimal (span >= 1.0).
        assert_eq!(
            calculate_span_bucket(3.2, 2.5).as_deref(),
            Some("2.5-5.0")
        );
        // span 0.05 → 3 decimal places, non-integer span → float form.
        assert_eq!(
            calculate_span_bucket(0.17, 0.05).as_deref(),
            Some("0.150-0.200")
        );
    }

    #[test]
    fn integer_value_span_formats_without_decimal() {
        // span=1.0 IS integer span, 3.2/1.0 = 3 (int), 4 (int) → "3-4" by Java's logic.
        assert_eq!(
            calculate_span_bucket(3.2, 1.0).as_deref(),
            Some("3-4")
        );
    }

    #[test]
    fn non_positive_span_returns_null() {
        assert_eq!(calculate_span_bucket(5.0, 0.0), None);
        assert_eq!(calculate_span_bucket(5.0, -1.0), None);
        assert_eq!(calculate_span_bucket(5.0, f64::NAN), None);
        assert_eq!(calculate_span_bucket(5.0, f64::INFINITY), None);
    }

    // Coercion: PPL emits span_bucket with any numeric pair (Int32/Float32 are
    // the common failing paths). Exact signatures rejected everything that
    // wasn't in the four listed (i64,i64)/(f64,f64)/… pairs. Canonicalize both
    // to Float64 since invoke_with_args uses `as_f64` for each slot.
    #[test]
    fn coerce_types_normalizes_mixed_numeric_pairs() {
        let udf = SpanBucketUdf::new();
        let out = udf
            .coerce_types(&[DataType::Int32, DataType::Float32])
            .unwrap();
        assert_eq!(out, vec![DataType::Float64, DataType::Float64]);
    }

    #[test]
    fn coerce_types_rejects_non_numeric() {
        let udf = SpanBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Int64])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = SpanBucketUdf::new();
        assert!(udf.coerce_types(&[DataType::Int64]).is_err());
    }

    #[test]
    fn invoke_batch_with_nulls_and_mixed_types() {
        let udf = SpanBucketUdf::new();
        let field = Int64Array::from(vec![Some(39225_i64), None, Some(5_i64)]);
        let span = Int64Array::from(vec![Some(10000_i64), Some(10000_i64), None]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(field)),
                ColumnarValue::Array(Arc::new(span)),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Utf8,
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
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "30000-40000");
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}
