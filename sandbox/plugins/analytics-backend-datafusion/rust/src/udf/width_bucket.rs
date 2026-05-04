/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `width_bucket(field, num_bins, data_range, max_value)` — histogram bucket label.
//!
//! Rationale (UDF over decomposition): bit-exact parity with sql-repo
//! `org.opensearch.sql.expression.function.udf.binning.WidthBucketFunction`
//! requires the `ceil(range/width) > requestedBins` next-magnitude rescale and the
//! `maxValue % width == 0` off-by-one adjustment that WidthBucketAdapter dropped.
//! Encoding both branches in a Rex tree means nested CASEs around power() / ceil()
//! / mod — each step lossy when the CASE arms coerce through the same numeric
//! type. Straight Rust with f64 matches Java's algorithm exactly.

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

/// BinConstants.MIN_BINS / MAX_BINS from sql-repo (see
/// `org.opensearch.sql.calcite.utils.binning.BinConstants`).
const MIN_BINS: i64 = 2;
const MAX_BINS: i64 = 50_000;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(WidthBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WidthBucketUdf {
    signature: Signature,
}

impl WidthBucketUdf {
    pub fn new() -> Self {
        // PPL emits width_bucket with arbitrary numeric types on any arg.
        // user_defined + coerce normalizes: field/range/max → Float64,
        // num_bins → Int64, matching invoke_with_args's as_f64/as_i64 contract.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for WidthBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for WidthBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "width_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 4 {
            return plan_err!(
                "width_bucket expects 4 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "width_bucket",
            arg_types,
            &[
                CoerceMode::Float64,
                CoerceMode::Int64,
                CoerceMode::Float64,
                CoerceMode::Float64,
            ],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 4 {
            return plan_err!(
                "width_bucket expects 4 arguments, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;
        let field = args.args[0].clone().into_array(n)?;
        let num_bins = args.args[1].clone().into_array(n)?;
        let data_range = args.args[2].clone().into_array(n)?;
        let max_value = args.args[3].clone().into_array(n)?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if field.is_null(i)
                || num_bins.is_null(i)
                || data_range.is_null(i)
                || max_value.is_null(i)
            {
                builder.append_null();
                continue;
            }
            let v = as_f64(&field, i)?;
            let bins = as_i64(&num_bins, i)?;
            let range = as_f64(&data_range, i)?;
            let max = as_f64(&max_value, i)?;
            match calculate_width_bucket(v, bins, range, max) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Mirrors `WidthBucketFunction.calculateWidthBucket` in sql-repo.
fn calculate_width_bucket(value: f64, num_bins: i64, range: f64, max: f64) -> Option<String> {
    if !(MIN_BINS..=MAX_BINS).contains(&num_bins) {
        return None;
    }
    if !range.is_finite() || range <= 0.0 {
        return None;
    }
    let width = calculate_optimal_width(range, max, num_bins);
    if !width.is_finite() || width <= 0.0 {
        return None;
    }
    let bin_start = (value / width).floor() * width;
    let bin_end = bin_start + width;
    Some(format_range(bin_start, bin_end, width))
}

/// Mirrors `WidthBucketFunction.calculateOptimalWidth`.
fn calculate_optimal_width(data_range: f64, max_value: f64, requested_bins: i64) -> f64 {
    if data_range <= 0.0 || requested_bins <= 0 {
        return 1.0; // safe fallback, matches Java
    }
    let target_width = data_range / requested_bins as f64;
    let exponent = target_width.log10().ceil();
    let mut optimal_width = 10f64.powf(exponent);
    let mut actual_bins = (data_range / optimal_width).ceil();
    // Java: if (maxValue % optimalWidth == 0) actualBins++;
    if max_value % optimal_width == 0.0 {
        actual_bins += 1.0;
    }
    if actual_bins > requested_bins as f64 {
        optimal_width = 10f64.powf(exponent + 1.0);
    }
    optimal_width
}

/// Same formatRange as span_bucket; duplicated locally to keep this module
/// self-contained (span_bucket and width_bucket may ship to different codepaths
/// and we don't want to risk an inadvertent shared-format divergence).
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
            .ok_or_else(|| DataFusionError::Internal("width_bucket: i64 cast".into()))?
            .value(i) as f64),
        DataType::Float64 => Ok(arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("width_bucket: f64 cast".into()))?
            .value(i)),
        other => Err(DataFusionError::Internal(format!(
            "width_bucket: unsupported type {:?}",
            other
        ))),
    }
}

fn as_i64(arr: &ArrayRef, i: usize) -> Result<i64> {
    match arr.data_type() {
        DataType::Int64 => Ok(arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("width_bucket: i64 cast".into()))?
            .value(i)),
        DataType::Float64 => Ok(arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("width_bucket: f64 cast".into()))?
            .value(i) as i64),
        other => Err(DataFusionError::Internal(format!(
            "width_bucket: unsupported int type {:?}",
            other
        ))),
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Bank fixture case: range=33539, bins=10 → width 10000 → bucket "30000-40000".
    #[test]
    fn bank_fixture_happy_path() {
        assert_eq!(
            calculate_width_bucket(39225.0, 10, 33539.0, 33539.0).as_deref(),
            Some("30000-40000")
        );
    }

    // When actualBins would exceed requestedBins, width rescales one magnitude up.
    #[test]
    fn rescale_when_actual_bins_exceeds_requested() {
        // range=100, bins=2 → targetWidth=50 → exponent=ceil(log10(50))=2 → optimal_width=100.
        // actualBins = ceil(100/100) = 1. max % width check: 100 % 100 == 0 → actualBins = 2.
        // 2 > 2 is false, so no rescale.
        assert_eq!(calculate_optimal_width(100.0, 100.0, 2), 100.0);
        // range=99, bins=10 → targetWidth=9.9 → exponent=1 → width=10. ceil(99/10)=10. 99%10=9 → no inc.
        // 10 > 10 false → width=10.
        assert_eq!(calculate_optimal_width(99.0, 99.0, 10), 10.0);
        // range=120, bins=10 → targetWidth=12 → exponent=2 → width=100. ceil(120/100)=2. 120%100=20 → no inc.
        // 2 > 10 false → width=100.
        assert_eq!(calculate_optimal_width(120.0, 120.0, 10), 100.0);
    }

    #[test]
    fn invalid_bins_returns_null() {
        // bins below MIN_BINS (2).
        assert_eq!(calculate_width_bucket(5.0, 1, 100.0, 100.0), None);
        // bins above MAX_BINS.
        assert_eq!(calculate_width_bucket(5.0, 1_000_000, 100.0, 100.0), None);
    }

    #[test]
    fn invalid_range_returns_null() {
        assert_eq!(calculate_width_bucket(5.0, 10, 0.0, 100.0), None);
        assert_eq!(calculate_width_bucket(5.0, 10, -1.0, 100.0), None);
    }

    #[test]
    fn max_value_mod_optimal_width_zero_triggers_rescale_or_label() {
        // range=50, bins=5 → targetWidth=10 → exponent=1 → width=10.
        // ceil(50/10)=5. 50 % 10 == 0 → actualBins=6 > 5 → rescale to 10^2=100.
        assert_eq!(calculate_optimal_width(50.0, 50.0, 5), 100.0);
    }

    // Coercion: PPL emits width_bucket with field as any numeric type (Int32
    // from integer-typed columns, Float32 from half-precision aggregates, etc.).
    // Existing Signature::one_of only admitted Int64/Float64, so Int32 inputs
    // failed planning. User-defined + coerce normalizes: field/range/max → f64,
    // num_bins → i64.
    #[test]
    fn coerce_types_normalizes_mixed_numerics() {
        let udf = WidthBucketUdf::new();
        let out = udf
            .coerce_types(&[
                DataType::Int32,
                DataType::Int32,
                DataType::Float32,
                DataType::Int32,
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Float64,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64,
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_non_numeric() {
        let udf = WidthBucketUdf::new();
        assert!(udf
            .coerce_types(&[
                DataType::Utf8,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64,
            ])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = WidthBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Int64, DataType::Int64, DataType::Int64])
            .is_err());
    }

    #[test]
    fn invoke_with_float_types() {
        let udf = WidthBucketUdf::new();
        let field = Float64Array::from(vec![Some(39225.0)]);
        let bins = Int64Array::from(vec![Some(10_i64)]);
        let range = Float64Array::from(vec![Some(33539.0)]);
        let max = Float64Array::from(vec![Some(33539.0)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(field)),
                ColumnarValue::Array(Arc::new(bins)),
                ColumnarValue::Array(Arc::new(range)),
                ColumnarValue::Array(Arc::new(max)),
            ],
            number_rows: 1,
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
    }
}
