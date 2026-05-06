/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `span(value, interval, unit)` — numeric-span UDF (PPL).
//!
//! Partial port of the OpenSearch-SQL polymorphic `SPAN` UDF. PPL's
//! {@code SPAN(field, interval, unit)} is emitted by the frontend with a
//! string unit literal for date/time spans and `null` for pure numeric spans
//! (see `CalciteRexNodeVisitor.visitSpan`). This UDF covers the **numeric-
//! only** path:
//!
//!   `bin_start = floor(value / interval) * interval`
//!
//! The date/time span path (unit = "us" | "ms" | "s" | "m" | "h" | "d" | "w"
//! | "M" | "q" | "y") is expected to be bridged on the coordinator via the
//! three-tier execution pattern — the frontend produces a coordinator-side
//! eval that never dispatches to this UDF. If a date/time call does reach
//! this UDF (misrouting), we return a plan error rather than silently wrong
//! answers.
//!
//! <b>Correctness note — integer semantics drift:</b> Java's local PPL
//! execution uses integer truncation for integer fields (`(v / n) * n`,
//! where `/` truncates toward zero). This UDF uses `floor` semantics
//! uniformly (coerces integers to Float64). The two diverge only on
//! negative integer values with non-factor intervals (e.g. `span(-5, 3)`
//! is `-3` under truncation, `-6` under floor). Matches the choice made in
//! subtraitupdates's `opensearch_span` kernel. Flagged in PR description; a
//! future refinement may add an Int64 overload that preserves truncation.
//!
//! Argument handling:
//! * `value` — any numeric (canonicalised to Float64)
//! * `interval` — any numeric (canonicalised to Float64); `<= 0` or
//!   non-finite → null output
//! * `unit` — string literal or null. Null-or-empty → numeric mode. Any
//!   non-null non-empty value → plan error (misrouted date/time span).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_slot, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SpanUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SpanUdf {
    signature: Signature,
}

impl SpanUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SpanUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SpanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "span"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("span expects 3 arguments, got {}", arg_types.len());
        }
        // Numeric-only UDF: return Float64 (post-coerce canonical type).
        Ok(DataType::Float64)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return plan_err!("span expects 3 arguments, got {}", arg_types.len());
        }
        // Slots 0 and 1 are numeric → Float64. Slot 2 is the unit literal —
        // accept Utf8 / LargeUtf8 / Utf8View / Null. `coerce_slot`'s Utf8
        // mode rejects Null, so handle it locally.
        let value = coerce_slot("span", 0, &arg_types[0], CoerceMode::Float64)?;
        let interval = coerce_slot("span", 1, &arg_types[1], CoerceMode::Float64)?;
        let unit = match &arg_types[2] {
            DataType::Null => DataType::Null,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => {
                return plan_err!(
                    "span: arg 2 (unit) expected string or null, got {:?}",
                    other
                );
            }
        };
        Ok(vec![value, interval, unit])
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("span expects 3 arguments, got {}", args.args.len());
        }
        // Extract unit literal first — fail fast if misrouted (non-empty
        // unit → date/time span, should not reach this UDF).
        ensure_numeric_mode(&args.args[2])?;

        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let interval = args.args[1].clone().into_array(n)?;

        let value = value
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "span: value expected Float64 post-coerce, got {:?}",
                    value.data_type()
                ))
            })?;
        let interval = interval
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "span: interval expected Float64 post-coerce, got {:?}",
                    interval.data_type()
                ))
            })?;

        let mut builder = Float64Builder::with_capacity(n);
        for i in 0..n {
            if value.is_null(i) || interval.is_null(i) {
                builder.append_null();
                continue;
            }
            match calculate(value.value(i), interval.value(i)) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Validate that the unit argument is null/empty (numeric-span mode). Returns
/// `Ok(())` for valid numeric-mode calls; a plan error if the unit is a
/// non-empty string (which should have been bridged on the coordinator).
fn ensure_numeric_mode(cv: &ColumnarValue) -> Result<()> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => Ok(()),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
            if s.is_empty() {
                Ok(())
            } else {
                plan_err!(
                    "span: time unit '{s}' not supported by the data-node UDF \
                     (expected null for numeric span; date/time span must be \
                     bridged on the coordinator)"
                )
            }
        }
        ColumnarValue::Array(arr) if matches!(arr.data_type(), DataType::Null) => Ok(()),
        ColumnarValue::Array(arr) => {
            // If the unit reached us as a string *array* rather than scalar,
            // scan it for any non-null non-empty value. In practice PPL emits
            // the unit as a literal so this path is defensive.
            if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                for i in 0..s.len() {
                    if !s.is_null(i) && !s.value(i).is_empty() {
                        return plan_err!(
                            "span: time unit '{}' not supported by the data-node UDF",
                            s.value(i)
                        );
                    }
                }
                Ok(())
            } else {
                plan_err!(
                    "span: arg 2 (unit) must be a string or null literal, got array of {:?}",
                    arr.data_type()
                )
            }
        }
        other => plan_err!("span: arg 2 (unit) unexpected shape: {:?}", other),
    }
}

/// Core numeric span: `floor(value / interval) * interval`. Returns `None`
/// when interval is non-finite or non-positive (matches the span_bucket /
/// width_bucket family's failure-mode convention).
fn calculate(value: f64, interval: f64) -> Option<f64> {
    if !interval.is_finite() || interval <= 0.0 {
        return None;
    }
    Some((value / interval).floor() * interval)
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{AsArray, NullArray};
    use datafusion::arrow::datatypes::Field;

    // ── calculate(): core math ─────────────────────────────────────────────

    #[test]
    fn positive_value_rounds_down_to_bin_start() {
        assert_eq!(calculate(15.0, 5.0), Some(15.0));
        assert_eq!(calculate(17.0, 5.0), Some(15.0));
        assert_eq!(calculate(19.999, 5.0), Some(15.0));
    }

    #[test]
    fn negative_value_floors_toward_negative_infinity() {
        // floor(-5/3) = floor(-1.667) = -2 → -2*3 = -6. This is the
        // documented divergence from Java integer-trunc semantics:
        // Java: (-5/3)*3 = -3; Rust: -6.
        assert_eq!(calculate(-5.0, 3.0), Some(-6.0));
        // -15/5 = -3 exactly → no divergence from Java (exact multiples
        // behave the same under trunc and floor).
        assert_eq!(calculate(-15.0, 5.0), Some(-15.0));
    }

    #[test]
    fn zero_value_stays_in_first_positive_bucket() {
        assert_eq!(calculate(0.0, 5.0), Some(0.0));
    }

    #[test]
    fn non_positive_interval_returns_none() {
        assert_eq!(calculate(10.0, 0.0), None);
        assert_eq!(calculate(10.0, -1.0), None);
    }

    #[test]
    fn non_finite_interval_returns_none() {
        assert_eq!(calculate(10.0, f64::NAN), None);
        assert_eq!(calculate(10.0, f64::INFINITY), None);
        assert_eq!(calculate(10.0, f64::NEG_INFINITY), None);
    }

    #[test]
    fn fractional_interval_buckets_exactly() {
        // Basic fractional interval — no float rounding gotchas expected
        // for these inputs.
        assert_eq!(calculate(3.7, 1.5), Some(3.0));
        assert_eq!(calculate(3.0, 1.5), Some(3.0));
    }

    // ── ensure_numeric_mode(): unit-literal validation ─────────────────────

    #[test]
    fn null_unit_scalar_accepted() {
        assert!(ensure_numeric_mode(&ColumnarValue::Scalar(ScalarValue::Null)).is_ok());
        assert!(ensure_numeric_mode(&ColumnarValue::Scalar(ScalarValue::Utf8(None))).is_ok());
    }

    #[test]
    fn empty_unit_scalar_accepted() {
        assert!(ensure_numeric_mode(&ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::new())))).is_ok());
    }

    #[test]
    fn time_unit_scalar_rejected_with_informative_error() {
        let err = ensure_numeric_mode(&ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "d".to_string(),
        ))))
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("time unit 'd'"),
            "error must name the offending unit, got: {msg}"
        );
    }

    #[test]
    fn null_array_unit_accepted() {
        // DataFusion may materialise a `null` literal as a single-row NullArray.
        let null_arr = Arc::new(NullArray::new(1)) as ArrayRef;
        assert!(ensure_numeric_mode(&ColumnarValue::Array(null_arr)).is_ok());
    }

    #[test]
    fn string_array_with_only_nulls_accepted() {
        let arr = Arc::new(StringArray::from(vec![None::<&str>, None])) as ArrayRef;
        assert!(ensure_numeric_mode(&ColumnarValue::Array(arr)).is_ok());
    }

    #[test]
    fn string_array_with_non_empty_value_rejected() {
        let arr = Arc::new(StringArray::from(vec![Some(""), Some("h")])) as ArrayRef;
        let err = ensure_numeric_mode(&ColumnarValue::Array(arr)).unwrap_err();
        assert!(err.to_string().contains("time unit 'h'"));
    }

    // ── invoke_with_args: batch semantics ──────────────────────────────────

    fn invoke_batch(
        values: Vec<Option<f64>>,
        intervals: Vec<Option<f64>>,
    ) -> Vec<Option<f64>> {
        let n = values.len();
        assert_eq!(n, intervals.len());
        let udf = SpanUdf::new();
        let value_arr = Arc::new(Float64Array::from(values)) as ArrayRef;
        let interval_arr = Arc::new(Float64Array::from(intervals)) as ArrayRef;
        let unit = ColumnarValue::Scalar(ScalarValue::Null);
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("interval", DataType::Float64, true)),
            Arc::new(Field::new("unit", DataType::Null, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(value_arr),
                ColumnarValue::Array(interval_arr),
                unit,
            ],
            arg_fields,
            number_rows: n,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(a) = out else { panic!("expected array") };
        let f = a.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        (0..n)
            .map(|i| if f.is_null(i) { None } else { Some(f.value(i)) })
            .collect()
    }

    #[test]
    fn batch_happy_path_with_null_unit_scalar() {
        let out = invoke_batch(
            vec![Some(15.0), Some(0.0), Some(-5.0)],
            vec![Some(5.0), Some(5.0), Some(3.0)],
        );
        assert_eq!(out, vec![Some(15.0), Some(0.0), Some(-6.0)]);
    }

    #[test]
    fn batch_null_value_propagates_null() {
        let out = invoke_batch(vec![None, Some(15.0)], vec![Some(5.0), Some(5.0)]);
        assert_eq!(out, vec![None, Some(15.0)]);
    }

    #[test]
    fn batch_null_interval_propagates_null() {
        let out = invoke_batch(vec![Some(15.0), Some(25.0)], vec![None, Some(5.0)]);
        assert_eq!(out, vec![None, Some(25.0)]);
    }

    #[test]
    fn batch_non_positive_interval_produces_null_not_error() {
        let out = invoke_batch(
            vec![Some(15.0), Some(15.0), Some(15.0)],
            vec![Some(0.0), Some(-1.0), Some(5.0)],
        );
        assert_eq!(out, vec![None, None, Some(15.0)]);
    }

    #[test]
    fn batch_with_time_unit_scalar_returns_plan_error() {
        let udf = SpanUdf::new();
        let value_arr = Arc::new(Float64Array::from(vec![Some(15.0)])) as ArrayRef;
        let interval_arr = Arc::new(Float64Array::from(vec![Some(5.0)])) as ArrayRef;
        let unit = ColumnarValue::Scalar(ScalarValue::Utf8(Some("h".to_string())));
        let return_field = Arc::new(Field::new("out", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("interval", DataType::Float64, true)),
            Arc::new(Field::new("unit", DataType::Utf8, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(value_arr),
                ColumnarValue::Array(interval_arr),
                unit,
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err(), "expected plan error on time-unit call, got {result:?}");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("time unit 'h' not supported")
        );
    }

    // ── coerce_types ───────────────────────────────────────────────────────

    #[test]
    fn coerce_types_canonicalises_numeric_inputs_and_accepts_null_unit() {
        let udf = SpanUdf::new();
        let out = udf
            .coerce_types(&[DataType::Int32, DataType::Int64, DataType::Null])
            .unwrap();
        assert_eq!(out, vec![DataType::Float64, DataType::Float64, DataType::Null]);
    }

    #[test]
    fn coerce_types_accepts_utf8_variants_for_unit() {
        let udf = SpanUdf::new();
        for unit_ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let out = udf
                .coerce_types(&[DataType::Float64, DataType::Float64, unit_ty.clone()])
                .unwrap();
            assert_eq!(out[2], DataType::Utf8, "unit {:?} should canonicalise to Utf8", unit_ty);
        }
    }

    #[test]
    fn coerce_types_rejects_non_string_non_null_unit() {
        let udf = SpanUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Float64, DataType::Float64, DataType::Int64])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_string_value_slot() {
        let udf = SpanUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Float64, DataType::Null])
            .is_err());
    }

    #[test]
    fn return_type_enforces_three_arg_arity() {
        let udf = SpanUdf::new();
        assert_eq!(
            udf.return_type(&[DataType::Float64, DataType::Float64, DataType::Null])
                .unwrap(),
            DataType::Float64
        );
        assert!(udf.return_type(&[DataType::Float64]).is_err());
    }
}
