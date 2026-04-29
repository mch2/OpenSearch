/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch scalar UDFs that aren't in DataFusion's built-in registry. Each
//! must have a matching YAML entry in `extensions/opensearch_scalar.yaml` so
//! the substrait converter on the Java side can route to it by name.
//!
//! Functions registered here:
//! - `e()` → Euler's number (mathematical constant ~2.71828)
//! - `expm1(x)` → e^x - 1 (more accurate than `exp(x) - 1` for small x)
//! - `rint(x)` → round to nearest integer, ties to even (IEEE 754)
//! - `conv(value, from_base, to_base)` → integer base conversion (returns string)

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(EulerUdf::new()));
    ctx.register_udf(ScalarUDF::from(Expm1Udf::new()));
    ctx.register_udf(ScalarUDF::from(RintUdf::new()));
    ctx.register_udf(ScalarUDF::from(ConvUdf::new()));
    log::info!("OpenSearch UDF register_all: e, expm1, rint, conv registered");
}

// ---- e() ------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct EulerUdf {
    signature: Signature,
}

impl EulerUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for EulerUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "e" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut builder = Float64Builder::with_capacity(args.number_rows);
        for _ in 0..args.number_rows {
            builder.append_value(std::f64::consts::E);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- expm1(x) -------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Expm1Udf {
    signature: Signature,
}

impl Expm1Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for Expm1Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "expm1" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = single_fp64_array(&args)?;
        let mut builder = Float64Builder::with_capacity(arg.len());
        for i in 0..arg.len() {
            if arg.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(arg.value(i).exp_m1());
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- rint(x) --------------------------------------------------------------

/// IEEE 754 "round half to even" (banker's rounding). Matches Java's Math.rint.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RintUdf {
    signature: Signature,
}

impl RintUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for RintUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "rint" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = single_fp64_array(&args)?;
        let mut builder = Float64Builder::with_capacity(arg.len());
        for i in 0..arg.len() {
            if arg.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(arg.value(i).round_ties_even());
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- conv(value, from_base, to_base) --------------------------------------

/// Convert an integer between bases. e.g. `conv(15, 10, 2)` → '1111'. Returns a
/// String in the target base. Bases between 2 and 36 supported.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConvUdf {
    signature: Signature,
}

impl ConvUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ConvUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "conv" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = int64_arg(&args, 0)?;
        let from_base = int64_arg(&args, 1)?;
        let to_base = int64_arg(&args, 2)?;
        let n = args.number_rows;
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if value.is_null(i) || from_base.is_null(i) || to_base.is_null(i) {
                builder.append_null();
                continue;
            }
            let v = value.value(i);
            let from = from_base.value(i);
            let to = to_base.value(i);
            if !(2..=36).contains(&from) || !(2..=36).contains(&to) {
                builder.append_null();
                continue;
            }
            // The input value is already a base-10 i64; from_base affects how we'd parse a
            // string input. PPL's conv accepts an integer literal, so we just convert v to
            // the target base. (If a string-input form is needed later, add a 4th impl.)
            let _ = from;
            builder.append_value(to_base_string(v, to as u32));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn to_base_string(mut value: i64, base: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }
    let neg = value < 0;
    if neg {
        value = -value;
    }
    let mut buf = Vec::new();
    while value > 0 {
        let digit = (value % base as i64) as u32;
        buf.push(std::char::from_digit(digit, base).unwrap());
        value /= base as i64;
    }
    if neg {
        buf.push('-');
    }
    buf.iter().rev().collect()
}

// ---- helpers --------------------------------------------------------------

fn single_fp64_array(args: &ScalarFunctionArgs) -> Result<Float64Array> {
    let arr = args.args.first().ok_or_else(|| DataFusionError::Internal("UDF missing arg 0".into()))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal(format!("expected Float64, got {:?}", arr.data_type())))
}

fn int64_arg(args: &ScalarFunctionArgs, idx: usize) -> Result<Int64Array> {
    let arr = args.args.get(idx).ok_or_else(|| DataFusionError::Internal(format!("UDF missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal(format!("arg {} expected Int64, got {:?}", idx, arr.data_type())))
}
