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
//! - `crc32(string)` → CRC-32 hash, returned as i64
//! - `md5(string)` → MD5 hex digest (overrides DF's built-in to ensure standard MD5)
//! - `sha1(string)` → SHA-1 hex digest
//! - `sha2(string, bits)` → SHA-2 hex digest with bits ∈ {224, 256, 384, 512}
//! - `cidrmatch(cidr, ip)` → boolean, true iff `ip` is in `cidr`
//! - `json_valid(string)` → boolean, true iff input parses as JSON
//! - `json_object(k, v, ...)` → JSON object string from alternating key/value pairs

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Array, Float64Builder, Int64Array, StringBuilder,
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
    ctx.register_udf(ScalarUDF::from(Crc32Udf::new()));
    ctx.register_udf(ScalarUDF::from(Md5Udf::new()));
    ctx.register_udf(ScalarUDF::from(Sha1Udf::new()));
    ctx.register_udf(ScalarUDF::from(Sha2Udf::new()));
    ctx.register_udf(ScalarUDF::from(Sha256Udf::new()));
    ctx.register_udf(ScalarUDF::from(CidrMatchUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonValidUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonObjectUdf::new()));
    log::info!("OpenSearch UDF register_all: e, expm1, rint, conv, crc32, md5, sha1, sha2, cidrmatch, json_valid, json_object registered");
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

// ---- crc32(string) --------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Crc32Udf {
    signature: Signature,
}

impl Crc32Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for Crc32Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "crc32" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arr = args.args.first().ok_or_else(|| DataFusionError::Internal("crc32 missing arg".into()))?;
        let arr = arr.clone().into_array(args.number_rows)?;
        let string_arr = arr.as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| DataFusionError::Internal(format!("crc32 expected Utf8, got {:?}", arr.data_type())))?;
        let mut builder = datafusion::arrow::array::Int64Builder::with_capacity(string_arr.len());
        for i in 0..string_arr.len() {
            if string_arr.is_null(i) {
                builder.append_null();
            } else {
                let crc = crc32_hash(string_arr.value(i).as_bytes());
                builder.append_value(crc as i64);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn crc32_hash(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
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

fn string_arg(args: &ScalarFunctionArgs, idx: usize) -> Result<datafusion::arrow::array::StringArray> {
    let arr = args.args.get(idx).ok_or_else(|| DataFusionError::Internal(format!("UDF missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal(format!("arg {} expected Utf8, got {:?}", idx, arr.data_type())))
}

fn int32_arg(args: &ScalarFunctionArgs, idx: usize) -> Result<datafusion::arrow::array::Int32Array> {
    let arr = args.args.get(idx).ok_or_else(|| DataFusionError::Internal(format!("UDF missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal(format!("arg {} expected Int32, got {:?}", idx, arr.data_type())))
}

// ---- md5(string) ----------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Md5Udf {
    signature: Signature,
}

impl Md5Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for Md5Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ppl_md5" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use md5::{Digest, Md5};
        let arr = string_arg(&args, 0)?;
        let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 32);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                let mut hasher = Md5::new();
                hasher.update(arr.value(i).as_bytes());
                builder.append_value(hex::encode(hasher.finalize()));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- sha1(string) ---------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha1Udf {
    signature: Signature,
}

impl Sha1Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for Sha1Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ppl_sha1" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use sha1::{Digest, Sha1};
        let arr = string_arg(&args, 0)?;
        let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 40);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                let mut hasher = Sha1::new();
                hasher.update(arr.value(i).as_bytes());
                builder.append_value(hex::encode(hasher.finalize()));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- sha2(string, bits) ---------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha2Udf {
    signature: Signature,
}

impl Sha2Udf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Sha2Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ppl_sha2" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
        let value = string_arg(&args, 0)?;
        let bits = int32_arg(&args, 1)?;
        let n = value.len();
        let mut builder = StringBuilder::with_capacity(n, n * 64);
        for i in 0..n {
            if value.is_null(i) || bits.is_null(i) {
                builder.append_null();
                continue;
            }
            let bytes = value.value(i).as_bytes();
            let hex_str = match bits.value(i) {
                224 => hex::encode(Sha224::digest(bytes)),
                256 => hex::encode(Sha256::digest(bytes)),
                384 => hex::encode(Sha384::digest(bytes)),
                512 => hex::encode(Sha512::digest(bytes)),
                _ => {
                    builder.append_null();
                    continue;
                }
            };
            builder.append_value(hex_str);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- sha256(string) — alias for sha2(x, 256) -----------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha256Udf {
    signature: Signature,
}

impl Sha256Udf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for Sha256Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ppl_sha256" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use sha2::{Digest, Sha256};
        let arr = string_arg(&args, 0)?;
        let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 64);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                let mut hasher = Sha256::new();
                hasher.update(arr.value(i).as_bytes());
                builder.append_value(hex::encode(hasher.finalize()));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- cidrmatch(cidr, ip) --------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CidrMatchUdf {
    signature: Signature,
}

impl CidrMatchUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CidrMatchUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cidrmatch" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use ipnet::IpNet;
        use std::net::IpAddr;
        use std::str::FromStr;
        let cidr = string_arg(&args, 0)?;
        let ip = string_arg(&args, 1)?;
        let n = cidr.len();
        let mut builder = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if cidr.is_null(i) || ip.is_null(i) {
                builder.append_null();
                continue;
            }
            let net = IpNet::from_str(cidr.value(i));
            let addr = IpAddr::from_str(ip.value(i));
            match (net, addr) {
                (Ok(net), Ok(addr)) => builder.append_value(net.contains(&addr)),
                _ => builder.append_value(false),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_valid(string) ---------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonValidUdf {
    signature: Signature,
}

impl JsonValidUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonValidUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_valid" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arr = string_arg(&args, 0)?;
        let mut builder = BooleanBuilder::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                let valid = serde_json::from_str::<serde_json::Value>(arr.value(i)).is_ok();
                builder.append_value(valid);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_object(k, v, ...) -----------------------------------------------

/// Variadic key/value pairs → JSON object string. PPL emits the call with an
/// even number of operands; odd count → null.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObjectUdf {
    signature: Signature,
}

impl JsonObjectUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonObjectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_object" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arity = args.args.len();
        let mut builder = StringBuilder::with_capacity(n, n * 64);
        if arity % 2 != 0 {
            for _ in 0..n { builder.append_null(); }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
        }
        // Materialize each operand into a StringArray once.
        let mut cols: Vec<datafusion::arrow::array::StringArray> = Vec::with_capacity(arity);
        for i in 0..arity {
            cols.push(string_arg(&args, i)?);
        }
        for row in 0..n {
            let mut obj = serde_json::Map::new();
            let mut row_null = false;
            for pair in 0..(arity / 2) {
                let k = &cols[pair * 2];
                let v = &cols[pair * 2 + 1];
                if k.is_null(row) { row_null = true; break; }
                let key = k.value(row).to_string();
                let val = if v.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(v.value(row).to_string())
                };
                obj.insert(key, val);
            }
            if row_null {
                builder.append_null();
            } else {
                builder.append_value(serde_json::Value::Object(obj).to_string());
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}
