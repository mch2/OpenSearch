/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `hll_estimate(state: Binary) -> Int64` — cardinality estimate from a serialized
//! HyperLogLog sketch.
//!
//! Used as the finalize expression for {@code APPROX_COUNT_DISTINCT} in the
//! shard-bucket-oversampling rule's expression-based shard sort key. The shard's
//! intermediate-state Aggregate emits the HLL sketch as `Binary` per group; the
//! shard-local top-K Sort orders by `hll_estimate(state)` to truncate to per-shard
//! top-K before shipping the state to the coordinator.
//!
//! Byte format compatibility: the sketch is the same 16384-byte register array
//! produced by DataFusion's built-in `approx_distinct` UDAF (see
//! `datafusion-functions-aggregate-X.Y.Z/src/hyperloglog.rs`). The `count()` formula
//! is the parameterless register-based estimator from
//! "New cardinality estimation algorithms for HyperLogLog sketches"
//! (Otmar Ertl, 2017, arXiv:1702.01284) — the same algorithm DataFusion uses, so
//! the per-shard estimate this UDF returns is bit-for-bit identical to what
//! coord-side `approx_distinct(state)` would produce as a single-row final.
//!
//! The constants `HLL_P=14`, `HLL_Q=64-14=50`, `NUM_REGISTERS=16384` mirror
//! DataFusion's sketch shape; if upstream DataFusion ever changes precision, the
//! shape mismatch is caught here as an `exec_err!` rather than producing a wrong
//! count.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

const HLL_P: usize = 14;
const HLL_Q: usize = 64 - HLL_P;
const NUM_REGISTERS: usize = 1 << HLL_P;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(HllEstimateUdf::new()));
}

/// `hll_estimate(binary)` → `i64`.
#[derive(Debug)]
pub struct HllEstimateUdf {
    signature: Signature,
}

impl HllEstimateUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

super::udf_identity!(HllEstimateUdf, "hll_estimate");

impl ScalarUDFImpl for HllEstimateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hll_estimate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("hll_estimate expects exactly 1 argument, got {}", args.args.len());
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(opt) | ScalarValue::LargeBinary(opt)) => {
                let estimate = opt.as_ref().map(|bytes| count_from_registers(bytes)).transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(estimate)))
            }
            ColumnarValue::Scalar(other) => exec_err!("hll_estimate: expected Binary input, got {other:?}"),
            ColumnarValue::Array(arr) => {
                let mut builder = Int64Builder::with_capacity(arr.len());
                let bin = arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| datafusion::common::DataFusionError::Execution("hll_estimate: expected BinaryArray".to_string()))?;
                for i in 0..bin.len() {
                    if bin.is_null(i) {
                        builder.append_null();
                    } else {
                        let bytes = bin.value(i);
                        let estimate = count_from_registers(bytes)?;
                        builder.append_value(estimate);
                    }
                }
                let out: Int64Array = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

/// Estimates cardinality from the 16384-byte HLL register array using the Ertl-2017
/// estimator. Constant-time (no allocations beyond the histogram).
fn count_from_registers(bytes: &[u8]) -> Result<i64> {
    if bytes.len() != NUM_REGISTERS {
        return exec_err!(
            "hll_estimate: expected {} register bytes (HLL_P={}), got {}",
            NUM_REGISTERS,
            HLL_P,
            bytes.len()
        );
    }
    // Histogram of register values. Each value is in 0..=HLL_Q+1 so the histogram
    // size is HLL_Q+2 (=52). u32 is enough because we only have 16384 registers.
    let mut histogram = [0u32; HLL_Q + 2];
    for &r in bytes {
        let idx = r as usize;
        if idx >= histogram.len() {
            // Defensive: a sketch byte outside [0, HLL_Q+1] indicates corruption.
            return exec_err!(
                "hll_estimate: register byte {} out of range [0, {}]",
                r,
                HLL_Q + 1
            );
        }
        histogram[idx] += 1;
    }
    let m = NUM_REGISTERS as f64;
    let mut z = m * hll_tau((m - histogram[HLL_Q + 1] as f64) / m);
    for i in histogram[1..=HLL_Q].iter().rev() {
        z += *i as f64;
        z *= 0.5;
    }
    z += m * hll_sigma(histogram[0] as f64 / m);
    let estimate = (0.5 / 2_f64.ln() * m * m / z).round() as i64;
    Ok(estimate)
}

/// Helper sigma per Ertl-2017 (mirrors DataFusion's hyperloglog.rs).
#[inline]
fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        f64::INFINITY
    } else {
        let mut y = 1.0;
        let mut z = x;
        let mut x = x;
        loop {
            x *= x;
            let z_prime = z;
            z += x * y;
            y += y;
            if z_prime == z {
                break;
            }
        }
        z
    }
}

/// Helper tau per Ertl-2017 (mirrors DataFusion's hyperloglog.rs).
#[inline]
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        0.0
    } else {
        let mut y = 1.0;
        let mut z = 1.0 - x;
        let mut x = x;
        loop {
            x = x.sqrt();
            let z_prime = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, Int64Array};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> HllEstimateUdf {
        HllEstimateUdf::new()
    }

    fn invoke_scalar(value: ScalarValue) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn as_int64(v: ColumnarValue) -> Option<i64> {
        match v {
            ColumnarValue::Scalar(ScalarValue::Int64(opt)) => opt,
            other => panic!("expected Int64 scalar, got {other:?}"),
        }
    }

    /// Empty registers (all zero) → estimate 0.
    #[test]
    fn empty_registers_yield_zero() {
        let bytes = vec![0u8; NUM_REGISTERS];
        let out = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap();
        assert_eq!(as_int64(out).unwrap(), 0);
    }

    /// Null input yields null.
    #[test]
    fn null_input_yields_null() {
        let out = invoke_scalar(ScalarValue::Binary(None)).unwrap();
        assert!(as_int64(out).is_none());
    }

    /// Wrong-size input is rejected with a structured error.
    #[test]
    fn wrong_size_rejected() {
        let bytes = vec![0u8; 100];
        let err = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap_err();
        assert!(err.to_string().contains("expected 16384 register bytes"));
    }

    /// All-saturated registers (every register = HLL_Q+1) — produces a finite,
    /// large estimate. This is the "every register saw a high-leading-zero hash"
    /// degenerate case; the formula handles it.
    #[test]
    fn saturated_registers_finite() {
        let bytes = vec![(HLL_Q + 1) as u8; NUM_REGISTERS];
        let out = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap();
        let n = as_int64(out).unwrap();
        // Just verify it didn't blow up / panic / return negative.
        assert!(n >= 0);
    }

    /// Random-but-bounded register values produce a positive estimate consistent
    /// with the formula. Sanity check: setting every other register to 1 should
    /// give a non-zero, modest estimate.
    #[test]
    fn half_one_registers_positive_estimate() {
        let mut bytes = vec![0u8; NUM_REGISTERS];
        for i in (0..NUM_REGISTERS).step_by(2) {
            bytes[i] = 1;
        }
        let out = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap();
        let n = as_int64(out).unwrap();
        assert!(n > 0, "expected positive estimate, got {n}");
        assert!(n < 1_000_000, "estimate {n} is implausibly large for half-one registers");
    }

    /// Array invocation path returns one estimate per row.
    #[test]
    fn array_invocation_per_row() {
        let mut row0 = vec![0u8; NUM_REGISTERS];
        row0[0] = 5;
        let row1 = vec![0u8; NUM_REGISTERS];
        let arr = BinaryArray::from(vec![Some(row0.as_slice()), Some(row1.as_slice()), None]);
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr) as ArrayRef)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        let out = u.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Array(a) => {
                let arr = a.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(arr.len(), 3);
                assert!(arr.value(0) > 0);
                assert_eq!(arr.value(1), 0);
                assert!(arr.is_null(2));
            }
            other => panic!("expected Array result, got {other:?}"),
        }
    }
}
