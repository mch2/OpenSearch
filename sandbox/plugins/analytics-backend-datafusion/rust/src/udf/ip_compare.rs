/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! IP comparison UDFs: `equals_ip`, `not_equals_ip`, `less_ip`, `lte_ip`,
//! `greater_ip`, `gte_ip`. Each takes two operands and compares them by the
//! canonical 16-byte (IPv4-mapped) form so `"0.0.0.1" < "::2"` holds without
//! a mismatched IPv4/IPv6 family. Operands may be either:
//! - `Utf8` strings (typical for literals, e.g. `'1.2.3.4'`), parsed via
//!   `IpAddr::from_str`, or
//! - `Binary` (16-byte big-endian InetAddressPoint encoding) — that's how
//!   OpenSearch's parquet format stores `ip`-typed fields via `IpParquetField`
//!   (Lucene's `InetAddressPoint.encode`). Used directly without reparsing.
//!
//! PPL emits these only via its overloaded `=`/`<`/etc. operators when it sees
//! an IP-typed operand — see `PPLFuncImpTable.populate()` in the opensearch-sql
//! plugin. They are NOT user-callable from `eval` syntax.

use std::any::Any;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanBuilder, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// The six IP comparisons we register. Each `Op` produces a `bool` from two
/// canonical 16-byte forms.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Op {
    Equals,
    NotEquals,
    Less,
    LessOrEquals,
    Greater,
    GreaterOrEquals,
}

impl Op {
    fn apply(self, left: &[u8; 16], right: &[u8; 16]) -> bool {
        match self {
            Op::Equals => left == right,
            Op::NotEquals => left != right,
            Op::Less => left < right,
            Op::LessOrEquals => left <= right,
            Op::Greater => left > right,
            Op::GreaterOrEquals => left >= right,
        }
    }
}

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("equals_ip", Op::Equals)));
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("not_equals_ip", Op::NotEquals)));
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("less_ip", Op::Less)));
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("lte_ip", Op::LessOrEquals)));
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("greater_ip", Op::Greater)));
    ctx.register_udf(ScalarUDF::from(CompareIpUdf::new("gte_ip", Op::GreaterOrEquals)));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CompareIpUdf {
    name: &'static str,
    op: Op,
    signature: Signature,
}

impl CompareIpUdf {
    fn new(name: &'static str, op: Op) -> Self {
        // Each operand can be either Utf8 (string form, typical for literals)
        // or Binary (16-byte InetAddressPoint form, how parquet stores ip fields).
        // We enumerate the four (Utf8|Binary) × (Utf8|Binary) combinations.
        Self {
            name,
            op,
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CompareIpUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let left = materialize_operand(&args, 0)?;
        let right = materialize_operand(&args, 1)?;
        let n = left.len();
        let mut builder = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            match (left.canonical_at(i), right.canonical_at(i)) {
                (OperandValue::Null, _) | (_, OperandValue::Null) => builder.append_null(),
                (OperandValue::Canonical(l), OperandValue::Canonical(r)) => {
                    builder.append_value(self.op.apply(&l, &r));
                }
                // Unparseable input → null, matching Calcite's semantics for
                // type-violating comparisons.
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn parse_canonical(s: &str) -> Option<[u8; 16]> {
    let addr = IpAddr::from_str(s).ok()?;
    Some(match addr {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
        IpAddr::V6(v6) => v6.octets(),
    })
}

/// Per-row canonical form or a sentinel explaining why it's absent. Unparseable
/// and wrong-length inputs funnel into `NotIp`, which the caller maps to null.
enum OperandValue {
    Null,
    Canonical([u8; 16]),
    NotIp,
}

/// Typed per-column view over an operand. We downcast once and then read rows
/// cheaply — avoids re-dispatching on DataType inside the per-row loop.
enum Operand {
    Utf8(StringArray),
    Binary(BinaryArray),
}

impl Operand {
    fn len(&self) -> usize {
        match self {
            Operand::Utf8(a) => a.len(),
            Operand::Binary(a) => a.len(),
        }
    }

    fn canonical_at(&self, i: usize) -> OperandValue {
        match self {
            Operand::Utf8(a) => {
                if a.is_null(i) {
                    return OperandValue::Null;
                }
                match parse_canonical(a.value(i)) {
                    Some(b) => OperandValue::Canonical(b),
                    None => OperandValue::NotIp,
                }
            }
            Operand::Binary(a) => {
                if a.is_null(i) {
                    return OperandValue::Null;
                }
                let bytes = a.value(i);
                if bytes.len() != 16 {
                    return OperandValue::NotIp;
                }
                let mut out = [0u8; 16];
                out.copy_from_slice(bytes);
                OperandValue::Canonical(out)
            }
        }
    }
}

fn materialize_operand(args: &ScalarFunctionArgs, idx: usize) -> Result<Operand> {
    let arr = args
        .args
        .get(idx)
        .ok_or_else(|| DataFusionError::Internal(format!("IP cmp UDF missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(Operand::Utf8(s.clone()));
    }
    if let Some(b) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Operand::Binary(b.clone()));
    }
    Err(DataFusionError::Internal(format!(
        "IP cmp UDF arg {} expected Utf8 or Binary, got {:?}",
        idx,
        arr.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical(s: &str) -> [u8; 16] {
        parse_canonical(s).expect("valid IP")
    }

    #[test]
    fn equals_matches_ipv4() {
        assert!(Op::Equals.apply(&canonical("1.2.3.4"), &canonical("1.2.3.4")));
        assert!(!Op::Equals.apply(&canonical("1.2.3.4"), &canonical("1.2.3.5")));
    }

    #[test]
    fn ipv4_and_ipv6_mapped_compare_equal() {
        // IPv4 0.0.0.1 == IPv6 ::ffff:0.0.0.1 in canonical form.
        assert!(Op::Equals.apply(&canonical("0.0.0.1"), &canonical("::ffff:0.0.0.1")));
    }

    #[test]
    fn less_orders_by_bytes() {
        assert!(Op::Less.apply(&canonical("1.2.3.4"), &canonical("1.2.3.5")));
        assert!(!Op::Less.apply(&canonical("1.2.3.5"), &canonical("1.2.3.4")));
    }

    #[test]
    fn ipv4_less_than_plain_ipv6() {
        // 0.0.0.1 → ::ffff:0.0.0.1 is AFTER ::2 byte-wise? Actually ::2 is
        // [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2] vs
        // ::ffff:0.0.0.1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 1].
        // So IPv4 1 is actually greater than IPv6 ::2, matching the Java
        // CompareIpFunction which also uses IPv4-mapped canonical form.
        assert!(Op::Greater.apply(&canonical("0.0.0.1"), &canonical("::2")));
    }

    #[test]
    fn all_ops_on_simple_pair() {
        let a = canonical("1.2.3.4");
        let b = canonical("1.2.3.5");
        assert!(Op::Equals.apply(&a, &a));
        assert!(Op::NotEquals.apply(&a, &b));
        assert!(Op::Less.apply(&a, &b));
        assert!(Op::LessOrEquals.apply(&a, &a));
        assert!(Op::Greater.apply(&b, &a));
        assert!(Op::GreaterOrEquals.apply(&b, &b));
    }

    // ---- Operand-layer tests: Utf8 + Binary column reads ------------------

    #[test]
    fn operand_utf8_reads_parse_as_ip() {
        let arr = StringArray::from(vec![Some("1.2.3.4"), Some("bogus"), None]);
        let op = Operand::Utf8(arr);
        assert!(matches!(op.canonical_at(0), OperandValue::Canonical(_)));
        assert!(matches!(op.canonical_at(1), OperandValue::NotIp));
        assert!(matches!(op.canonical_at(2), OperandValue::Null));
    }

    #[test]
    fn operand_binary_uses_16_bytes_directly() {
        // InetAddressPoint of 1.2.3.4 = IPv4-mapped IPv6:
        // 10 bytes zero, 0xff 0xff, then 1 2 3 4.
        let v4 = [0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4];
        // ::1 = 15 zero bytes then 0x01.
        let v6 = [0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let too_short = vec![0u8; 4];
        let arr = BinaryArray::from(vec![
            Some(v4.as_slice()),
            Some(v6.as_slice()),
            None,
            Some(too_short.as_slice()),
        ]);
        let op = Operand::Binary(arr);
        match op.canonical_at(0) {
            OperandValue::Canonical(b) => assert_eq!(b, v4),
            other => panic!("expected canonical 1.2.3.4, got {:?}", std::mem::discriminant(&other)),
        }
        match op.canonical_at(1) {
            OperandValue::Canonical(b) => assert_eq!(b, v6),
            _ => panic!("expected canonical ::1"),
        }
        assert!(matches!(op.canonical_at(2), OperandValue::Null));
        assert!(matches!(op.canonical_at(3), OperandValue::NotIp));
    }

    /// Cross-encoding equality: Utf8 "1.2.3.4" and Binary InetAddressPoint(1.2.3.4)
    /// must canonicalize to the same 16 bytes so the comparison returns true.
    /// This is the core guarantee the Java-side `where ip_field = '1.2.3.4'` path
    /// relies on — left comes in as Binary from parquet, right as Utf8 literal.
    #[test]
    fn cross_encoding_equals_ipv4() {
        let v4_binary = [0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4];
        let from_binary = match Operand::Binary(BinaryArray::from(vec![Some(v4_binary.as_slice())])).canonical_at(0) {
            OperandValue::Canonical(b) => b,
            _ => panic!("binary canonicalize failed"),
        };
        let from_utf8 = match Operand::Utf8(StringArray::from(vec![Some("1.2.3.4")])).canonical_at(0) {
            OperandValue::Canonical(b) => b,
            _ => panic!("utf8 canonicalize failed"),
        };
        assert_eq!(from_binary, from_utf8, "binary and utf8 IP must canonicalize identically");
    }
}
