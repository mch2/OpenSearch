/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `convert_tz(ts, from_tz, to_tz)` — shift a timestamp from one timezone to another.
//!
//! Rationale (UDF over decomposition): DST transitions produce offset values that
//! vary per-row (same timezone, different instants → different offsets). Operator
//! decomposition would need runtime per-row offset lookup against the IANA
//! database, which isthmus has no primitive for. The `chrono-tz` crate embeds
//! the zoneinfo tables, so the UDF can resolve DST-correctly in one shot.
//!
//! Semantics (MySQL-compatible):
//! * `ts` is interpreted as a wall-clock time in `from_tz`.
//! * The return is the wall-clock time in `to_tz` for the same instant.
//! * Timezone strings may be IANA names (`'America/New_York'`) or ISO offsets
//!   of the form `±HH:MM`.
//! * Any null input → null output (null propagation).
//! * Unparseable timezone → null output (matches MySQL's `CONVERT_TZ` lenient
//!   behaviour rather than throwing — keeps batch processing tolerant).

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, StringArray, TimestampMillisecondArray, TimestampMillisecondBuilder,
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
    ctx.register_udf(ScalarUDF::from(ConvertTzUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTzUdf {
    signature: Signature,
}

impl ConvertTzUdf {
    pub fn new() -> Self {
        // PPL emits `convert_tz(ts, from, to)` with ts typed as Utf8 (string
        // literal), Date32, or Timestamp(any precision, any tz). Signature::exact
        // only let through the Timestamp(Ms, None) variant, so planning rejected
        // the Utf8 case (CalciteConvertTZFunctionIT dropped from 7 passes to 0 at
        // Phase 2 audit — see tasks/reports/phase-2-consolidation-audit.md).
        // user_defined + coerce_types lets DF insert the right casts.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ConvertTzUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ConvertTzUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "convert_tz"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("convert_tz expects 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "convert_tz",
            arg_types,
            &[CoerceMode::TimestampMs, CoerceMode::Utf8, CoerceMode::Utf8],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("convert_tz expects 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let ts = args.args[0].clone().into_array(n)?;
        let from = args.args[1].clone().into_array(n)?;
        let to = args.args[2].clone().into_array(n)?;

        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "convert_tz: expected TimestampMillisecond, got {:?}",
                    ts.data_type()
                ))
            })?;
        let from = from
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "convert_tz: from_tz expected Utf8, got {:?}",
                    from.data_type()
                ))
            })?;
        let to = to.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "convert_tz: to_tz expected Utf8, got {:?}",
                to.data_type()
            ))
        })?;

        let mut builder = TimestampMillisecondBuilder::with_capacity(n);
        for i in 0..n {
            if ts.is_null(i) || from.is_null(i) || to.is_null(i) {
                builder.append_null();
                continue;
            }
            match shift_millis(ts.value(i), from.value(i), to.value(i)) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Parse timezone string (IANA name or `±HH:MM` offset).
enum TzSpec {
    Iana(Tz),
    /// Fixed offset in seconds east of UTC.
    Offset(i32),
}

fn parse_tz(s: &str) -> Option<TzSpec> {
    if let Some(off) = parse_offset_seconds(s) {
        return Some(TzSpec::Offset(off));
    }
    s.parse::<Tz>().ok().map(TzSpec::Iana)
}

/// Parse `±HH:MM` → seconds east of UTC; None if not an offset literal.
fn parse_offset_seconds(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    if bytes[3] != b':' {
        return None;
    }
    let hours: i32 = s.get(1..3)?.parse().ok()?;
    let minutes: i32 = s.get(4..6)?.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(sign * (hours * 3600 + minutes * 60))
}

/// The stored timestamp has no tz attached — interpret its wall clock in
/// `from_tz`, render that instant in `to_tz`, then return the shifted millis as
/// a tz-free value the caller can continue to treat as naive. The shift is
/// exactly `to_offset(ts) - from_offset(ts)` milliseconds — seconds
/// cancel out once both sides agree on the instant.
fn shift_millis(ts_millis: i64, from_tz: &str, to_tz: &str) -> Option<i64> {
    let from = parse_tz(from_tz)?;
    let to = parse_tz(to_tz)?;
    // Treat the stored millis as a naive wall-clock in `from_tz`.
    let naive = DateTime::<Utc>::from_timestamp_millis(ts_millis)?.naive_utc();
    let from_off = offset_seconds_at(&from, &naive)?;
    let to_off = offset_seconds_at_instant(&to, ts_millis, from_off)?;
    let delta_millis = (to_off - from_off) as i64 * 1_000;
    ts_millis.checked_add(delta_millis)
}

/// Offset (seconds east of UTC) for `from_tz` at wall-clock `naive`.
fn offset_seconds_at(tz: &TzSpec, naive: &NaiveDateTime) -> Option<i32> {
    match tz {
        TzSpec::Offset(o) => Some(*o),
        TzSpec::Iana(z) => {
            // Use .from_local_datetime → pick the earliest resolution for ambiguous
            // (DST-fall-back) wall times, which matches MySQL's behaviour.
            match z.from_local_datetime(naive) {
                chrono::LocalResult::Single(dt) => Some(dt.offset().fix().local_minus_utc()),
                chrono::LocalResult::Ambiguous(dt, _) => Some(dt.offset().fix().local_minus_utc()),
                chrono::LocalResult::None => None, // wall time in the DST "spring-forward" gap
            }
        }
    }
}

/// Offset (seconds east of UTC) for `to_tz` at the UTC *instant* represented by
/// the input. We reconstruct the instant from `ts_millis` + `from_offset` (since
/// `ts_millis` is a wall clock in from_tz), then look up to_tz's offset at that
/// instant — DST-correct even across transitions.
fn offset_seconds_at_instant(
    tz: &TzSpec,
    ts_millis: i64,
    from_offset_seconds: i32,
) -> Option<i32> {
    match tz {
        TzSpec::Offset(o) => Some(*o),
        TzSpec::Iana(z) => {
            // instant_utc_millis = wall_millis - from_offset_millis
            let instant_millis =
                ts_millis.checked_sub((from_offset_seconds as i64) * 1_000)?;
            let instant = DateTime::<Utc>::from_timestamp_millis(instant_millis)?;
            Some(z.offset_from_utc_datetime(&instant.naive_utc()).fix().local_minus_utc())
        }
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ±HH:MM offsets parse to the expected second counts.
    #[test]
    fn parse_offset_accepts_positive_and_negative() {
        assert_eq!(parse_offset_seconds("+00:00"), Some(0));
        assert_eq!(parse_offset_seconds("+05:30"), Some(5 * 3600 + 30 * 60));
        assert_eq!(parse_offset_seconds("-08:00"), Some(-8 * 3600));
        assert_eq!(parse_offset_seconds("+14:00"), Some(14 * 3600));
    }

    #[test]
    fn parse_offset_rejects_malformed() {
        assert_eq!(parse_offset_seconds("bogus"), None);
        assert_eq!(parse_offset_seconds("0500"), None);
        assert_eq!(parse_offset_seconds("+24:00"), None);
        assert_eq!(parse_offset_seconds("+05:60"), None);
    }

    // Offset → offset: simple wall-clock delta, no calendar.
    #[test]
    fn fixed_offset_to_fixed_offset_shifts_by_delta() {
        // 2024-01-05T12:00:00 in +00:00 → same wall clock in +05:30 means
        // +5h30m = +19_800_000 ms added.
        let ts = 1_704_456_000_000; // 2024-01-05T12:00:00Z (stored naive)
        let out = shift_millis(ts, "+00:00", "+05:30").unwrap();
        assert_eq!(out - ts, 5 * 3600 * 1000 + 30 * 60 * 1000);
    }

    // IANA ↔ IANA: DST-correct jump across a transition.
    #[test]
    fn iana_new_york_to_london_applies_correct_offset() {
        // 2024-01-05T12:00:00 wall-clock in America/New_York (UTC-5 in winter)
        // → 17:00 UTC → London (UTC+0 in winter) = 17:00 local. Delta = +5h.
        let ts = 1_704_456_000_000; // treat as 2024-01-05T12:00:00 naive
        let out = shift_millis(ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((out - ts) / 1000, 5 * 3600);
    }

    #[test]
    fn iana_dst_summer_offset_differs_from_winter() {
        // Summer: NY is UTC-4, winter: NY is UTC-5. Pull data at both dates,
        // confirm the two shifts to UTC (London+0 in winter, +1 in summer) produce
        // the expected distinct deltas.
        // 2024-01-05T12:00:00 (winter): NY→London → +5h.
        let winter_ts = 1_704_456_000_000;
        let winter_out = shift_millis(winter_ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((winter_out - winter_ts) / 1000, 5 * 3600);
        // 2024-07-05T12:00:00 (summer): NY (UTC-4) → London (UTC+1) → +5h.
        // Same delta because both shift to/from their summer offsets in lockstep.
        let summer_ts = 1_720_180_800_000; // 2024-07-05T12:00:00Z naive
        let summer_out = shift_millis(summer_ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((summer_out - summer_ts) / 1000, 5 * 3600);
    }

    // When from_tz crosses DST boundary but to_tz doesn't, the delta changes.
    #[test]
    fn iana_to_utc_crosses_dst_in_source_tz() {
        // 2024-01-05 in UTC (no DST there): NY winter = UTC-5, shift = +5h.
        let winter_ts = 1_704_456_000_000;
        let winter_out = shift_millis(winter_ts, "America/New_York", "UTC").unwrap();
        assert_eq!((winter_out - winter_ts) / 1000, 5 * 3600);

        // 2024-07-05: NY summer = UTC-4, shift = +4h.
        let summer_ts = 1_720_180_800_000;
        let summer_out = shift_millis(summer_ts, "America/New_York", "UTC").unwrap();
        assert_eq!((summer_out - summer_ts) / 1000, 4 * 3600);
    }

    #[test]
    fn unknown_tz_returns_none() {
        assert_eq!(shift_millis(0, "Not/AZone", "UTC"), None);
        assert_eq!(shift_millis(0, "UTC", "Not/AZone"), None);
    }

    // Coercion: PPL may emit the ts arg as Utf8 (string literal), Date32,
    // or Timestamp with a different precision/tz. coerce_types should
    // normalize them all to Timestamp(Millisecond, None) + Utf8 + Utf8.
    #[test]
    fn coerce_types_accepts_utf8_ts() {
        let udf = ConvertTzUdf::new();
        let out = udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
                DataType::Utf8,
            ]
        );
    }

    #[test]
    fn coerce_types_accepts_date32_ts() {
        let udf = ConvertTzUdf::new();
        let out = udf
            .coerce_types(&[DataType::Date32, DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(out[0], DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn coerce_types_accepts_other_ts_precisions() {
        let udf = ConvertTzUdf::new();
        // Nanosecond with tz → should coerce down to Millisecond, None.
        let out = udf
            .coerce_types(&[
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                DataType::Utf8,
                DataType::Utf8,
            ])
            .unwrap();
        assert_eq!(out[0], DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn coerce_types_passes_through_exact_match() {
        let udf = ConvertTzUdf::new();
        let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
        let out = udf
            .coerce_types(&[ts.clone(), DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(out, vec![ts, DataType::Utf8, DataType::Utf8]);
    }

    #[test]
    fn coerce_types_rejects_unsupported_ts_type() {
        let udf = ConvertTzUdf::new();
        // A boolean in the ts slot is clearly wrong — must error explicitly.
        let err = udf
            .coerce_types(&[DataType::Boolean, DataType::Utf8, DataType::Utf8])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("convert_tz") && msg.contains("Boolean"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = ConvertTzUdf::new();
        assert!(udf.coerce_types(&[DataType::Utf8]).is_err());
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .is_err());
    }

    // Batch / null handling through the full UDF.
    #[test]
    fn invoke_nulls_and_bad_tz_propagate() {
        let udf = ConvertTzUdf::new();
        let ts = TimestampMillisecondArray::from(vec![
            Some(1_704_456_000_000),
            None,
            Some(0),
        ]);
        let from = StringArray::from(vec![
            Some("+00:00"),
            Some("UTC"),
            Some("Mars/Olympus"), // unknown → null
        ]);
        let to = StringArray::from(vec![Some("+05:30"), Some("UTC"), Some("UTC")]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(ts)),
                ColumnarValue::Array(Arc::new(from)),
                ColumnarValue::Array(Arc::new(to)),
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
            _ => panic!("expected array"),
        };
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}
