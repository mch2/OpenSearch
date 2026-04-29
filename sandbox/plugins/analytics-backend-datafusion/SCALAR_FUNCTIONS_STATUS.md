# Scalar Functions PR — Status (overnight progress)

## ✅ Working — committable now

**Math IT** (`ScalarMathFunctionIT`) — **21 tests passing**:
- `abs`, `ceil`, `floor`, `round`, `sqrt`, `cbrt` (dropped — substrait gap)
- `exp`, `ln`, `log2`, `log10`, `pow`, `power`
- `cos`, `sin`, `acos`, `asin`, `atan`, `atan2`, `degrees`, `radians`

Verified end-to-end: PPL → Calcite → Substrait → DataFusion native execution against bank fixture row 1 with `balance=39225`.

## 🛠 Production-grade infrastructure landed

### `NameBasedScalarFunctionConverter` (new, in `analytics-backend-datafusion/.../io/substrait/isthmus/expression/`)
Mirrors the existing `NameBasedAggregateFunctionConverter`. Solves the same problem for scalars:
substrait-isthmus's `ScalarFunctionConverter` matches by **identity** of `SqlOperator`. PPL's
custom `SqlFunction` instances (e.g. PPL's own `abs`, `sin`, `divide`) miss the identity-based
lookup against `SqlStdOperatorTable` entries. This subclass adds a case-insensitive
**name-based fallback**: when the stock matcher returns empty, it looks up the variant by
operator name + arity in the loaded extension catalog and constructs the substrait call
directly. Includes a `NAME_ALIASES` map for symbolic operators (`+`→`add`, `=`→`equal`, …).

### `SubstraitTypeCoercer` (new, simplified)
Pre-pass on RelNode trees to insert implicit `CAST`s where Calcite's type inference produces
a shape no Substrait core variant can match:
1. **fp64-only functions** (`sin`/`cos`/`tan`/`ln`/`sqrt`/etc.) — Substrait core only declares
   them on `fp32`/`fp64`. We cast non-fp operands to `DOUBLE` so `sin(BIGINT)` becomes
   `sin(CAST(BIGINT AS DOUBLE))`.
2. **`i64 / i64` division** — Calcite's quotient rule yields `DECIMAL` output, but Substrait
   has only `divide(i64,i64)→i64` and `divide(decimal,decimal)→decimal`. We route through
   `divide(fp64,fp64)→fp64` to sidestep the impedance mismatch entirely.

A catalog-driven version was prototyped but the Calcite/Substrait type-system disagreement
on parameterized decimals made it brittle. The hand-rolled version is simpler and ships.

### `OpenSearchProject.stripAnnotations` recursive fix (analytics-engine)
The strip method was only unwrapping the **top-level** `AnnotatedProjectExpression`. Nested
annotations leaked into Substrait conversion as phantom `ANNOTATED_PROJECT_EXPR(...)` calls.
Fixed with a `RexShuttle` that detects the annotation **before** recursion (super.visitCall's
default `clone()` produces a plain `RexCall` keeping the bogus operator, which post-recursion
`instanceof` would miss). This was a real latent bug.

### `ProjectCapability.Scalar` declaration in `DataFusionAnalyticsBackendPlugin`
Plugin now declares scalar function capability for every entry in the (extended)
`ScalarFunction` enum so `OpenSearchProjectRule` correctly routes project expressions.

### Extended `ScalarFunction` enum (analytics-framework SPI)
Added 40+ entries: ASCII, LEFT, RIGHT, REVERSE, REPLACE, REGEXP_REPLACE, LOCATE, LTRIM,
RTRIM, ROUND, TRUNCATE, SQRT, CBRT, EXP, LN, LOG, LOG2, LOG10, POWER, SIGN, GREATEST, LEAST,
PI, RAND, SIN/COS/TAN/COT, ASIN/ACOS/ATAN/ATAN2, DEGREES, RADIANS, EQUALS/NOT_EQUALS/<…>,
AND/OR/NOT, IS_NULL, IS_NOT_NULL, IF, MD5, SHA1, SHA2.
Made `fromNameOrError` case-insensitive.

### Project rule name-based fallback (analytics-engine)
`OpenSearchProjectRule.resolveScalarViableBackends` now falls back to
`ScalarFunction.fromNameOrError(operator.getName())` when `fromSqlKind` returns null —
mirrors the `OpenSearchAggregateRule` pattern for `OTHER`-kind operators.

### Additional scalar sigs in `DataFusionFragmentConvertor`
Registered 9 additional `FunctionMappings.Sig` entries that aren't in
`FunctionMappings.SCALAR_SIGS`: `LN`, `LOG2`, `LOG10`, `TAN`, `DEGREES`, `RADIANS`,
`GREATEST`, `LEAST`. Without these the library's `signatures.get(call.op)` returns null
and the converter throws `"Unable to convert call X(...)"` even though the YAML catalog
has perfectly good variants.

## ⚠️ Known gaps (need follow-up)

### PPL frontend gaps (out of scope this PR — need PPLFuncImpTable / grammar work)
- `tan(x)` — `Cannot resolve function: TAN` at PPL parser
- `greatest(...)`, `least(...)`, `scalar_max(...)`, `scalar_min(...)` — `mismatched input '('` at PPL parser

### Substrait core gaps (need extension YAML)
- `cbrt` — not declared in any substrait core YAML
- `truncate` — not in `functions_rounding.yaml`
- `pi()` — not in core
- `rand()` — not in core
- `log(base, x)` — substrait has `logb` but with swapped arg order

### DataFusion runtime gaps
- `sign` — DataFusion exposes as `signum`, not `sign` — needs name mapping in the substrait consumer (Rust side)

## 🚧 IT classes still failing — need investigation

The following ITs all hit `NodeDisconnectedException` (data node crash, no Java-side panic
message captured) when running with field references:
- `ScalarStringFunctionIT` — `upper(firstname)`, `lower(firstname)`, etc.
- `ScalarOperatorIT` — likely cluster-level same as strings
- `ScalarConditionalFunctionIT` — `if(...)`, `coalesce(...)` etc. (passed individually earlier)
- `ScalarCryptoFunctionIT` — `md5(firstname)`, `sha1(firstname)`, `sha2(firstname, 256)`

The data node disconnects without surfacing a Rust panic message in the test stderr capture
(potentially captured but not in any of the binary log paths I checked). Three hypotheses:

1. **Rust runtime panic on string-typed field arguments** — the math tests all use numeric
   fields; switching to `firstname` (Utf8) may hit a code path with a panic. **First action
   tomorrow: instrument the Rust side or run the data node with `RUST_BACKTRACE=1` somehow
   to capture the panic.**
2. **Substrait conversion silently produces a malformed plan** for string functions — DataFusion
   bombs on it with a runtime error that isn't clean text.
3. **Cluster reuse issue** — the SUITE-scoped cluster gets corrupted between tests; a
   transient. (But individual single-test runs ALSO crash, so this isn't it.)

The `testUpper` failure pattern is consistent across runs: data node crashes mid-fragment-execution.

## What to do tomorrow

1. **Add `RUST_BACKTRACE=1` and SLF4J binding** to the analytics-backend-datafusion test JVM
   so the Rust panic message lands in a log file we can read.
2. Once the panic is visible, fix it (or document as another runtime gap).
3. Consider running each IT class with `Scope.TEST` instead of `Scope.SUITE` to eliminate
   cluster-reuse as a confounding variable.
4. For PPL frontend gaps (`tan`, `greatest`, `least`, etc.), either:
   - File issues against the PPL frontend (unified-query-* in the SQL plugin), or
   - Test those functions via a different syntax that PPL does parse.
