# Overnight Reproduction Summary (read me first)

**Goal:** reproduce the ~229 failing upstream `opensearch-project/sql` Calcite-remote tests against
our sandbox/qa analytics-engine REST suite (`/_plugins/_ppl`, parquet primary + lucene secondary,
`cluster.pluggable.dataformat=composite`), confirm each fails the same way, and bucket the root cause.

**No code was pushed anywhere.** All work is local repro ITs under
`sandbox/qa/analytics-engine-rest/src/test/java/org/opensearch/analytics/qa/Calcite*ReproIT.java`
plus datasets under `.../resources/datasets/`. Tracker lives in this `.tracker/` dir.

## Environment notes (important)
- Build needs **JDK 25**: `~/.sdkman/candidates/java/25.0.3-amzn`.
- The Rust native lib (`libopensearch_native.so`) on disk was **stale** (missing
  `df_stream_get_metrics`) so the cluster wouldn't boot. Rebuilt it. The build needs a **modern
  protoc** — system `/usr/bin/protoc` is 2.5.0 (proto2 only) and fails the substrait build script.
  Use `PROTOC=/local/home/handalm/.local/protoc/bin/protoc` (25.1) on PATH.
- Run cmd:
  ```
  JAVA_HOME=~/.sdkman/candidates/java/25.0.3-amzn PROTOC=/local/home/handalm/.local/protoc/bin/protoc \
  PATH=/local/home/handalm/.local/protoc/bin:$PATH \
  ./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true --tests "...ReproIT"
  ```
  Cluster boot is the bottleneck (~2-3 min/run) — batch many `--tests` per invocation.

## Progress (latest)
- **82** tests root-caused + bucketed (🔴), **32** pass in sandbox/qa (⚪ — already fixed on this
  branch, or the upstream failure had a different cause than the asserted query), **115** not yet
  ported (⬜). **114 of 229 reproduced/resolved; 25 root-cause buckets (A–Y).**
- See `PROGRESS.tsv` for the per-test table and `FAILURES.md` for full bucket descriptions.
- Repro ITs authored (all under `.../qa/Calcite*ReproIT.java`): PPLBasic, ConditionBuiltinFunction,
  EvalMaxMinFunction, DataType, EnhancedCoalesce, Operator, ParseRexError, BuiltinFunction,
  MultiValueStats, AppendPipeCommand, Sort, StatsCommand, StringMath, InSubquery, CastFunction,
  Misc (Rename/System/Settings/CaseAgg), Eventstats, DateTimeFunction, Aggregation, Json.
- Shared base: `CalciteReproTestCase` (matcher-style verifySchema/verifyDataRows/schema/rows on the
  Map-based executePpl; inline parquet index helpers). Datasets added under `resources/datasets/`:
  bank, bank_null, account, dog, null_missing, datatypes_numeric, datatypes_nonnumeric (geo_point
  removed — see bucket E), state_country, state_country_null, occupation, worker, work_information,
  date_formats, weblogs, date_kw, nested_simple.

## Root-cause buckets found so far (A–R)
| Bucket | One-liner | # confirmed |
|--------|-----------|-------------|
| A | Full-table `source=idx` schema columns come out **alphabetical**, not mapping-declaration order (explicit `\| fields` is fine) | 9 |
| B | Unsuffixed decimal literal (`7.0`) and half_float widen to **float32**, not double → precision loss | 4 |
| C | Cross-index `source=a,b` with same field at **different numeric/typed widths** (int vs long, text vs boolean) → HTTP 500, no widening | 4 |
| D | **Implicit** post-source filter (no `where` kw): `source=idx age=32` → 0 rows; `not age>32` → unfiltered | 8 |
| E | AE parquet format **rejects `geo_point`** (and likely other) field types at index creation | 1 |
| F | Scalar `eval max()/min()` (GREATEST/LEAST) → `undefined` type or 500 "Cannot infer return type" | 8 |
| G | `coalesce()` of all-missing fields → 500 instead of graceful `undefined` null | 1 |
| H | `parse`/`rex` invalid capture-group → 500 + missing "must be alphanumeric" suggestion (upstream 4xx) | 8 |
| I | `list/values(boolean)` renders `TRUE`/`FALSE` not `true`/`false` | 2 |
| J | `values()` doesn't dedup / sort / honor `plugins.ppl.values.max.limit` | 5 |
| K | `list()/values()` keep null/empty elements instead of dropping | 2 |
| L | `scaled_float` typed as `bigint`, not `double` | 1 |
| M | `DELETE /idx/_doc/N` on parquet index → 500 (deletes unsupported) | 1 |
| N | `appendpipe [ where ... ]` → 500 "delegation_possible UDF body invoked" (filter-delegation marker leak) | 1 |
| O | bare `object` field flattened to dotted leaf cols, not kept as `array`/`struct` | 1 |
| P | `sort` tie-break order differs on equal keys (maybe nondeterministic) | 2 |
| Q | `percentile()` is interpolated `double`, not discrete `bigint`; empty bucket `null` vs `0` | 4 |
| R | Unsupported scalar UDFs: `rand()`, `conv()`, `adddate()`, `subdate()` → 400/500 `No backend supports scalar function [X]` | 4 |
| S | `cast(x as IP)` unsupported → 500 `No backend supports scalar function [IP]` | 1 |
| T | aliased (`as o`/`as i`) and correlated IN-subqueries return wrong/0 rows (plain IN-subquery works) | 2 |
| U | range-`case` agg keeps the null/"unknown" bucket where upstream pushdown drops it | 1 |
| V | approx aggregations unsupported: `distinct_count_approx` → 500 `APPROX_COUNT_DISTINCT` unbound; `perc50/p95` shortcuts → 500 | 5 |
| W | JSON builtins: `json()`/`json_append`/`json_extend` → 500; `json_set`/`json_delete` with `$.path` are no-ops | 5 |
| X | `multisearch [..A..] [..B..]` row count off-by-one (52 vs 51) | 1 |
| Y | filter on nested-object subfield (`where address.city = '...'`) returns 0 rows | 1 |

### Highest-leverage buckets (fix these first — broadest blast radius)
- **A** (full-table schema column order alphabetical) and **D** (implicit post-source filter) each
  cause the most cross-class failures and are likely single root causes in the planner/parser.
- **R/S/V** (unsupported scalar/agg functions) are a long tail of "add a DataFusion binding" gaps;
  each function is independent but the fix pattern is uniform.

## How to continue
1. Port remaining ⬜ classes (datasets already created for most: bank, account, bank_null, dog,
   null_missing, datatypes_numeric/nonnumeric, state_country(_null), calcs, occupation, worker,
   work_information, date_formats, weblogs). Remaining datasets still needed: events, logs, telemetry,
   time_test_data(+2), locations_type_conflict, json_test, hdfs_logs, array, strings, wildcard,
   game_of_thrones, people2, nested_simple/deep_nested/cascaded_nested, duplication_nullable,
   bank_csv_sanitize, click_bench, otel_logs, events_null.
2. One repro IT per upstream class; copy query + expected schema/rows verbatim; run; bucket from the
   self-documenting failure message.
3. Update `PROGRESS.tsv` (status emoji + bucket + note) and add any new bucket to `FAILURES.md`.
