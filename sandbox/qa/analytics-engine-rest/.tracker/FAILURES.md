# Root-cause buckets

39 buckets across 144 confirmed failures (2 tests span two buckets). Counts in `PROGRESS.tsv`. All confirmed by a sandbox/qa repro.

| Bucket | n | Root cause |
|--------|---|------------|
| A | 17 | Full-table `source=idx` (no explicit `\| fields`) returns schema columns **alphabetical**, not mapping-declaration order. Explicit projections are unaffected. |
| B | 4 | Unsuffixed decimal literal (`7.0`) and `half_float` widen to **float32**, not double → precision loss (`3.14285` vs `3.142857142857143`). |
| C | 4 | Cross-index `source=a,b` where a field differs in width/type (int vs long, text vs boolean) → 500 "incompatible field types"; no widening. |
| D | 10 | Implicit post-source filter (no `where` kw): `source=idx age=32` → 0 rows; `not age>32` → unfiltered. Explicit `\| where` works. |
| F | 8 | Scalar `eval max()/min()` (GREATEST/LEAST) → `undefined` type or 500 "Cannot infer return type". |
| G | 1 | `coalesce()` of all-missing fields → 500 instead of `undefined` null. |
| H | 10 | Error message/status divergence: `parse`/`rex` bad capture group and invalid date literals return 500 with a different/missing message vs the expected 4xx validation text. |
| I | 2 | `list/values(boolean)` renders `TRUE`/`FALSE` not `true`/`false`. |
| J | 5 | `values()` doesn't dedup / sort / honor `plugins.ppl.values.max.limit`. |
| K | 2 | `list()/values()` keep null/empty elements instead of dropping. |
| L | 2 | `scaled_float` typed/`typeof`'d as `bigint`, not `double`. |
| M | 1 | `DELETE /idx/_doc/N` on a parquet index → 500 (deletes unsupported). |
| N | 2 | `appendpipe`/`not in` with a filter subpipe → 500 "delegation_possible UDF body invoked" (Lucene delegation marker leaks into execution). |
| P | 3 | Sort/`head` order differs on equal keys / scan order (possibly nondeterministic). |
| Q | 5 | `percentile()` is interpolated `double`, not the discrete `bigint` order-statistic; empty bucket `null` vs `0`. |
| R | 14 | Unsupported scalar fns: `rand`, `conv`, `adddate`/`subdate`, `convert_tz`, `REGEXP`, higher-order `exists`/`filter`/`forall` → 400/500. |
| S | 1 | `cast(x as IP)` → 500 "No backend supports scalar function [IP]". |
| T | 3 | Aliased (`as o`/`as i`) and correlated/disjunctive subqueries return wrong/0 rows (plain IN-subquery works). |
| U | 1 | Range-`case` agg keeps the null/"unknown" bucket that upstream pushdown drops. |
| V | 11 | Approx/shortcut aggs unsupported: `distinct_count_approx` ("APPROX_COUNT_DISTINCT" unbound), `perc50`/`p95` → 500. |
| W | 5 | JSON builtins: `json()`/`json_append`/`json_extend` → 500; `json_set`/`json_delete` with `$.path` are no-ops. |
| X | 1 | `multisearch [A] [B]` plain row count off-by-one (52 vs 51). |
| Z | 5 | `plugins.ppl.subsearch.maxout` (bounded) not honored — returns all rows or 0. |
| AA | 1 | No native TIME type: a `time` field widens to TIMESTAMP, so `TIMEDIFF(time,time)` fails its `[TIME,TIME]` signature. |
| AB | 7 | `bin @timestamp bins=N \| stats by @timestamp`: group key comes back `string` not `timestamp`; term+time variants 500. |
| AC | 2 | `dedup ... CONSECUTIVE=true` → 500 "Consecutive deduplication is unsupported in Calcite". |
| AD | 2 | `date`/`time`-mapped columns keep `date`/`time` schema label where the AE wire-format tests expect `timestamp`. |
| AE | 4 | `union`/`multisearch` + stats miscounts / wrong grouping (1014 vs 1000; every group 22). |
| AF | 1 | Variadic `concat(field, 'lit', ...)` → 500 "Variadic arguments must have consistent types". |
| AG | 1 | `where not true = case(...)` lowers to `IS NOT TRUE` → 500 "Unrecognized filter operator". |
| AH | 1 | `earliest('now', now)` returns `true` vs expected `false` (relative-time eval semantics). |
| AI | 1 | `==` on a `text` field (no keyword subfield) returns 0 rows; compares analyzed text not exact term. |
| AJ | 2 | `patterns method=BRAIN show_numbered_token` emits a different `patterns_field`/`tokens` struct. |
| AK | 1 | `append [...] \| where cidrmatch(host,...)` merging an IP-UDT column → 500 "unsupported object class [B". |
| AL | 1 | `mvcombine <field>` → 500 "No enum constant AggregateFunction.ARRAY_AGG". |
| AM | 2 | SQL `?format=csv` ignored — returns JSON, so CSV sanitization/quoting never applied. |
| AN | 1 | Explain-only test asserts the DSL pushdown plan string (`.keyword`); AE explain is a Calcite/Substrait plan. |
| AO | 1 | Timestamp sub-seconds padded to nanos (`.95500000`) instead of trimmed (`.955`). |
| AP | 1 | `plugins.query.memory_limit=1%` not enforced — query succeeds instead of failing with a resource error. |

## ⏭ Skipped (16, out of scope)
- **geo_point / nested-object-in-query** (per maintainer policy): the field type can't run on the AE
  parquet route, or the query filters/groups a nested-object subfield. Binary fields are kept
  (mappings use `store:true`) and DO run.
- **can't-disable-calcite**: `FieldsCommandIT.testEnhancedFieldsWhenCalciteDisabled` needs
  `plugins.calcite.enabled=false`; the AE route forces calcite on.

Two further AE behaviors were observed while provisioning but their only upstream test
(`DataTypeIT.test_nonnumeric_data_types`) is skipped (geo_point + nested), so they have no live test:
geo_point rejected at index creation, and a bare `object` field flattened to dotted leaf columns
instead of a single `array`/`struct`.
