# Multi-value search path: measured findings

Branch `mv-search-experiments` = `main` + PR #22883 (linuxpi, adaptive keyword promotion — ingestion only)
+ the ingestion extension to every scalar type + a measurement harness. PR #22902 is fetched as
`mv-search-ref` for comparison but **not** merged, so the search side here is ours.

Everything below is measured on a live 2-node cluster over composite parquet+lucene indices, not
inferred. Reports:

* `sandbox/qa/analytics-engine-rest/build/testrun/integTest/build/multivalue-matrix-arrays.md`
* `.../testrun/integTestMultiValueScalarSchema/build/multivalue-matrix-scalar.md`
* `.../multivalue-promotion-*.md`

Run them with:

```
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-amzn PROTOC=/local/home/handalm/.local/protoc/bin/protoc
./gradlew -Dsandbox.enabled=true -PrustDebug :sandbox:qa:analytics-engine-rest:integTest \
  --tests "*MultiValueFunctionMatrixIT*"                                  # arm A: Calcite says ARRAY
./gradlew -Dsandbox.enabled=true -PrustDebug \
  :sandbox:qa:analytics-engine-rest:integTestMultiValueScalarSchema       # arm B: Calcite says element
```

# The headline

Arrays already work end to end through the engine. On the two types whose parquet LIST read is not
broken (`boolean`, `date`), a stored multi-value column flows Calcite `ARRAY` → Substrait `list` →
DataFusion `List` → nested kernels → row codec → JSON, and `array_length`, `mvdedup`, `mvindex` all
return correct per-element answers with `[]` and absent still distinguishable:

| query | result |
|---|---|
| `eval x = array_length(dt_m)` | `[1,1] [2,2] [3,3] [4,0] [5,null] [6,1]` |
| `eval x = mvdedup(bool_m)` | `[3, [true, false]]` from stored `[true, false, true]` |
| `eval x = mvindex(bool_m, 0)` | first element per row |

So Approach A is not blocked on plumbing. It is blocked on three separable things, in this order:

1. a parquet read bug that corrupts LIST column reads for most types;
2. the sql plugin's type checker rejecting `ARRAY` operands for ~every scalar, comparison and
   aggregate signature;
3. grouping, ordering and joining silently keying on the **whole array** instead of per element.

# Arm A — Calcite surfaces ARRAY (the 8-line change)

`OpenSearchSchemaBuilder` wraps the leaf type in `createArrayType` when the mapping says
`multi_value: true`. That is the whole front-end change, gated here on
`-Danalytics.multivalue.surface_arrays` so the other arm is reachable.

Verdicts over 272 probe×type cells:

| verdict | count | meaning |
|---|---|---|
| DIFFERS | 81 | both answer, differently — a semantics decision |
| MULTI_FAILS | 160 | works on scalar, errors on multi — the work list |
| BOTH_FAIL | 24 | unsupported for the type regardless of cardinality |
| SCALAR_FAILS | 6 | array-native probes, meaningless on a scalar column |
| SAME | 1 | |

Failure stage for the multi side: 102 FRONTEND, 74 redacted backend 500s, 38 "type has no LIST
writer" (`text`).

## 1. Parquet cannot read the LIST column for most types

The redacted `Internal error [task_id=N]` responses are, in the node log:

```
Execution error: Parquet error: Arrow: Parquet argument error: External: Src size is incorrect
Execution error: Parquet error: Arrow: Parquet argument error: External: the offset to copy is not
  contained in the decompressed buffer
```

Cause: `cache/page_index/column_schema_resolver.rs` resolves a predicate column to a physical parquet
column via `StatisticsConverter::parquet_column_index()`, which by design refuses to resolve a nested
Arrow root — a `LIST` root can map to several leaves. The LIST column therefore gets no `OffsetIndex`,
the scoped page-index reader falls back to a placeholder range, and the decompressor is handed a
wrong-sized source buffer. `boolean` and `date` escape because they do not take that path.

This is exactly what PR #22902's `column_schema_resolver.rs` hunk fixes (include every leaf whose
`get_column_root_idx` matches the requested root). Whatever we do about semantics, that read fix is a
prerequisite — take it, or write the equivalent.

## 2. The sql plugin rejects ARRAY operands

Surfacing `ARRAY` makes the front end reject the query before any RelNode exists. Verbatim:

```
EQUAL function expects {[IP,IP],[COMPARABLE_TYPE,COMPARABLE_TYPE]}, but got [ARRAY,STRING]
In expression types are incompatible: fields type ARRAY, values type [STRING, STRING]
LIKE function expects {[STRING,STRING,BOOLEAN]}, but got [ARRAY,STRING,BOOLEAN]
Aggregation function SUM expects field type {...}, but got [ARRAY]
```

Mechanism: `PPLTypeChecker.validateOperands` (sql plugin, `core/.../PPLTypeChecker.java`) checks
`funcTypeFamily.getTypeNames().contains(paramType)`. `ARRAY` is in no scalar function's type family,
so every comparison, `IN`, `LIKE`, `SUM`/`AVG`/`PERCENTILE`/`VALUES`/`LIST` fails. The type *mapping*
side is already fine — `OpenSearchTypeFactory.convertSqlTypeNameToExprType` has `case ARRAY -> ARRAY`,
so the response schema renders and nothing throws on conversion.

This is loud rather than silently wrong, which is the good failure mode, but it means the choice is:

* **A-front:** teach the sql plugin that a multi-value column is acceptable wherever its element type
  is, and let it emit the element-semantics call. Requires sql-plugin changes and a way for it to know
  which columns are multi-valued.
* **A-back:** keep surfacing the element type to the front end (arm B), let PPL build the ordinary
  single-valued RelNode it builds today, and rewrite to array semantics inside our planner. No
  sql-plugin change. This is the shape you sketched — "we'll get relnodes with single-valued functions
  on single-valued fields, and filters would change to in or contains."

Arm B is the cheaper path, and the matrix says the rewrite targets exist (§Rewrite table). It needs one
correction to what you assumed, below.

## 3. Grouping, ordering, dedup and joins key on the whole array

These do **not** fail. They answer, with array-as-one-key semantics:

| probe | scalar column | multi column |
|---|---|---|
| `stats count() by f` | `[2, null] [4, alpha]` | `[1, null] [1, []] [2, [alpha]] [1, [alpha, beta]] [1, [alpha, beta, alpha]]` |
| `top 3 f` | `[alpha, 4] [null, 2]` | `[[alpha], 2] [[], 1] [[alpha, beta], 1]` |
| `eventstats count() by f` | every row 4 | per-row 1 or 2 |
| `dedup f \| stats count()` | `[1]` | `[4]` |
| `join on l.f = r.f` | `[16]` | `[7]` |
| `sort f` | value order | lexicographic array order, `[]` after `null` |

A DSL terms aggregation on a multi-valued keyword puts a document in one bucket per element, so the
right answer for `stats count() by tags` over `[alpha]`, `[alpha,beta]`, `[alpha,beta,alpha]` is
`alpha: 3, beta: 2` — not five whole-array buckets. This works because Arrow's row format encodes
`List` (`arrow-row-58.3.0`), so DataFusion's `GroupValuesRows` fallback accepts a List grouping column
and hashes the encoded bytes. Nothing errors; the answer is just a different question.

Same mechanism explains sort (row-format list ordering), `dedup`, `top`/`rare`, and the hash join key.

## 4. Aggregates over a LIST column

`count(f)` works and returns 5 where the scalar column returns 4 — the empty array counts as a value.
`distinct_count` has a real kernel gap:

```
Support for 'approx_distinct' for data type List(Int64, field: 'element') is not implemented
```

`min`/`max` over a List fail too, including for `date` where the read works, so that is a kernel gap
rather than the parquet bug. `sum`/`avg`/`percentile`/`values`/`list` never reach the backend — the
front end rejects them first.

# Arm B — Calcite surfaces the element type

Your hypothesis was that leaving the schema alone would let everything pass through because Substrait
would not complain. Substrait indeed does not complain. DataFusion does, at schema merge:

```
Arrow error: Schema error: Fail to merge schema field 'tags' because the from data_type =
  List(Utf8, field: 'element') does not equal Utf8
```

And it is worse than per-query: **255 of 272 cells are BOTH_FAIL**, meaning probes that only touch the
ordinary scalar columns fail as well. One multi-value column in the mapping breaks every query against
the index, because the scan reconciles the whole file schema rather than only projected columns.

So arm B is not viable as-is. It becomes viable only with the scalar→singleton-LIST adapter that
#22902 adds (`scalar_to_list_adapter.rs`), or by teaching the reconcile step to accept
`List<T>` where the plan says `T` and unwrap on read. Either way, that adapter is load-bearing for the
A-back path, because A-back deliberately keeps the plan's declared type scalar.

# Auto-promotion (mixed generations)

`multi_value` reached by promotion rather than declaration leaves a scalar-column file and a
LIST-column file in the same shard. Every read against that index fails today:

```
source=mv_promoted | fields id, tags | sort id            → backend 500
source=mv_promoted | stats count() by tags | sort tags     → backend 500
source=mv_promoted | where tags = 'alpha' | stats count()  → 400 (ARRAY operand)
```

Two consequences worth naming early:

* A data stream that rolls over gets a mix of scalar and LIST backing indices by construction, not by
  accident. Mixed-generation reads are the normal case, not an edge case.
* `IndexResolution.validateSchemaCompatibility` compares only `mapFieldType(type)`, so `keyword` and
  `keyword multi_value:true` across two backing indices are judged compatible, while
  `OpenSearchSchemaBuilder.resolveTable` picks one shape first-wins via `putIfAbsent`. An index
  pattern spanning a promotion boundary therefore builds a schema that is wrong for half the shards
  with no error at plan time. This needs a fix regardless of which approach wins.

# Rewrite table:

What Approach A has to emit, and whether DataFusion 54 already has it. `datafusion-functions-nested`
54.0.0 is richer than expected — `array_has`/`array_has_all`/`array_has_any`, `array_min`/`array_max`,
`array_distinct`/`array_union`/`array_intersect`/`array_except`, `cardinality`, `array_length`,
`flatten`, `array_position`, `array_sort`, `array_to_string`, plus higher-order `array_transform`,
`array_filter`, `any_match`.

| user intent | today | element semantics | DF54 target | reachable over Substrait |
|---|---|---|---|---|
| `f = v` | `=` | any element equals | `array_has(f, v)` | yes |
| `f != v` | `!=` | `NOT array_has(f, v)` | `array_has` + `NOT` | yes |
| `f IN (a,b)` | `IN` | any element in set | `array_has_any(f, [a,b])` | yes |
| `f > v` | `>` | any element `> v` | `array_max(f) > v` | yes |
| `f < v` | `<` | any element `< v` | `array_min(f) < v` | yes |
| `isnull(f)` | `IS NULL` | decide whether `[]` is null | `f IS NULL OR cardinality(f) = 0` | yes |
| `sort f` | ORDER BY | DSL default sort mode | order by `array_min(f)` / `array_max(f)` desc | yes |
| `min(f)` / `max(f)` | MIN/MAX | over all elements | `min(array_min(f))` / `max(array_max(f))` | yes |
| `like(f, p)` | LIKE | any element matches | `any_match(f, x -> x LIKE p)` | **no — lambda** |
| `upper(f)` etc. | scalar | map over elements | `array_transform(f, x -> upper(x))` | **no — lambda** |
| `sum(f)` / `avg(f)` | SUM/AVG | over all elements | no `array_sum` in DF54 | **no** |
| `dc(f)` | distinct count | across elements | `approx_distinct` rejects List | **no** |
| `stats … by f` | GROUP BY | one bucket per element | `unnest` then group | **no — no Unnest rel** |
| `match(f, …)` | full text | Lucene is natively multi-valued | delegate | already correct |

Three real blockers in that table, each with a shape of answer:

### Lambdas cannot cross Substrait:

`datafusion-substrait` 54 has no lambda encoding, and the repo already knows it —
`ArrayFunctionIT`'s header says substrait extension YAML cannot declare `func<…>` arguments. So
`array_transform` and `any_match` are unreachable even though the kernels exist.

Your instinct that arrow kernels should let us lift `int` to `int[]` for free is right in substance and
wrong in mechanism: there is no implicit map-over-list coercion anywhere in DataFusion, but the lift is
trivial to build explicitly. A `List<T>` child array *is* a contiguous `T` array, so a generic wrapper
UDF can flatten the child, invoke the inner kernel on it, and rewrap with the original offsets and
validity. One Rust module can register `mv_<fn>` for every length-preserving elementwise scalar UDF by
walking `SessionContext::state().scalar_functions()` at session setup — `udf/mod.rs::register_all` is
the single place it plugs in. The Calcite side then renames `upper(ARRAY<VARCHAR>)` to `mv_upper` and
retypes the result as `ARRAY<VARCHAR>`. That covers `upper`, `lower`, `trim`, `length`, `substr`,
`replace`, `md5`, `abs`, `round`, `year`, `date_format`, and the rest of the elementwise set in one
change, with no lambda and no wire-format work.

`like` is the same shape with a boolean result: `mv_like` returning a `List<Bool>`, plus a reduce to
`any` — or a direct `mv_any_like(f, p) -> Bool`.

### `unnest` cannot cross Substrait either:

`datafusion-substrait` 54 explicitly refuses it in both directions:
`LogicalPlan::Unnest(plan) => not_impl_err!` in the producer, and the consumer's rel set is only
Read/Filter/Project/Aggregate/Sort/Fetch/Join/Cross/Set/Exchange. So per-element grouping cannot be
expressed on the current wire.

The repo already has the mechanism to add it: an `ExtensionSingleRel` carrying a JSON detail, which is
how `os_stage_boundary` gets a `StageBoundaryNode` across for whole-plan lowering. An
`os_unnest` marker with `{column}` detail, a `UserDefinedLogicalNodeCore` on the Rust side that lowers
to `LogicalPlan::Unnest`, and a Calcite rule that inserts it below the Aggregate when a grouping key is
ARRAY-typed. That is the honest answer for group-by; the alternatives are unnesting inside
`ShardScanExec` (breaks row-id late materialization, since row count stops matching) or declaring
whole-array grouping as our semantics (a divergence users will hit immediately with `stats count() by
tags`).

### `sum`/`avg` over all elements have no kernel:

DF54 has `array_min`/`array_max` but no `array_sum`. Either add a small Rust UDF (`array_sum`,
`array_avg` — trivial over the child array with offsets), or route through the same `os_unnest` marker
and let ordinary `SUM` do the work. If `os_unnest` lands for group-by anyway, reuse it.

# Delegation is an accidental ally, and an inconsistency risk

`FieldStorageInfo` is built from the **mapping** type string via `FieldType.fromMappingType`
(`FieldStorageResolver.resolveField`), not from the Calcite type. So a base-column predicate on a
multi-value keyword still presents as `KEYWORD` to the capability registry, delegation to the Lucene
backend stays eligible, and Lucene — natively multi-valued — evaluates `tags = 'alpha'` with correct
contains semantics for free.

The same predicate evaluated in parquet gets whole-value semantics or a coercion error. That means one
query can change answers based on whether delegation fired, which depends on
`analytics.delegation.lucene.blocked_predicates` and on whether the field has Lucene index formats.
Any approach has to pick one semantics and make the delegated and non-delegated paths agree.

Note also that `FieldType.ARRAY` already exists in the SPI with a comment saying arrays have no
OpenSearch mapping equivalent, and it is used only for array-*returning* expression results. No backend
declares filter or scalar capability on `ARRAY`, so a **derived** array column — a HAVING clause over
`values(...)`, a filter above a project that produced an array — is rejected by
`OpenSearchFilterRule` with "No backend can evaluate filter predicate". That path needs capability
registrations whichever approach wins.

# What this branch contains

### Ingestion, extended past keyword:

PR #22883's list machinery in `ParquetField` is generic; each type needs `addToVector` plus
`supportsMultiValue()`. Added for `long`, `integer`, `double`, `boolean`, `date`, `ip` (keyword came
with the PR). On the mapper side `ParametrizedFieldMapper.multiValueParameter()` and
`addFieldForPluggableFormat` are likewise generic; wired into `NumberFieldMapper`,
`BooleanFieldMapper`, `DateFieldMapper`, `IpFieldMapper`. `text` is deliberately left out —
`TextParquetField` has no list writer, and index creation rejecting `multi_value: true` on it is a
result the matrix records.

### Minimum search-side plumbing:

`OpenSearchSchemaBuilder` ARRAY surfacing behind a switch; `ArrowCalciteTypes.toArrowField` so an
ARRAY column carries its element child (a childless Arrow `List` field cannot be allocated, and
`LateMaterializationStageExecution` built exactly that); `ArrowValues` list rendering so `_source`
reconstruction does not drop the column. These three are the same three files #22902 touches on the
Java side, arrived at independently — that is a decent signal they are the true minimum.

### Harness:

`sandbox/qa/analytics-engine-rest/src/test/java/org/opensearch/analytics/qa/multivalue/`

* `MultiValueDataset` — one composite parquet index with a scalar/multi column pair per type, and six
  documents covering single, pair, triple-with-duplicate, `[]`, absent, and a repeat of single so a
  per-element group-by is distinguishable from a per-array one by count alone. `promoteField` builds
  the mixed-generation index separately.
* `Probe` — 48 query shapes across projection, equality, range, null, text, sort, group-by, aggregate,
  collecting aggregate, string/numeric/date/ip scalars, array-native, commands and join, each tagged
  with the types it applies to and per-type literals.
* `MultiValueFunctionMatrixIT` — runs every applicable probe against both columns, classifies failures
  by layer, and emits the markdown report. It asserts only that the scalar control did not regress
  (four pre-existing scalar failures are listed in `KNOWN_SCALAR_FAILURES`), so the interesting
  failures are data rather than a red build.

# Incidental bugs found on single-valued columns

These fail on ordinary scalar columns and have nothing to do with multi-value work:

* `where bool_s = true` → backend 500.
* `stats distinct_count(dbl_s)` and `distinct_count(bool_s)` → backend 500.
* `stats min(ip_s), max(ip_s)` → backend 500.
* `head 3 | fields ip_m` → `unsupported object class [B` from the row codec, so a binary-typed array
  cell has no rendering.

# Recommendation

1. Take #22902's read fixes (`column_schema_resolver.rs` leaf resolution, `scalar_to_list_adapter.rs`,
   `segment_info.rs`). Nothing else can be measured until LIST columns read correctly, and mixed
   generations are the normal case for data streams.
2. Fix `IndexResolution.validateSchemaCompatibility` and `OpenSearchSchemaBuilder.resolveTable` to
   treat scalar and LIST declarations of the same field as a conflict to resolve rather than a
   first-wins pick. Today an index pattern across a promotion boundary is silently wrong.
3. Choose the front-end contract. A-back (element type to the sql plugin, rewrite in our planner) needs
   no sql-plugin change and keeps every existing query parseable; it depends on the scalar→LIST read
   adapter and on knowing which columns are multi-valued, which the index-metadata flag gives us.
4. Build the two generic pieces the rewrite needs: the `mv_<fn>` elementwise wrapper (cheap, unlocks
   the whole scalar function surface at once) and the `os_unnest` extension rel (the only honest answer
   for group-by, `dc()`, and `sum`/`avg` over elements).
5. Decide the semantics questions the matrix surfaced, and write them down before coding: is `[]` null;
   does `count(f)` count documents or values; is `f != v` "no element equals" or "some element differs";
   which sort mode is the default; what does a join on a multi-valued key mean.
