# Analytics-Engine QA — 2-Shard Failure Report

_Branch `report-2shard-all-unmuted` — DatasetProvisioner defaulted to **2 shards**, all 39 `@AwaitsFix` mutes removed, with the 4 PR'd fixes + CBO-2x + Cause-A sentinel applied. Native lib rebuilt. Full `:sandbox:qa:analytics-engine-rest:integTest`._

## Headline

- **768 test methods**: 654 passed, 114 failed, 0 skipped.
- Expanded per-query (monolithic `*PplIT` suites run every `q*.ppl` in one method): **355 individual failing queries / IT methods**.
- The `*PplIT` suites were muted wholesale on a single `"Failing due to unsupported operations"` tag; unmuting them surfaces every underlying per-query failure below.

## Failures by bucket (overview)

```text
Limit/row-count doubling (limi ██████████████████████████████████████ 137
Unimplemented scalar function  ████████████████ 58
2-shard ordering / row-positio ████████████████ 56
Value/cell correctness mismatc ████████████ 44
Other 500 / backend error      ██████ 22
Perf-delegation shape (canSeri ████ 13
Calcite->Substrait conversion  ██ 8
Field-not-found / mapping      ██ 6
Type coercion / cast (variadic █ 4
Unmapped AggregateFunction enu █ 2
Streaming fragment failed to s █ 2
Other 4xx invalid query        █ 1
Frontend type-resolution (UNDE █ 1
Other / uncategorized          █ 1
```

## Failure buckets (assign one owner per bucket)

_Owner = suggested area; replace with a teammate's name._

| # | Bucket | Failing units | New under 2-shard? | Owner | Suggested action |
|---|--------|---------------|--------------------|-------|------------------|
| 1 | Limit/row-count doubling (limit pushdown — other team) | 137 (4 not-previously-muted) | ? | Triage | triage |
| 2 | Unimplemented scalar function | 58 (0 not-previously-muted) | pre-muted | Backlog — unassigned | Backlog / won't-fix per directive (no new functions). Triage the function list below for priority. |
| 3 | 2-shard ordering / row-position | 56 (56 not-previously-muted) | **YES — new** | QTF / reduce-path (ordering) | Concat-gather is not merge-sorted across shards. QTF/LateMaterialization + reduce-path ordering. One owner. |
| 4 | Value/cell correctness mismatch | 44 (10 not-previously-muted) | mixed | Per-suite triage | Per-suite correctness (under-aggregation, union counts). Triage per suite. |
| 5 | Other 500 / backend error | 22 (6 not-previously-muted) | mixed | Triage | Residual 500s — triage individually. |
| 6 | Perf-delegation shape (canSerialize prune) | 13 (0 not-previously-muted) | pre-muted | Filter-delegation | Known fix scoped: `canSerialize` prune in `OpenSearchFilterRule` (separate PR). |
| 7 | Calcite->Substrait conversion (convert/cast) | 8 (2 not-previously-muted) | ? | Triage | triage |
| 8 | Field-not-found / mapping | 6 (0 not-previously-muted) | pre-muted | QA datasets / mapping | Dataset mapping / field-resolution gaps (latency_ms, exception_type, json_test_data). |
| 9 | Type coercion / cast (variadic, RexCall) | 4 (0 not-previously-muted) | pre-muted | Type system | Type-coercion + a RexCall->plan cast bug. |
| 10 | Unmapped AggregateFunction enum/UDAF | 2 (0 not-previously-muted) | pre-muted | Aggregations / UDAF | Map/implement the agg enums (ARG_MIN, DISTINCT_COUNT_APPROX, percentile_approx UDAF). |
| 11 | Streaming fragment failed to start | 2 (1 not-previously-muted) | mixed | Runtime / ffm routing | Residual-filter / ffm routing on object & dynamic-mapping fields. |
| 12 | Other 4xx invalid query | 1 (0 not-previously-muted) | ? | Triage | triage |
| 13 | Frontend type-resolution (UNDEFINED) | 1 (0 not-previously-muted) | pre-muted | opensearch-sql frontend | opensearch-sql frontend type bug (patterns auto-take N). |
| 14 | Other / uncategorized | 1 (0 not-previously-muted) | mixed | Triage | Needs manual triage. |

## Recommended assignment order

1. **Binary/IP type mismatch** (1 engineer) — single root cause, **170 queries** unlocked by one Substrait↔table type alignment. Biggest ROI. Owner: engine/Substrait.
2. **2-shard ordering / row-position** (1 engineer) — the only **genuinely new** regression class from the 2-shard flip (**65 units**). Concat-gather must merge-sort. Owner: QTF/reduce-path.
3. **Planner: unmarked child [LogicalJoin]** (1 engineer) — 8 join queries 500 on a planner-rule gap; self-contained.
4. **Unmapped AggregateFunction enum/UDAF** + **Value/cell correctness** — split per suite among 1–2 engineers.
5. **Unimplemented functions** — backlog; prioritise from the function-frequency list, do not block on these (per directive: no new scalar functions).


## Detail by bucket

### Limit/row-count doubling (limit pushdown — other team)  — 137 units
**Owner:** Triage  ·  **New under 2-shard:** 4 of 137

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 108
FunctionsPplIT                 █████ 15
RexCommandPplIT                ██ 5
HeadCommandIT                  █ 4
MultiIndexQueriesPplIT         █ 1
ComplexJoinsPplIT              █ 1
LookupJoinQueriesPplIT         █ 1
MultiSourceJoinsPplIT          █ 1
FulltextWindowPplIT            █ 1
```

Examples:
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q1`: : Row count mismatch - expected 10, got 20
- `HeadCommandIT` → `testHeadFromOffset`: Row count for query: source=calcs | fields str2 | head 5 from 14 expected:<3> but was:<0>
- `FunctionsPplIT` → `functions:Q13`: : Row count mismatch - expected 5, got 10
- `ComplexJoinsPplIT` → `complex_joins:Q4`: : Row count mismatch - expected 3, got 0

### Unimplemented scalar function  — 58 units
**Owner:** Backlog — unassigned  ·  **New under 2-shard:** 0 of 58

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 46
ComplexRegexPplIT              ████ 5
AggregationsPplIT              ███ 4
AppLogsPplIT                   ██ 2
LookupTableQueriesPplIT        █ 1
```

Examples:
- `AggregationsPplIT` → `aggregations:Q7`: Unrecognized field type [SYMBOL] for field [$f4]
- `AggregationsPplIT` → `aggregations:Q8`: Unrecognized field type [SYMBOL] for field [$f5]
- `AggregationsPplIT` → `aggregations:Q9`: Unrecognized field type [SYMBOL] for field [$f6]
- `AggregationsPplIT` → `aggregations:Q10`: Unrecognized field type [SYMBOL] for field [$f6]

**Distinct functions (per-query hits):** `SPLIT`×9, `GROK`×8, `fieldtype:SYMBOL`×7, `JSON_OBJECT`×2, `CASE`×2, `DATE_ADD`×1, `DATE_SUB`×1, `JSON_ARRAY`×1, `IS JSON VALUE`×1, `ARRAY_COMPACT`×1, `forall`×1, `exists`×1, `filter`×1, `transform`×1, `reduce`×1, `ADDDATE`×1, `ADDTIME`×1, `DATEDIFF`×1, `DAYNAME`×1, `FROM_DAYS`×1, `GET_FORMAT`×1, `LAST_DAY`×1, `MONTHNAME`×1, `PERIOD_ADD`×1, `PERIOD_DIFF`×1, `SEC_TO_TIME`×1, `SUBDATE`×1, `SUBTIME`×1, `TIME_TO_SEC`×1, `TIME_DIFF`×1, `TO_DAYS`×1, `TO_SECONDS`×1, `UTC_DATE`×1, `UTC_TIME`×1, `UTC_TIMESTAMP`×1, `WEEKDAY`×1, `YEARWEEK`×1, `JSON`×1, `RINT`×1

### 2-shard ordering / row-position  — 56 units
**Owner:** QTF / reduce-path (ordering)  ·  **New under 2-shard:** 56 of 56

Failing units by suite:
```text
StreamstatsCommandIT           ██████████████████████████████████████ 31
FillNullCommandIT              █████████████ 11
EvalCommandIT                  █████ 4
SortCommandIT                  ██ 2
ReverseCommandIT               ██ 2
RangeBucketCommandIT           ██ 2
WidthBucketCommandIT           ██ 2
MinspanBucketCommandIT         ██ 2
```

Examples:
- `SortCommandIT` → `testSortByAbsExpression`: Row 0 should be null expected null, but was:<12.3>
- `SortCommandIT` → `testSortByAbsTakesNonNullsFromTail`: Row 0 unexpectedly null
- `ReverseCommandIT` → `testReverseAfterFilterFindsUpstreamSort`: Cell mismatch at row 0, col 0 for query: source=calcs | sort int0 | where int0 >= 4 | reverse | head 3 | fields int0: expected <11> but was <4>
- `RangeBucketCommandIT` → `testBinStartEndPreservesNullsInNullableField`: Cell mismatch at row 0, col 0 for query: source=calcs | bin num0 start=-100 end=100 | fields num0 | head 10 expected:<[0-10]0> but was:<[-100-]0>

### Value/cell correctness mismatch  — 44 units
**Owner:** Per-suite triage  ·  **New under 2-shard:** 10 of 44

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 7
FulltextWindowPplIT            ██████████████████████████████████████ 7
LookupJoinQueriesPplIT         ███████████████████████████ 5
MultiIndexQueriesPplIT         ██████████████████████ 4
MathFunctionIT                 ██████████████████████ 4
SecurityLogsPplIT              ████████████████ 3
SpanBucketCommandIT            ████████████████ 3
RexCommandPplIT                ████████████████ 3
ComplexJoinsPplIT              ███████████ 2
RenameCommandIT                █████ 1
RexCommandIT                   █████ 1
AggregationsPplIT              █████ 1
KubernetesLogsPplIT            █████ 1
FieldsCommandIT                █████ 1
MultiSourceJoinsPplIT          █████ 1
```

Examples:
- `RenameCommandIT` → `testRenameSingleField`: Row count expected:<3> but was:<6>
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q3`: row 0 col 0: Value mismatch - expected 8, got 1
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q5`: row 0 col 0: Value mismatch - expected 43, got 17
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q11`: row 2 col 1: Value mismatch - expected 2026-05-12 09:00:00, got 2026-05-12 10:00:00

### Other 500 / backend error  — 22 units
**Owner:** Triage  ·  **New under 2-shard:** 6 of 22

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 10
MathFunctionIT                 ███████████████████ 5
ComplexJoinsPplIT              ███████████ 3
ObjectFieldIT                  ████████ 2
ArrayFunctionIT                ████ 1
MultisearchCommandIT           ████ 1
```

Examples:
- `ArrayFunctionIT` → `testArrayLength`: Execution error: Substrait error: Field 'len' in Substrait schema has a different type (Int32) than the corresponding field in the table schema (Int64).
- `ComplexJoinsPplIT` → `complex_joins:Q1`: StreamException[errorCode=INTERNAL, message=java.lang.RuntimeException: Execution error: Execution error: prefetch for row group 0 panicked: create_provider FFM upcall failed: \
- `ComplexJoinsPplIT` → `complex_joins:Q2`: StreamException[errorCode=INTERNAL, message=java.lang.RuntimeException: Execution error: Execution error: prefetch for row group 0 panicked: create_provider FFM upcall failed: \
- `ComplexJoinsPplIT` → `complex_joins:Q3`: TaskCancelledException[query cancelled]\nFor more details, please send request for Json format to see the raw response from OpenSearch engine.

### Perf-delegation shape (canSerialize prune)  — 13 units
**Owner:** Filter-delegation  ·  **New under 2-shard:** 0 of 13

Failing units by suite:
```text
ReplaceCommandIT               ██████████████████████████████████████ 8
RexCommandIT                   ██████████████ 3
WhereCommandIT                 █████ 1
SpathCommandIT                 █████ 1
```

Examples:
- `RexCommandIT` → `testRexSedReplaceCaseInsensitive`: EQUALS performance-delegation requires (RexInputRef, RexLiteral); got REGEXP_REPLACE($20, 'furniture', 'FURN', 'i') = 'FURN':VARCHAR

### Calcite->Substrait conversion (convert/cast)  — 8 units
**Owner:** Triage  ·  **New under 2-shard:** 2 of 8

Failing units by suite:
```text
ComplexJoinsPplIT              ██████████████████████████████████████ 3
ExtensiveCoveragePplIT         ██████████████████████████████████████ 3
MathScalarFunctionsIT          █████████████████████████ 2
```

Examples:
- `ComplexJoinsPplIT` → `complex_joins:Q6`: Unable to convert call DIVIDE(decimal<23,1>, i64).
- `ComplexJoinsPplIT` → `complex_joins:Q8`: Unable to convert call DIVIDE(decimal<21,1>?, i64).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q149`: Unable to convert call TIMESTAMPADD(string, i32, precision_timestamp<0>?).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q163`: Unable to convert call json_set(string?, string, i32).

### Field-not-found / mapping  — 6 units
**Owner:** QA datasets / mapping  ·  **New under 2-shard:** 0 of 6

Failing units by suite:
```text
MultiIndexQueriesPplIT         ██████████████████████████████████████ 3
ObjectFieldIT                  █████████████████████████ 2
ExtensiveCoveragePplIT         █████████████ 1
```

Examples:
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q2`: Field [latency_ms] not found.
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q7`: Field [exception_type] not found.
- `ObjectFieldIT` → `testSelectTopLevelObjectField`: Field [city] not found.

### Type coercion / cast (variadic, RexCall)  — 4 units
**Owner:** Type system  ·  **New under 2-shard:** 0 of 4

Failing units by suite:
```text
SecurityLogsPplIT              ██████████████████████████████████████ 2
ExtensiveCoveragePplIT         ██████████████████████████████████████ 2
```

Examples:
- `SecurityLogsPplIT` → `security_logs:Q5`: class org.apache.calcite.rex.RexCall cannot be cast to class org.opensearch.analytics.planner.rel.AnnotatedPredicate (org.apache.calcite.rex.RexCall and org.opensearch.analytics.pl
- `SecurityLogsPplIT` → `security_logs:Q7`: class org.apache.calcite.rex.RexCall cannot be cast to class org.opensearch.analytics.planner.rel.AnnotatedPredicate (org.apache.calcite.rex.RexCall and org.opensearch.analytics.pl
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q39`: Variadic arguments must have consistent types when parameterConsistency is CONSISTENT. Argument at index 1 has type FixedChar{nullable=false, length=1} but argument at index 2 has 

### Unmapped AggregateFunction enum/UDAF  — 2 units
**Owner:** Aggregations / UDAF  ·  **New under 2-shard:** 0 of 2

Failing units by suite:
```text
SecurityLogsPplIT              ██████████████████████████████████████ 1
ExtensiveCoveragePplIT         ██████████████████████████████████████ 1
```

Examples:
- `SecurityLogsPplIT` → `security_logs:Q8`: No enum constant org.opensearch.analytics.spi.AggregateFunction.ARG_MIN
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q93`: No enum constant org.opensearch.analytics.spi.AggregateFunction.DISTINCT_COUNT_APPROX

### Streaming fragment failed to start  — 2 units
**Owner:** Runtime / ffm routing  ·  **New under 2-shard:** 1 of 2

Failing units by suite:
```text
DynamicMappingSearchIT         ██████████████████████████████████████ 1
ObjectFieldIT                  ██████████████████████████████████████ 1
```

Examples:
- `DynamicMappingSearchIT` → `testSearchOnDynamicallyAddedFields`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:65400], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { "err

### Other 4xx invalid query  — 1 units
**Owner:** Triage  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
LookupTableQueriesPplIT        ██████████████████████████████████████ 1
```

Examples:
- `LookupTableQueriesPplIT` → `lookup_table_queries:Q7`: failed: method [POST], host [http://127.0.0.1:65401], URI [/_plugins/_ppl], status line [HTTP/1.1 400 Bad Request] {"error":{"root_cause":[{"type":"illegal_argu

### Frontend type-resolution (UNDEFINED)  — 1 units
**Owner:** opensearch-sql frontend  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
PatternsCommandIT              ██████████████████████████████████████ 1
```

Examples:
- `PatternsCommandIT` → `testSimplePatternAggregationModeMultiShard`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:65400], URI [/_analytics/ppl], status line [HTTP/1.1 500 Internal Server Error] {"err

### Other / uncategorized  — 1 units
**Owner:** Triage  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
StreamstatsCommandIT           ██████████████████████████████████████ 1
```

Examples:
- `StreamstatsCommandIT` → `testWhereInWithStreamstatsSubquery`: Expected query [source=calcs | where key in [ source=calcs | streamstats count() as cnt | where cnt < 5 | fields key ] | head 1] to fail but got: {schema=[{name=bool0, type=boolean
