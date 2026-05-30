# Analytics-Engine QA — 2-Shard Failure Report

_Branch `report-2shard-all-unmuted` — DatasetProvisioner defaulted to **2 shards**, all 39 `@AwaitsFix` mutes removed, with the 4 PR'd fixes + CBO-2x + Cause-A sentinel applied. Native lib rebuilt. Full `:sandbox:qa:analytics-engine-rest:integTest`._

## Headline

- **768 test methods**: 649 passed, 119 failed, 0 skipped.
- Expanded per-query (monolithic `*PplIT` suites run every `q*.ppl` in one method): **387 individual failing queries / IT methods**.
- The `*PplIT` suites were muted wholesale on a single `"Failing due to unsupported operations"` tag; unmuting them surfaces every underlying per-query failure below.

## Failures by bucket (overview)

```text
Binary/IP type mismatch        ██████████████████████████████████████ 174
Unimplemented scalar function  █████████████ 58
2-shard ordering / row-positio ████████████ 56
Value/cell correctness mismatc ███████ 33
Other / uncategorized          █████ 22
Other 500 / backend error      ███ 16
Perf-delegation shape (canSeri ███ 13
Field-not-found / mapping      █ 6
Type coercion / cast (variadic █ 4
Unmapped AggregateFunction enu █ 2
Streaming fragment failed to s █ 2
Frontend type-resolution (UNDE █ 1
```

## Failure buckets (assign one owner per bucket)

_Owner = suggested area; replace with a teammate's name._

| # | Bucket | Failing units | New under 2-shard? | Owner | Suggested action |
|---|--------|---------------|--------------------|-------|------------------|
| 1 | Binary/IP type mismatch | 174 (0 not-previously-muted) | mostly pre-muted | Substrait / type-mapping | **One engine fix** — Substrait emits `Binary` but the parquet table schema is `BinaryView` for `ip`/`binary` fields. Align the type mapping. Highest leverage. |
| 2 | Unimplemented scalar function | 58 (0 not-previously-muted) | pre-muted | Backlog — unassigned | Backlog / won't-fix per directive (no new functions). Triage the function list below for priority. |
| 3 | 2-shard ordering / row-position | 56 (56 not-previously-muted) | **YES — new** | QTF / reduce-path (ordering) | Concat-gather is not merge-sorted across shards. QTF/LateMaterialization + reduce-path ordering. One owner. |
| 4 | Value/cell correctness mismatch | 33 (16 not-previously-muted) | mixed | Per-suite triage | Per-suite correctness (under-aggregation, union counts). Triage per suite. |
| 5 | Other / uncategorized | 22 (3 not-previously-muted) | mixed | Triage | Needs manual triage. |
| 6 | Other 500 / backend error | 16 (8 not-previously-muted) | mixed | Triage | Residual 500s — triage individually. |
| 7 | Perf-delegation shape (canSerialize prune) | 13 (0 not-previously-muted) | pre-muted | Filter-delegation | Known fix scoped: `canSerialize` prune in `OpenSearchFilterRule` (separate PR). |
| 8 | Field-not-found / mapping | 6 (0 not-previously-muted) | pre-muted | QA datasets / mapping | Dataset mapping / field-resolution gaps (latency_ms, exception_type, json_test_data). |
| 9 | Type coercion / cast (variadic, RexCall) | 4 (0 not-previously-muted) | pre-muted | Type system | Type-coercion + a RexCall->plan cast bug. |
| 10 | Unmapped AggregateFunction enum/UDAF | 2 (0 not-previously-muted) | pre-muted | Aggregations / UDAF | Map/implement the agg enums (ARG_MIN, DISTINCT_COUNT_APPROX, percentile_approx UDAF). |
| 11 | Streaming fragment failed to start | 2 (1 not-previously-muted) | mixed | Runtime / ffm routing | Residual-filter / ffm routing on object & dynamic-mapping fields. |
| 12 | Frontend type-resolution (UNDEFINED) | 1 (0 not-previously-muted) | pre-muted | opensearch-sql frontend | opensearch-sql frontend type bug (patterns auto-take N). |

## Recommended assignment order

1. **Binary/IP type mismatch** (1 engineer) — single root cause, **170 queries** unlocked by one Substrait↔table type alignment. Biggest ROI. Owner: engine/Substrait.
2. **2-shard ordering / row-position** (1 engineer) — the only **genuinely new** regression class from the 2-shard flip (**65 units**). Concat-gather must merge-sort. Owner: QTF/reduce-path.
3. **Planner: unmarked child [LogicalJoin]** (1 engineer) — 8 join queries 500 on a planner-rule gap; self-contained.
4. **Unmapped AggregateFunction enum/UDAF** + **Value/cell correctness** — split per suite among 1–2 engineers.
5. **Unimplemented functions** — backlog; prioritise from the function-frequency list, do not block on these (per directive: no new scalar functions).


## Detail by bucket

### Binary/IP type mismatch  — 174 units
**Owner:** Substrait / type-mapping  ·  **New under 2-shard:** 0 of 174

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 138
FunctionsPplIT                 █████ 18
SecurityLogsPplIT              ██ 7
MultiIndexQueriesPplIT         ██ 6
ComplexJoinsPplIT              █ 5
```

Examples:
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q1`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q3`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q14`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `SecurityLogsPplIT` → `security_logs:Q3`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).

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
ReverseCommandIT               ██ 2
RangeBucketCommandIT           ██ 2
TableCommandIT                 █ 1
SortCommandIT                  █ 1
SpathCommandIT                 █ 1
StatsCommandIT                 █ 1
WidthBucketCommandIT           █ 1
MinspanBucketCommandIT         █ 1
```

Examples:
- `TableCommandIT` → `testFieldsAndTableEquivalence`: rows from fields vs table expected:<[[FURNITURE, 12.3, 1], [FURNITURE, -12.3, null], [OFFICE SUPPLIES, 15.7, null], [OFFICE SUPPLIES, -15.7, null], [OFFICE SUPPLIES, 0.0, 8], [TECH
- `SortCommandIT` → `testSortByAbsExpression`: Row 0 should be null expected null, but was:<12.3>
- `ReverseCommandIT` → `testReverseAfterFilterFindsUpstreamSort`: Cell mismatch at row 0, col 0 for query: source=calcs | sort int0 | where int0 >= 4 | reverse | head 3 | fields int0: expected <11> but was <4>
- `SpathCommandIT` → `testSpathAutoExtractWithEval`: Cell mismatch at row 0, col 0 for query: source=spath_cmd | spath input=doc | eval name = doc.user.name | fields name expected:<[John]> but was:<[Alice]>

### Value/cell correctness mismatch  — 33 units
**Owner:** Per-suite triage  ·  **New under 2-shard:** 16 of 33

Failing units by suite:
```text
FulltextWindowPplIT            ██████████████████████████████████████ 9
MathFunctionIT                 █████████████████████ 5
HeadCommandIT                  █████████████████ 4
SpanBucketCommandIT            █████████████ 3
RexCommandPplIT                █████████████ 3
RexCommandIT                   ████████ 2
RenameCommandIT                ████ 1
MultiIndexQueriesPplIT         ████ 1
AggregationsPplIT              ████ 1
ComplexJoinsPplIT              ████ 1
KubernetesLogsPplIT            ████ 1
FieldsCommandIT                ████ 1
MultiSourceJoinsPplIT          ████ 1
```

Examples:
- `RenameCommandIT` → `testRenameSingleField`: Row count expected:<3> but was:<6>
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q12`: row 0 col 0: Value mismatch - expected 8, got 5
- `RexCommandIT` → `testRexSedTransliterationVowels`: Row count mismatch for query: source=calcs | where str0='FURNITURE' | rex field=str0 mode=sed "y/AEIOU/aeiou/" | fields str0 | head 1 expected:<1> but was:<2>
- `AggregationsPplIT` → `aggregations:Q1`: row 0 col 1: Value mismatch - expected Engineering, got Sales

### Other / uncategorized  — 22 units
**Owner:** Triage  ·  **New under 2-shard:** 3 of 22

Failing units by suite:
```text
RexCommandPplIT                ██████████████████████████████████████ 7
ComplexJoinsPplIT              ██████████████████████ 4
ExtensiveCoveragePplIT         ████████████████ 3
MathFunctionIT                 ███████████ 2
SortCommandIT                  █████ 1
LookupTableQueriesPplIT        █████ 1
LookupJoinQueriesPplIT         █████ 1
MultiSourceJoinsPplIT          █████ 1
StreamstatsCommandIT           █████ 1
FulltextWindowPplIT            █████ 1
```

Examples:
- `SortCommandIT` → `testSortByAbsTakesNonNullsFromTail`: Row 0 unexpectedly null
- `ComplexJoinsPplIT` → `complex_joins:Q4`: : Row count mismatch - expected 3, got 0
- `ComplexJoinsPplIT` → `complex_joins:Q6`: Unable to convert call DIVIDE(decimal<23,1>, i64).
- `ComplexJoinsPplIT` → `complex_joins:Q8`: Unable to convert call DIVIDE(decimal<21,1>?, i64).

### Other 500 / backend error  — 16 units
**Owner:** Triage  ·  **New under 2-shard:** 8 of 16

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 5
MathFunctionIT                 ██████████████████████████████████████ 5
MathScalarFunctionsIT          ███████████████ 2
ObjectFieldIT                  ███████████████ 2
ArrayFunctionIT                ████████ 1
MultisearchCommandIT           ████████ 1
```

Examples:
- `ArrayFunctionIT` → `testArrayLength`: Execution error: Substrait error: Field 'len' in Substrait schema has a different type (Int32) than the corresponding field in the table schema (Int64).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q82`: filter must be BOOLEAN NOT NULL
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q99`: No backend supports scalar function [filter] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q100`: No backend supports scalar function [transform] among [datafusion]

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
- `ObjectFieldIT` → `testSelectTopLevelObjectFieldWithSiblings`: Field [city] not found.

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
- `DynamicMappingSearchIT` → `testSearchOnDynamicallyAddedFields`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:55396], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { "err

### Frontend type-resolution (UNDEFINED)  — 1 units
**Owner:** opensearch-sql frontend  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
PatternsCommandIT              ██████████████████████████████████████ 1
```

Examples:
- `PatternsCommandIT` → `testSimplePatternAggregationModeMultiShard`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:55396], URI [/_analytics/ppl], status line [HTTP/1.1 500 Internal Server Error] {"err
