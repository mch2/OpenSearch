# Analytics-Engine QA — 2-Shard Failure Report

_Branch `report-2shard-all-unmuted` — DatasetProvisioner defaulted to **2 shards**, all 39 `@AwaitsFix` mutes removed, with the 4 PR'd fixes + CBO-2x + Cause-A sentinel applied. Native lib rebuilt. Full `:sandbox:qa:analytics-engine-rest:integTest`._

## Headline

- **768 test methods**: 663 passed, 105 failed, 0 skipped.
- Expanded per-query (monolithic `*PplIT` suites run every `q*.ppl` in one method): **374 individual failing queries / IT methods**.
- The `*PplIT` suites were muted wholesale on a single `"Failing due to unsupported operations"` tag; unmuting them surfaces every underlying per-query failure below.

## Failure buckets (assign one owner per bucket)

| # | Bucket | Failing units | New under 2-shard? | Suggested action |
|---|--------|---------------|--------------------|------------------|
| 1 | Binary/IP type mismatch | 170 (0 not-previously-muted) | mostly pre-muted | **One engine fix** — Substrait emits `Binary` but the parquet table schema is `BinaryView` for `ip`/`binary` fields. Align the type mapping. Highest leverage. |
| 2 | 2-shard ordering / row-position | 65 (65 not-previously-muted) | **YES — new** | Concat-gather is not merge-sorted across shards. QTF/LateMaterialization + reduce-path ordering. One owner. |
| 3 | Unimplemented scalar function | 51 (0 not-previously-muted) | pre-muted | Backlog / won't-fix per directive (no new functions). Triage the function list below for priority. |
| 4 | Value/cell correctness mismatch | 28 (4 not-previously-muted) | mixed | Per-suite correctness (under-aggregation, union counts). Triage per suite. |
| 5 | Perf-delegation shape (canSerialize prune) | 13 (0 not-previously-muted) | pre-muted | Known fix scoped: `canSerialize` prune in `OpenSearchFilterRule` (separate PR). |
| 6 | Other / uncategorized | 10 (0 not-previously-muted) | mixed | Needs manual triage. |
| 7 | Unmapped AggregateFunction enum/UDAF | 9 (0 not-previously-muted) | pre-muted | Map/implement the agg enums (ARG_MIN, DISTINCT_COUNT_APPROX, percentile_approx UDAF). |
| 8 | Planner: unmarked child [LogicalJoin] | 8 (0 not-previously-muted) | mixed | Planner bug — Project/Sort rule hits an unmarked `LogicalJoin` child. Defer/mark fix. |
| 9 | Other 500 / backend error | 7 (0 not-previously-muted) | mixed | Residual 500s — triage individually. |
| 10 | Field-not-found / mapping | 6 (0 not-previously-muted) | pre-muted | Dataset mapping / field-resolution gaps (latency_ms, exception_type, json_test_data). |
| 11 | Type coercion / cast (variadic, RexCall) | 4 (0 not-previously-muted) | pre-muted | Type-coercion + a RexCall->plan cast bug. |
| 12 | Streaming fragment failed to start | 2 (1 not-previously-muted) | mixed | Residual-filter / ffm routing on object & dynamic-mapping fields. |
| 13 | Frontend type-resolution (UNDEFINED) | 1 (0 not-previously-muted) | pre-muted | opensearch-sql frontend type bug (patterns auto-take N). |

## Recommended assignment order

1. **Binary/IP type mismatch** (1 engineer) — single root cause, **170 queries** unlocked by one Substrait↔table type alignment. Biggest ROI. Owner: engine/Substrait.
2. **2-shard ordering / row-position** (1 engineer) — the only **genuinely new** regression class from the 2-shard flip (**65 units**). Concat-gather must merge-sort. Owner: QTF/reduce-path.
3. **Planner: unmarked child [LogicalJoin]** (1 engineer) — 8 join queries 500 on a planner-rule gap; self-contained.
4. **Unmapped AggregateFunction enum/UDAF** + **Value/cell correctness** — split per suite among 1–2 engineers.
5. **Unimplemented functions** — backlog; prioritise from the function-frequency list, do not block on these (per directive: no new scalar functions).


## Detail by bucket

### Binary/IP type mismatch  — 170 units
Suites: `ExtensiveCoveragePplIT`×138, `FunctionsPplIT`×18, `SecurityLogsPplIT`×7, `MultiIndexQueriesPplIT`×6, `ComplexJoinsPplIT`×1
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q1`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q3`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q14`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).
- `SecurityLogsPplIT` → `security_logs:Q3`: Substrait error: Field 'destination_ip' in Substrait schema has a different type (Binary) than the corresponding field in the table schema (BinaryView).

### 2-shard ordering / row-position  — 65 units
Suites: `StreamstatsCommandIT`×31, `FillNullCommandIT`×11, `SpathCommandIT`×4, `EvalCommandIT`×4, `SortCommandIT`×2, `ReverseCommandIT`×2, `RangeBucketCommandIT`×2, `WidthBucketCommandIT`×2, `SpanBucketCommandIT`×2, `TableCommandIT`×1, `HeadCommandIT`×1, `StatsCommandIT`×1, `FieldsCommandIT`×1, `MinspanBucketCommandIT`×1
- `TableCommandIT` → `testFieldsAndTableEquivalence`: rows from fields vs table expected:<[[FURNITURE, 12.3, 1], [FURNITURE, -12.3, null], [OFFICE SUPPLIES, 15.7, null]]> but was:<[[OFFICE SUPPLIES, -15.7, null], [OFFICE SUPPLIES, 3.5
- `SortCommandIT` → `testSortByAbsTakesNonNullsFromTail`: abs(num0) sorted value at row 0 expected:<0.0> but was:<12.3>
- `SortCommandIT` → `testSortByAbsExpression`: Row 0 should be null expected null, but was:<12.3>
- `ReverseCommandIT` → `testReverseAfterFilterFindsUpstreamSort`: Cell mismatch at row 0, col 0 for query: source=calcs | sort int0 | where int0 >= 4 | reverse | head 3 | fields int0: expected <11> but was <4>

### Unimplemented scalar function  — 51 units
Suites: `ExtensiveCoveragePplIT`×43, `ComplexRegexPplIT`×5, `AppLogsPplIT`×2, `LookupTableQueriesPplIT`×1
- `AppLogsPplIT` → `app_logs:Q5`: No backend supports scalar function [GROK] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q19`: No backend supports scalar function [JSON_OBJECT] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q29`: No backend supports scalar function [DATE_SUB] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q58`: No backend supports scalar function [SPLIT] among [datafusion]

**Distinct functions (per-query hits):** `SPLIT`×9, `GROK`×8, `percentile_approx`×7, `JSON_OBJECT`×2, `CASE`×2, `DATE_ADD`×1, `DATE_SUB`×1, `JSON_ARRAY`×1, `IS JSON VALUE`×1, `ARRAY_COMPACT`×1, `forall`×1, `exists`×1, `filter`×1, `transform`×1, `reduce`×1, `ADDDATE`×1, `ADDTIME`×1, `DATEDIFF`×1, `DAYNAME`×1, `FROM_DAYS`×1, `GET_FORMAT`×1, `LAST_DAY`×1, `MONTHNAME`×1, `PERIOD_ADD`×1, `PERIOD_DIFF`×1, `SEC_TO_TIME`×1, `SUBDATE`×1, `SUBTIME`×1, `TIME_TO_SEC`×1, `TIME_DIFF`×1, `TO_DAYS`×1, `TO_SECONDS`×1, `UTC_DATE`×1, `UTC_TIME`×1, `UTC_TIMESTAMP`×1, `WEEKDAY`×1, `YEARWEEK`×1, `JSON`×1, `RINT`×1

### Value/cell correctness mismatch  — 28 units
Suites: `FulltextWindowPplIT`×9, `LookupJoinQueriesPplIT`×5, `RexCommandPplIT`×5, `MathFunctionIT`×4, `MultiIndexQueriesPplIT`×1, `AggregationsPplIT`×1, `ComplexJoinsPplIT`×1, `KubernetesLogsPplIT`×1, `MultiSourceJoinsPplIT`×1
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q12`: row 0 col 0: Value mismatch - expected 8, got 5
- `AggregationsPplIT` → `aggregations:Q1`: row 0 col 1: Value mismatch - expected Engineering, got Marketing
- `ComplexJoinsPplIT` → `complex_joins:Q9`: row 0 col 0: Value mismatch - expected 1, got 24
- `KubernetesLogsPplIT` → `kubernetes_logs:Q9`: row 0 col 0: Value mismatch - expected 6, got 4

### Perf-delegation shape (canSerialize prune)  — 13 units
Suites: `ReplaceCommandIT`×8, `RexCommandIT`×3, `WhereCommandIT`×1, `SpathCommandIT`×1
- `RexCommandIT` → `testRexSedReplaceCaseInsensitive`: EQUALS performance-delegation requires (RexInputRef, RexLiteral); got REGEXP_REPLACE($20, 'furniture', 'FURN', 'i') = 'FURN':VARCHAR

### Other / uncategorized  — 10 units
Suites: `ExtensiveCoveragePplIT`×3, `RexCommandPplIT`×2, `ComplexJoinsPplIT`×1, `LookupTableQueriesPplIT`×1, `LookupJoinQueriesPplIT`×1, `MultiSourceJoinsPplIT`×1, `StreamstatsCommandIT`×1
- `ComplexJoinsPplIT` → `complex_joins:Q4`: : Row count mismatch - expected 3, got 0
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q149`: Unable to convert call TIMESTAMPADD(string, i32, precision_timestamp<0>?).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q150`: Unable to convert call TIMESTAMPDIFF(string, date?, precision_timestamp<0>?).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q163`: Unable to convert call json_set(string?, string, i32).

### Unmapped AggregateFunction enum/UDAF  — 9 units
Suites: `AggregationsPplIT`×4, `ExtensiveCoveragePplIT`×4, `SecurityLogsPplIT`×1
- `SecurityLogsPplIT` → `security_logs:Q8`: No enum constant org.opensearch.analytics.spi.AggregateFunction.ARG_MIN
- `AggregationsPplIT` → `aggregations:Q7`: No backend supports aggregate function [percentile_approx]
- `AggregationsPplIT` → `aggregations:Q9`: No backend supports aggregate function [percentile_approx]
- `AggregationsPplIT` → `aggregations:Q10`: No backend supports aggregate function [percentile_approx]

### Planner: unmarked child [LogicalJoin]  — 8 units
Suites: `ComplexJoinsPplIT`×7, `ExtensiveCoveragePplIT`×1
- `ComplexJoinsPplIT` → `complex_joins:Q1`: Project rule encountered unmarked child [LogicalJoin]
- `ComplexJoinsPplIT` → `complex_joins:Q2`: Project rule encountered unmarked child [LogicalJoin]
- `ComplexJoinsPplIT` → `complex_joins:Q3`: Project rule encountered unmarked child [LogicalJoin]
- `ComplexJoinsPplIT` → `complex_joins:Q8`: Project rule encountered unmarked child [LogicalJoin]

### Other 500 / backend error  — 7 units
Suites: `ExtensiveCoveragePplIT`×4, `ObjectFieldIT`×2, `MultisearchCommandIT`×1
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q98`: No backend supports scalar function [exists] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q99`: No backend supports scalar function [filter] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q100`: No backend supports scalar function [transform] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q101`: No backend supports scalar function [reduce] among [datafusion]

### Field-not-found / mapping  — 6 units
Suites: `MultiIndexQueriesPplIT`×3, `ObjectFieldIT`×2, `ExtensiveCoveragePplIT`×1
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q2`: Field [latency_ms] not found.
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q7`: Field [exception_type] not found.
- `ObjectFieldIT` → `testSelectTopLevelObjectFieldWithSiblings`: Field [city] not found.

### Type coercion / cast (variadic, RexCall)  — 4 units
Suites: `SecurityLogsPplIT`×2, `ExtensiveCoveragePplIT`×2
- `SecurityLogsPplIT` → `security_logs:Q5`: class org.apache.calcite.rex.RexCall cannot be cast to class org.opensearch.analytics.planner.rel.AnnotatedPredicate (org.apache.calcite.rex.RexCall and org.opensearch.analytics.pl
- `SecurityLogsPplIT` → `security_logs:Q7`: class org.apache.calcite.rex.RexCall cannot be cast to class org.opensearch.analytics.planner.rel.AnnotatedPredicate (org.apache.calcite.rex.RexCall and org.opensearch.analytics.pl
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q39`: Variadic arguments must have consistent types when parameterConsistency is CONSISTENT. Argument at index 1 has type FixedChar{nullable=false, length=1} but argument at index 2 has 

### Streaming fragment failed to start  — 2 units
Suites: `DynamicMappingSearchIT`×1, `ObjectFieldIT`×1
- `DynamicMappingSearchIT` → `testSearchOnDynamicallyAddedFields`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:51818], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { "err

### Frontend type-resolution (UNDEFINED)  — 1 units
Suites: `PatternsCommandIT`×1
- `PatternsCommandIT` → `testSimplePatternAggregationModeMultiShard`: org.opensearch.client.ResponseException: method [POST], host [http://127.0.0.1:51826], URI [/_analytics/ppl], status line [HTTP/1.1 500 Internal Server Error] {
