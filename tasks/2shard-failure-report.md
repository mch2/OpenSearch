# Analytics-Engine QA — 2-Shard Failure Report

_Branch `2shard-failure-buckets` (all `@AwaitsFix` mutes removed) merged with **latest upstream/main `dee5928a39f`**, which includes #21936 "Fix pure LIMIT: gather to coordinator before applying fetch". DatasetProvisioner defaults to **2 shards + lucene secondary**. Native lib rebuilt. Full `:sandbox:qa:analytics-engine-rest:integTest`._

**Key delta vs the pre-#21936 run:** the **Limit/row-count doubling** bucket collapsed from **~137 → 5** — the CBO sort/limit fix landed and largely eliminated `fetch × partitions` row-count doubling. Those failures didn't vanish; they **shifted** into **2-shard ordering (63)** and **Value/cell correctness (140)** — queries now return the correct row *count* but wrong *order/values*, because the coordinator concat-gather is still not merge-sorted across shards (the next fix to land). So the dominant remaining work is the gather-ordering / merge path, not limit-pushdown._

## Headline

- **774 test methods**: 644 passed, 128 failed, 2 skipped.
- Expanded per-query (monolithic `*PplIT` suites run every `q*.ppl` in one method): **349 individual failing queries / IT methods**.
- The `*PplIT` suites were muted wholesale on a single `"Failing due to unsupported operations"` tag; unmuting them surfaces every underlying per-query failure below.

## Failures by bucket (overview)

```text
Value/cell correctness mismatc ██████████████████████████████████████ 140
2-shard ordering / row-positio █████████████████ 63
Unimplemented scalar function  ████████████ 44
Other 500 / backend error      ████████████ 43
Receiver dropped before send ( ███████ 26
Field-not-found / mapping      ██ 6
Other / uncategorized          ██ 6
Calcite->Substrait conversion  ██ 6
Limit/row-count doubling (limi █ 5
Type coercion / cast (variadic █ 4
Unmapped AggregateFunction enu █ 2
Streaming fragment failed to s █ 2
Other 4xx invalid query        █ 1
Frontend type-resolution (UNDE █ 1
```

## Failure buckets (assign one owner per bucket)

_Owner = suggested area; replace with a teammate's name._

| # | Bucket | Failing units | New under 2-shard? | Owner | Suggested action |
|---|--------|---------------|--------------------|-------|------------------|
| 1 | Value/cell correctness mismatch | 140 (4 not-previously-muted) | mixed | Per-suite triage | Per-suite correctness (under-aggregation, union counts). Triage per suite. |
| 2 | 2-shard ordering / row-position | 63 (63 not-previously-muted) | **YES — new** | QTF / reduce-path (ordering) | Concat-gather is not merge-sorted across shards. QTF/LateMaterialization + reduce-path ordering. One owner. |
| 3 | Unimplemented scalar function | 44 (0 not-previously-muted) | pre-muted | Backlog — unassigned | Backlog / won't-fix per directive (no new functions). Triage the function list below for priority. |
| 4 | Other 500 / backend error | 43 (29 not-previously-muted) | mixed | Triage | Residual 500s — triage individually. |
| 5 | Receiver dropped before send (Cause A) | 26 (6 not-previously-muted) | fixed | (fixed — verify) | Should be resolved by the Cause-A sentinel; verify none remain. |
| 6 | Field-not-found / mapping | 6 (0 not-previously-muted) | pre-muted | QA datasets / mapping | Dataset mapping / field-resolution gaps (latency_ms, exception_type, json_test_data). |
| 7 | Other / uncategorized | 6 (5 not-previously-muted) | mixed | Triage | Needs manual triage. |
| 8 | Calcite->Substrait conversion (convert/cast) | 6 (0 not-previously-muted) | ? | Triage | triage |
| 9 | Limit/row-count doubling (limit pushdown — other team) | 5 (0 not-previously-muted) | ? | Triage | triage |
| 10 | Type coercion / cast (variadic, RexCall) | 4 (0 not-previously-muted) | pre-muted | Type system | Type-coercion + a RexCall->plan cast bug. |
| 11 | Unmapped AggregateFunction enum/UDAF | 2 (0 not-previously-muted) | pre-muted | Aggregations / UDAF | Map/implement the agg enums (ARG_MIN, DISTINCT_COUNT_APPROX, percentile_approx UDAF). |
| 12 | Streaming fragment failed to start | 2 (1 not-previously-muted) | mixed | Runtime / ffm routing | Residual-filter / ffm routing on object & dynamic-mapping fields. |
| 13 | Other 4xx invalid query | 1 (0 not-previously-muted) | ? | Triage | triage |
| 14 | Frontend type-resolution (UNDEFINED) | 1 (0 not-previously-muted) | pre-muted | opensearch-sql frontend | opensearch-sql frontend type bug (patterns auto-take N). |

## Recommended assignment order

1. **Binary/IP type mismatch** (1 engineer) — single root cause, **170 queries** unlocked by one Substrait↔table type alignment. Biggest ROI. Owner: engine/Substrait.
2. **2-shard ordering / row-position** (1 engineer) — the only **genuinely new** regression class from the 2-shard flip (**65 units**). Concat-gather must merge-sort. Owner: QTF/reduce-path.
3. **Planner: unmarked child [LogicalJoin]** (1 engineer) — 8 join queries 500 on a planner-rule gap; self-contained.
4. **Unmapped AggregateFunction enum/UDAF** + **Value/cell correctness** — split per suite among 1–2 engineers.
5. **Unimplemented functions** — backlog; prioritise from the function-frequency list, do not block on these (per directive: no new scalar functions).


## Detail by bucket

### Value/cell correctness mismatch  — 140 units
**Owner:** Per-suite triage  ·  **New under 2-shard:** 4 of 140

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 100
FunctionsPplIT                 ███ 8
FulltextWindowPplIT            ███ 8
LookupJoinQueriesPplIT         ██ 5
AggregationsPplIT              ██ 4
RexCommandPplIT                ██ 4
SecurityLogsPplIT              █ 3
MathFunctionIT                 █ 3
ComplexJoinsPplIT              █ 2
MultiIndexQueriesPplIT         █ 1
IndexPatternUnionIT            █ 1
MultiSourceJoinsPplIT          █ 1
```

Examples:
- `MultiIndexQueriesPplIT` → `multi_index_queries:Q1`: row 2 col 0: Value mismatch - expected jdoe, got asmith
- `SecurityLogsPplIT` → `security_logs:Q2`: row 0 col 1: Value mismatch - expected [blocked, denied, rejected], got [blocked, rejected, denied]
- `SecurityLogsPplIT` → `security_logs:Q6`: row 0 col 1: Value mismatch - expected 19, got 21
- `SecurityLogsPplIT` → `security_logs:Q10`: row 0 col 1: Value mismatch - expected 3, got 5

### 2-shard ordering / row-position  — 63 units
**Owner:** QTF / reduce-path (ordering)  ·  **New under 2-shard:** 63 of 63

Failing units by suite:
```text
StreamstatsCommandIT           ██████████████████████████████████████ 33
FillNullCommandIT              █████████████ 11
EvalCommandIT                  █████ 4
SpathCommandIT                 ███ 3
SpanBucketCommandIT            ███ 3
RangeBucketCommandIT           ██ 2
WidthBucketCommandIT           ██ 2
TableCommandIT                 █ 1
HeadCommandIT                  █ 1
StatsCommandIT                 █ 1
FieldsCommandIT                █ 1
MinspanBucketCommandIT         █ 1
```

Examples:
- `TableCommandIT` → `testFieldsAndTableEquivalence`: rows from fields vs table expected:<[[FURNITURE, 12.3, 1], [FURNITURE, -12.3, null], [OFFICE SUPPLIES, 15.7, null]]> but was:<[[OFFICE SUPPLIES, -15.7, null], [OFFICE SUPPLIES, 0.0
- `SpathCommandIT` → `testSpathAutoExtractWithOutput`: Cell mismatch at row 0, col 0 for query: source=spath_simple | spath input=doc output=result expected:<{"n": [1]}> but was:<{"n": [2]}>
- `RangeBucketCommandIT` → `testBinStartEndPreservesNullsInNullableField`: Cell mismatch at row 1, col 0 for query: source=calcs | bin num0 start=-100 end=100 | fields num0 | head 10 expected:<[-100-]0> but was:<[0-10]0>
- `RangeBucketCommandIT` → `testBinStartEndBucketsValuesIntoExpandedRange`: Cell mismatch at row 3, col 0 for query: source=calcs | bin num1 start=0 end=100 | fields num1 | head 5 expected:<[0-1]0> but was:<[10-2]0>

### Unimplemented scalar function  — 44 units
**Owner:** Backlog — unassigned  ·  **New under 2-shard:** 0 of 44

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 43
LookupTableQueriesPplIT        █ 1
```

Examples:
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q19`: No backend supports scalar function [JSON_OBJECT] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q20`: No backend supports scalar function [SPLIT] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q29`: No backend supports scalar function [DATE_SUB] among [datafusion]
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q58`: No backend supports scalar function [SPLIT] among [datafusion]

**Distinct functions (per-query hits):** `SPLIT`×9, `JSON_OBJECT`×2, `CASE`×2, `DATE_ADD`×1, `DATE_SUB`×1, `JSON_ARRAY`×1, `IS JSON VALUE`×1, `ARRAY_COMPACT`×1, `forall`×1, `exists`×1, `filter`×1, `transform`×1, `reduce`×1, `ADDDATE`×1, `ADDTIME`×1, `DATEDIFF`×1, `DAYNAME`×1, `FROM_DAYS`×1, `GET_FORMAT`×1, `LAST_DAY`×1, `MONTHNAME`×1, `PERIOD_ADD`×1, `PERIOD_DIFF`×1, `SEC_TO_TIME`×1, `SUBDATE`×1, `SUBTIME`×1, `TIME_TO_SEC`×1, `TIME_DIFF`×1, `TO_DAYS`×1, `TO_SECONDS`×1, `UTC_DATE`×1, `UTC_TIME`×1, `UTC_TIMESTAMP`×1, `WEEKDAY`×1, `YEARWEEK`×1, `JSON`×1, `RINT`×1, `GROK`×1

### Other 500 / backend error  — 43 units
**Owner:** Triage  ·  **New under 2-shard:** 29 of 43

Failing units by suite:
```text
MultiIndexQueryShapesIT        ██████████████████████████████████████ 17
ExtensiveCoveragePplIT         ███████████ 5
DataStreamIT                   ███████████ 5
ComplexRegexPplIT              ███████████ 5
IndexPatternUnionIT            █████████ 4
AliasIT                        ███████ 3
AppLogsPplIT                   ████ 2
ObjectFieldIT                  ████ 2
```

Examples:
- `AppLogsPplIT` → `app_logs:Q5`: failed: method [POST], host [http://127.0.0.1:54251], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { "error": { "reason": "There was i
- `MultiIndexQueryShapesIT` → `testDynamicMappingUnionAcrossIndices`: org.opensearch.client.ResponseException: method [POST], host [http://127.0.0.1:54251], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { 
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q82`: filter must be BOOLEAN NOT NULL
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q99`: No backend supports scalar function [filter] among [datafusion]

### Receiver dropped before send (Cause A)  — 26 units
**Owner:** (fixed — verify)  ·  **New under 2-shard:** 6 of 26

Failing units by suite:
```text
ExtensiveCoveragePplIT         ██████████████████████████████████████ 13
RexCommandPplIT                ████████████ 4
FunctionsPplIT                 █████████ 3
MathFunctionIT                 ██████ 2
ConditionalFunctionsIT         ███ 1
ArrayFunctionIT                ███ 1
MVAppendFunctionIT             ███ 1
ObjectFieldIT                  ███ 1
```

Examples:
- `ConditionalFunctionsIT` → `testIsNullInProject`: Execution error: partition stream receiver dropped before send
- `FunctionsPplIT` → `functions:Q3`: Execution error: partition stream receiver dropped before send
- `FunctionsPplIT` → `functions:Q5`: Execution error: partition stream receiver dropped before send
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q25`: Execution error: partition stream receiver dropped before send

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

### Other / uncategorized  — 6 units
**Owner:** Triage  ·  **New under 2-shard:** 5 of 6

Failing units by suite:
```text
MultiIndexQueryShapesIT        ██████████████████████████████████████ 2
AliasIT                        ██████████████████████████████████████ 2
StreamstatsCommandIT           ███████████████████ 1
DataStreamIT                   ███████████████████ 1
```

Examples:
- `MultiIndexQueryShapesIT` → `testAliasTypeMismatchIsRejected`: expected to contain [incompatible field types] but was: { "error": { "context": { "stage": "executing", "stage_description": "Running the query" }, "reason": "java.sql.SQLException
- `AliasIT` → `testFilterAliasIsRejected`: Expected failure but got: {schema=[{name=c, type=bigint}], total=1, datarows=[[0]], size=1}
- `StreamstatsCommandIT` → `testWhereInWithStreamstatsSubquery`: Expected query [source=calcs | where key in [ source=calcs | streamstats count() as cnt | where cnt < 5 | fields key ] | head 1] to fail but got: {schema=[{name=bool0, type=boolean

### Calcite->Substrait conversion (convert/cast)  — 6 units
**Owner:** Triage  ·  **New under 2-shard:** 0 of 6

Failing units by suite:
```text
ComplexJoinsPplIT              ██████████████████████████████████████ 3
ExtensiveCoveragePplIT         ██████████████████████████████████████ 3
```

Examples:
- `ComplexJoinsPplIT` → `complex_joins:Q6`: Unable to convert call DIVIDE(decimal<23,1>, i64).
- `ComplexJoinsPplIT` → `complex_joins:Q8`: Unable to convert call DIVIDE(decimal<21,1>?, i64).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q149`: Unable to convert call TIMESTAMPADD(string, i32, precision_timestamp<0>?).
- `ExtensiveCoveragePplIT` → `extensive_coverage:Q163`: Unable to convert call json_set(string?, string, i32).

### Limit/row-count doubling (limit pushdown — other team)  — 5 units
**Owner:** Triage  ·  **New under 2-shard:** 0 of 5

Failing units by suite:
```text
RexCommandPplIT                ██████████████████████████████████████ 2
ComplexJoinsPplIT              ███████████████████ 1
LookupJoinQueriesPplIT         ███████████████████ 1
MultiSourceJoinsPplIT          ███████████████████ 1
```

Examples:
- `ComplexJoinsPplIT` → `complex_joins:Q4`: : Row count mismatch - expected 3, got 0
- `LookupJoinQueriesPplIT` → `lookup_join_queries:Q1`: : Row count mismatch - expected 20, got 19
- `MultiSourceJoinsPplIT` → `multi_source_joins:Q5`: : Row count mismatch - expected 7, got 9
- `RexCommandPplIT` → `rex_command:Q5`: : Row count mismatch - expected 5, got 6

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
- `DynamicMappingSearchIT` → `testSearchOnDynamicallyAddedFields`: org.opensearch.client.ResponseException: method [POST], host [http://[::1]:54200], URI [/_plugins/_ppl], status line [HTTP/1.1 500 Internal Server Error] { "err

### Other 4xx invalid query  — 1 units
**Owner:** Triage  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
LookupTableQueriesPplIT        ██████████████████████████████████████ 1
```

Examples:
- `LookupTableQueriesPplIT` → `lookup_table_queries:Q7`: failed: method [POST], host [http://127.0.0.1:54201], URI [/_plugins/_ppl], status line [HTTP/1.1 400 Bad Request] {"error":{"root_cause":[{"type":"illegal_argu

### Frontend type-resolution (UNDEFINED)  — 1 units
**Owner:** opensearch-sql frontend  ·  **New under 2-shard:** 0 of 1

Failing units by suite:
```text
PatternsCommandIT              ██████████████████████████████████████ 1
```

Examples:
- `PatternsCommandIT` → `testSimplePatternAggregationModeMultiShard`: org.opensearch.client.ResponseException: method [POST], host [http://127.0.0.1:54201], URI [/_analytics/ppl], status line [HTTP/1.1 500 Internal Server Error] {
