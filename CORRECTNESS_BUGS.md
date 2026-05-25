# Correctness Bugs Found by ClickBench Single-Shard vs Multi-Shard Test

## Bug 1: `dc()` (APPROX_COUNT_DISTINCT) produces wrong values in distributed mode

**Queries affected:** Q5, Q9, Q10, Q14

**Symptom:** Multi-shard distinct counts are significantly lower than single-shard.
- Single-shard Q9: `[[11, 16], [10, 274], [9, 189], ...]` (distinct counts: 11, 10, 9, 8...)
- Multi-shard Q9: `[[6, 270], [3, 100], [1, 55], ...]` (distinct counts: 6, 3, 1, 1...)

**Root cause hypothesis:** Each shard computes its LOCAL distinct count (cardinality of its
subset of data) and returns that as the result, instead of emitting the HLL sketch state
for the coordinator to merge.

The `APPROX_COUNT_DISTINCT` function in DataFusion supports partial/final mode:
- Partial: emits binary HLL sketch via `state()` → `Vec<ScalarValue::Binary>`
- Final: merges sketches via `merge_batch(&[BinaryArray])` → emits final Int64 count

The distributed pipeline (`agg_mode.rs`) strips the plan to Partial on shards and Final
at the coordinator. The issue is likely in how the Substrait plan conveys the aggregate
mode, or how the coordinator's Final aggregate receives and interprets the shard output.

**Key files:**
- `sandbox/plugins/analytics-backend-datafusion/rust/src/agg_mode.rs` — partial/final stripping
- `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/planner/dag/DistributedAggregateRewriter.java` — Java-side split
- `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AggregateFunction.java` — `APPROX_COUNT_DISTINCT` intermediate field definition (Binary sketch)
- DataFusion's `approx_distinct.rs` — the actual HLL accumulator with `state_fields`, `state()`, `merge_batch()`

**Investigation path:**
1. Check if the shard-side partial plan actually emits binary sketch state or just emits the final count
2. Check if the coordinator reduce plan correctly declares FINAL mode for the aggregate
3. Check if the Substrait serialization preserves the aggregate mode (Partial vs Final vs Single)
4. Verify the DataFusion reduce session registers the correct aggregate function variant

---

## Bug 2: Sort + Limit (top-N) ordering broken in distributed mode

**Queries affected:** Q11, Q12, Q13, Q15, Q16, Q17, Q22, Q23, Q26, Q31, Q32, Q33, Q36, Q37, Q38

**Symptom:** Multi-shard results have the same data but in completely wrong order (not
properly sorted), or return different rows entirely (Q26).

Examples:
- Q13 single-shard: `[[79, docker tutorial], [75, openai chatgpt], ...]` (sorted by count DESC)
- Q13 multi-shard: `[[73, cheap flights], [65, weather today], ...]` (unsorted — shard results concatenated)

- Q26 single-shard: `[[best restaurants], [best restaurants], ...]` (sorted alphabetically, first 10)
- Q26 multi-shard: `[[cheap flights], [docker tutorial], ...]` (each shard's local top-10 concatenated)

**Root cause hypothesis:** The coordinator reduce is NOT applying a final merge-sort before
the global limit. Each shard applies its own local sort+limit and returns its top-N rows.
The coordinator concatenates these without re-sorting, then the client just takes the first
N rows from the concatenated (unordered) result.

In a correct distributed top-N:
1. Each shard: sort locally, emit top-N
2. Coordinator: merge-sort all shard outputs, take global top-N

The current behavior appears to be:
1. Each shard: sort locally, emit top-N
2. Coordinator: concatenate shard outputs, take first N (wrong!)

**Key files:**
- `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/planner/dag/DAGBuilder.java` — where the plan is cut at exchange boundaries
- `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java` — coordinator execution
- The Substrait reduce plan — does it include a Sort operator above the StageInputScan?
- `sandbox/plugins/analytics-backend-datafusion/rust/src/local_executor.rs` — coordinator reduce execution

**Investigation path:**
1. Examine the QueryDAG structure for a sort+limit query — is the Sort in the root stage (coordinator) or the child stage (shard)?
2. Check if the reduce Substrait plan includes a SortExec
3. If Sort is only on the shard side, the coordinator reduce has no way to re-sort after merging
4. The fix likely requires ensuring Sort+Limit queries have the Sort duplicated at the coordinator stage (or use a streaming merge-sort in the exchange)
