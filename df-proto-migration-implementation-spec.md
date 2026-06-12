# Implementation Spec: datafusion-proto at Stage Boundaries (Calcite Keeps the Cut)

**Audience:** implementing agent. Every decision in this document is final. There are no open questions. If a step appears to require deviating from a decision or touching a file on the DO-NOT-TOUCH list, stop and report — do not improvise.

**Scope:** `sandbox/plugins/analytics-engine`, `sandbox/plugins/analytics-backend-datafusion`, `sandbox/libs/analytics-framework`.

---

## 1. Mission

Replace the coordinator↔data-node stage boundary format. Today each stage ships a **Substrait logical fragment plus side channels** (instruction lists, delegation descriptor, schema stubs, post-decoration schemas) assembled by layered byte-stitching in `FragmentConversionDriver`, with the data node re-deriving schemas and force-rewriting aggregate modes at execution time. After this migration each stage ships **one serialized DataFusion physical plan** (`datafusion-proto` `PhysicalPlanNode` + custom codec), finalized at the coordinator. The plan that ships is the plan that runs; data nodes decode, bind shard readers, and execute — no rewriting.

**Unchanged by explicit decision:** Calcite owns all distribution decisions (split rules, `DistributedAggregateRewriter`, `OpenSearchExchangeReducer`, `DAGBuilder`, `QueryDAG`); the Java scheduler/transport/actions; per-shard request fan-out; inter-stage Arrow partition streams; the entire Lucene delegation framework (§3).

## 1.1 Mental model (read this first)

- **The cut happens first, in Calcite.** `DAGBuilder` slices the RelNode tree at `ExchangeReducer`s exactly as today. The ER is a Calcite-only cut marker: `DAGBuilder` cuts at it, the stripper drops it, and it never reaches Rust. There is no whole-query physical plan and no cutting of physical plans.
- **Substrait survives only as the in-process Calcite→DataFusion translation, per fragment, at the coordinator.** One whole-fragment isthmus pass per stage. It never crosses the network (Phase 2b onward) and never touches a data node.
- **Physical plans are built by DataFusion's planner, never by hand.** Per fragment: `from_substrait_plan` → logical → `create_physical_plan`, then fix-ups: leaf swap (`TableScan` → `OpenSearchShardScanExec`; `StageInputScan` → `StageReadExec`) and, for split aggregates, the **graft** (§4.1) — the shard fragment's planning yields a `Final ← Partial` pair; the Partial half ships as the shard stage, the retained Final half replaces the reduce stage's aggregation subtree. The partial→final boundary is therefore typed by DataFusion's own pair contract, never by Calcite's declared rowType and never by schema mapping.
- **N stages → N independent Substrait plans → N independent physical plans → N independent proto blobs. Plans are never stitched, merged, or assembled into a whole-query tree.** `finalize_query_plan` takes all stages in one call only for (a) child-before-parent ordering and (b) carrying the child's Partial-half schema (and, for agg boundaries, the retained Final half) into the parent's fix-up. At runtime, plans connect through data — the existing Arrow partition stream (`input-<stageId>`) — never through plan structure.
- **The only cross-stage coupling is deliberate:** stages finalize child-first in one session so the reduce stage's input schema is copied off the shard stage's actual physical output (§4.1).

## 2. Locked decisions

| # | Decision | Resolution |
|---|---|---|
| D1 | Stage cutting | Stays in Calcite/`DAGBuilder`. No DataFusion-side boundary insertion, no datafusion-distributed dependency. The only new crate dependency is `datafusion-proto`, pinned to the workspace DataFusion version. |
| D2 | Wire format | `datafusion-proto` `PhysicalPlanNode` encoded with `OpenSearchExtensionCodec` (composes `DefaultPhysicalExtensionCodec`). |
| D3 | Where physical plans are produced | Coordinator-side Rust (`stage_finalizer.rs`), bottom-up over the stage DAG in one session. The coordinator already loads the `.so` (it executes reduce stages locally today) — no deployment change. |
| D4 | Aggregate modes | Explicit `StageMeta.agg_mode` (`NONE\|PARTIAL\|FINAL`) read from `OpenSearchAggregate.getMode()` in Java. The finalizer applies it by **relocating** `agg_mode.rs::force_aggregate_mode` (move the code; do not rewrite it). `CombinePartialFinalAggregate` stays removed from the finalizer's optimizer rules (Calcite did the split deliberately). Delete `forceAggregatePhase` from `DataFusionFragmentConvertor`. |
| D5 | Boundary schemas | `StageReadExec.schema` := the already-finalized child stage plan's actual `.schema()` (mechanics in §4.1). Exception, Phase 2a only: while shard stages are still legacy-format, compute the child schema by lowering the child's Substrait coordinator-side at plan time (relocate `derive_schema_from_partial_plan` from `api.rs` for this transitional case; it is deleted with Phase 4 group 1). Calcite's declared rowType is an assertion, not the source of truth. |
| D6 | Schema mismatch policy | Two assertion points. (a) Non-agg boundaries (`agg_mode=NONE`): at `StageReadExec`, declared rowType vs child schema — nullable-widening is the only silent acceptance; anything else hard-errors with both schemas and the stage id. (b) Agg boundaries: asserted at the **top of the graft** (§4.1) — grafted-Final output ≡ replaced-subtree output, positions and types; rename projection permitted for name-only drift. Calcite's declared rowType is **never** asserted against partial state schemas (arity legitimately differs). No other coercion anywhere. |
| D7 | Engine-native-merge aggregates | Applies **only** to Lucene-engine-native aggregates whose partial state is computed inside Lucene and crosses the JNI boundary: implement as DataFusion UDAFs with a single opaque binary state column (`Accumulator::state()` → one `ScalarValue::Binary`; `merge_batch` deserializes and merges). Pure-DataFusion aggregates (`approx_distinct`, `approx_percentile_cont`, all native and custom Rust UDAFs) use their **native state schemas with no constraints** — multi-column states included — via the §4.1 graft. |
| D8 | Version safety | `FragmentExecutionRequest` (DF_PROTO form) carries `planFormatVersion: int` (start at 1) and `dataFusionVersion: String`. Data node compares against its own; mismatch → typed `PlanFormatMismatchException`. Coordinator handling: while the legacy format exists, catch and re-plan the query on the legacy path; after Phase 4 deletes legacy, fail the query with the version pair in the message. No other negotiation mechanism. |
| D9 | Consumer-quirk normalization | One place: a single ordered fix-up pass in `stage_finalizer.rs`, run after physical planning, before leaf rewrite. Seed it with the relocated reorder-Project/FINAL workaround and any rewrites currently in `SubstraitPlanProtoRewriter` that target consumer output. Java-side adapters (`*Adapter.java`) stay where they are — they fix Calcite→Substrait, which still happens. |
| D10 | Late materialization | LM stage execution (QTF scatter/gather/stitch, `Stitcher`, `LateMaterializationStageExecution`) is untouched. The LM stage gets no Rust plan. Its parent's `StageReadExec` schema comes from `StageMeta.lm_output_row_type` (the wrapper's rowType, declared by Java) — the one boundary where the declared type is the source of truth. Delete `convertLateMaterializationStage` and its stub Read. |
| D11 | Explain/debug | `StagePlan` permanently retains the stage's Substrait bytes as an optional coordinator-side `debugSubstrait` field for explain/profile output. It is never shipped in DF_PROTO requests. Re-point `FragmentExecutionStats` / `AnalyticsFragmentSlowLog` fields (`usedSecondaryIndex`, `delegatedPredicateCount`, `filterTreeShape`) to read from the scan-node config in Phase 2b (they read `DelegationDescriptor` today and would silently go null). |
| D12 | Rollout granularity | Setting `analytics.engine.plan_format` selects format **per stage kind**: `legacy` (default) → `reduce_proto` (Phase 2a: reduce/coordinator-local stages proto, shard stages byte-identical legacy) → `full_proto` (Phase 2b). Mixed formats are safe: the inter-stage boundary is Arrow partition streams either way. |
| D13 | Delegation classification source (Phase 2b) | The `BoolNode` classifier in `indexed_table` gains a **second entry point** taking a deserialized expression, sharing all classification logic with `substrait_to_tree`. Never fork the classifier. The `api.rs` (~line 755) `index_filter` routing check reads its signal from the scan node config instead of raw substrait bytes. |
| D14 | Request shape | Per-shard fan-out unchanged. DF_PROTO `FragmentExecutionRequest` carries `{queryId, stageId, shardId, planFormatVersion, dataFusionVersion, planBytes}` — no `PlanAlternative` list, no instructions, no delegation descriptor. Backend selection happens at the coordinator before finalization (registry picks per-stage backend; exactly one plan per stage). |
| D15 | Plan caching | Out of scope. Note only: finalized `Map<stageId, planBytes>` is the natural cache value for the existing plan-cache workstream. |

## 3. DO-NOT-TOUCH list (Lucene delegation + shard execution)

These files/protocols must not be modified by any phase. If a task seems to require it, the task is wrong.

- `analytics-engine/.../planner/dag/DelegatedPredicateCombiner.java` — classification logic.
- `analytics-framework/.../spi/DelegatedPredicateSerializer.java` and all implementations — payloads are `QueryBuilder` named-writeables; the byte format is owned by the Lucene backend.
- `analytics-framework/.../spi/FilterDelegationHandle.java` — the FFM upcall protocol (`createProvider` → `createCollector(providerKey, writerGeneration, minDoc, maxDoc)` → `collectDocs` → release lifecycle).
- `analytics-backend-lucene/.../LuceneFilterDelegationHandle.java` — query compilation, weight/scorer caching.
- `analytics-backend-datafusion/rust/src/indexed_table/` **below the classifier**: `bool_tree.rs`, `table_provider.rs`, `eval/` (all evaluators), page pruner, row selection; `indexed_executor.rs`; the `tests_e2e/fuzz` harness (you will run it, not edit it).
- `FilterTreeShapeDeriver.java` — runs before stripping, annotations intact; its output now travels in `StageMeta.tree_shape`.
- The marker UDF semantics: `delegated_predicate(annotationId)` and `delegation_possible(original, annotationId)` are parse-time markers and must **never** be physically evaluated.
- All LM/QTF execution: `LateMaterializationStageExecution`, `Stitcher`, fetch-by-row-ids path.
- `register_partition_stream` transport and partition senders (only its schema-derivation *arm* is deleted, per D5).

Permitted in Phase 2b only: adding the second classifier entry point next to `substrait_to_tree.rs` (additive), and re-pointing the `ShardScanWithDelegationHandler` inputs to node config (its session-creation body relocates intact).

## 4. Architecture (target state)

```
COORDINATOR (Java)                              COORDINATOR (Rust, stage_finalizer.rs)
SQL/PPL → Calcite → split rules/DAGBuilder      for each stage, bottom-up, one session:
→ QueryDAG (UNCHANGED)                            from_substrait_plan → physical plan
→ per stage:                                       → force agg mode (D4)
   AnnotationStripper (extracted strip logic)      → quirk fix-up pass (D9)
   → ONE whole-fragment Substrait conversion       → leaf rewrite:
   → StageMeta                                        SHARD_SCAN  → pushdown-stub → OpenSearchShardScanExec
→ FFM finalize_query_plan(stages, edges) ────────►    STAGE_INPUT → StageReadExec{schema := child.schema()} (D5/D6)
◄──────── Map<stageId, planBytes> ────────────        VALUES → as-is; LM_OUTPUT → declared rowType (D10)
→ scheduler ships per-shard DF_PROTO requests      → encode PhysicalPlanNode(OpenSearchExtensionCodec)
  along existing transport (UNCHANGED)             → debug decode + displayable() equality assert

DATA NODE: decode(codec) → register ShardBindings TaskContext extension → execute → existing partition senders.
No substrait consumer, no mode forcing, no schema derivation on data nodes.
```

The shard-scan leaf rewrite is **pushdown-faithful**: the finalizer registers a stub `TableProvider` whose `supports_filters_pushdown` returns the same claims as the real `IndexedTableProvider` for the same input, so physical planning routes the entire filter condition into the scan and emits no `FilterExec` above it. `OpenSearchShardScanExec` carries: serialized filter expression, `tree_shape`, `delegated_expressions` payloads, `requests_row_ids`, index/binding key, projected schema — i.e., exactly the union of today's `ShardScanWithDelegationInstructionNode` + `DelegationDescriptor`.

### 4.1 Two-phase aggregates — the graft mechanic (Calcite splits; DataFusion owns the state, end to end)

Calcite continues to emit a PARTIAL fragment and a FINAL fragment per its existing conventions. The boundary between them is **never typed by Calcite and never reconciled by schema mapping**. Instead, the finalizer exploits the fact that planning the shard fragment yields a complete `AggregateExec(Final ← Partial)` pair whose halves are mutually consistent by DataFusion's own construction — and stretches that pair across the stage boundary. **Order is mandatory: child stage before parent stage, one session.**

**PARTIAL (shard stage):**
1. Lower the whole fragment (consumer produces an ordinary logical `Aggregate`; Substrait phases are gone per D4).
2. Physical-plan with `CombinePartialFinalAggregate` removed → DataFusion emits its aggregation subtree (canonically `Final ← Partial`; `Single`/`*Partitioned` variants are normalized by the relocated `force_aggregate_mode` code, which already handles every mode the planner emits).
3. Split the pair: the **Partial half** (plus the scan below it) becomes the shard stage plan; its output schema **is** the physical state schema — multi-column states (e.g. `approx_percentile_cont`'s sum/count/min/max/centroid-list) are unconstrained and unmodified. The **Final half is retained** by the finalizer for the parent stage.
4. Encode the Partial half. State flows at runtime as ordinary Arrow data over the existing partition stream.

**FINAL (reduce stage):**
1. Register `input-<childStageId>` with **Calcite's declared rowType** — a binding skeleton whose only job is to let `from_substrait_plan` bind the fragment. It never types the runtime boundary.
2. Lower and physical-plan the fragment normally.
3. Fix-up: replace the plan's **entire aggregation subtree** (whatever exec(s) the planner produced for the FINAL call set — pair, `Single`, or partitioned variants) with: the retained Final half over `StageReadExec{child_stage_id, schema := Partial half's .schema()}`. Operators above the aggregate (Sort, Limit, projections such as avg's sum/cnt division) are kept from this fragment's own planning — they bind against the Final half's **output** schema, which is the final result types Calcite and DataFusion already agree on.
4. **D6 assertion point for agg boundaries:** grafted-Final output schema ≡ replaced-subtree output schema (positions and types; insert a rename projection if only names differ). The `StageReadExec`-level declared-rowType assertion applies **only to non-agg boundaries** (pure gather, `agg_mode=NONE`) — it cannot and must not be applied at agg boundaries, where state arity legitimately differs from Calcite's declaration.

**Consequences (these are the point of the migration):**
- DataFusion-native aggregates (`approx_distinct`, `approx_percentile_cont`, and any future or custom UDAF) distribute with **zero per-aggregate plumbing**: no `reduce_eval`, no schema derivation, no coercion, no merge handlers. The graft never inspects state shape.
- D7's opaque-binary-state rule applies **only** to Lucene-engine-native aggregates whose state is computed inside Lucene and crosses the JNI boundary. Pure-DataFusion aggregates use their native state schemas with no constraints.
- Decomposable aggregates (Calcite reduce-rule output: partial `SUM, COUNT`; final `SUM(sum), SUM0(cnt)`) need no special handling — the graft replaces the reduce fragment's semantically-equivalent merge subtree with the pair's Final half; results are identical.

**Edge rules:**
- *Union-fed FINAL* (multiple child stages under one final aggregate): graft one child's retained Final half over `UnionExec(StageReadExec…)`. Assert all children's Partial output schemas are identical (same agg calls + same DF version guarantee it; assert anyway).
- *TopK above the partial aggregate (shard side):* the shard fragment's Sort/Limit above the Partial half is kept only when every aggregate's state is 1:1 with its declared column (the only form today's TopK rewriter emits — count/sum based). Assert this; reject TopK-over-partial with multi-column state (semantics would be undefined, matching today's capabilities).
- *`agg_mode=NONE`:* no graft; plain lower → leaf-swap → encode, with the declared-rowType assertion at `StageReadExec` per D6.

Worked example — `… | stats approx_percentile(latency, 0.99) by status`:
```
Shard stage (ships):                          Reduce stage (ships):
AggregateExec(Partial,                        ProjectionExec / SortExec (fragment's own)
  gby=[status],                                 AggregateExec(Final,            ← grafted, born
  agg=[approx_percentile_cont(latency,.99)])      gby=[status], agg=[…])          from the same
  schema: [status, sum, count, max, min,          StageReadExec{child=1,           pair as the
           centroids: List<Float64>]                schema=[status, sum, count,    Partial below
OpenSearchShardScanExec                                     max, min, centroids]}
```
Calcite declared `[status, DOUBLE]` at this boundary; that declaration is used once (substrait binding) and asserted never.

**Phase 0a additions:** include `approx_percentile_cont` in the round-trip spike (multi-field state with `List<Float64>` over the partition stream — confirm Arrow list types flow through `partition_stream.rs` / the senders unmodified).
**Phase 1 test additions (fold into the agg-pair checkbox):**
- [ ] Decomposed avg: graft end-to-end, results correct.
- [ ] `approx_distinct`: HLL state flows natively; results match single-node execution.
- [ ] `approx_percentile_cont`: multi-column state incl. centroid list; results match single-node execution.
- [ ] Opaque Lucene-native UDAF (Phase 0b function) through the same graft path.
- [ ] `Single`-mode planner output normalizes and grafts correctly.
- [ ] Union-fed FINAL with two identical children.
- [ ] TopK-over-partial assertion fires on a synthetic multi-column-state case.

## 5. New artifacts

**Rust (`analytics-backend-datafusion/rust/src/`):**
- `stage_finalizer.rs` — lower / mode-force / fix-up / leaf-rewrite / encode, per §4. Bottom-up DAG order enforced here.
- `os_exec/shard_scan_exec.rs` — `OpenSearchShardScanExec` (fields above; `execute()` builds the indexed session via the same internals `createSessionContextForIndexedExecution` uses today, sourcing the bool tree from the embedded expression via the D13 entry point; resolves shard reader from `ShardBindings` `TaskContext` extension).
- `os_exec/stage_read_exec.rs` — `StageReadExec{child_stage_id, schema}`; `execute()` pulls from the registered partition stream for `input-<child_stage_id>` (reuse `partition_stream.rs`).
- `os_codec.rs` — `OpenSearchExtensionCodec` covering both nodes above plus every custom exec discoverable in plans. Concrete first task of Phase 1: grep the crate for `impl ExecutionPlan` (`relabel_exec.rs`, `project_row_id_*`, indexed exec, anything under `os_exec/`) and add a codec arm or a written justification ("never appears in a serialized stage plan because …") for each.
- FFM entries in `api.rs`: `finalize_query_plan(stages: Vec<(substrait_bytes, StageMeta)>, edges) -> Vec<(stage_id, plan_bytes)>` and `execute_stage_task(query_id, stage_id, plan_bytes, shard_bindings) -> stream handle` (reuse `execute_local_plan` plumbing: cancellation, memory, streams).

**Shared proto (prost, in the existing native-bridge proto location):**
```
StageMeta {
  int32 stage_id; repeated int32 child_stage_ids;
  AggMode agg_mode;                       // NONE | PARTIAL | FINAL
  LeafKind leaf_kind;                     // SHARD_SCAN | STAGE_INPUT | VALUES | LM_OUTPUT
  int32 tree_shape;                       // FilterTreeShape.ordinal()
  bool requests_row_ids;
  repeated DelegatedExpr delegated;       // {int32 annotation_id; string backend_id; bytes payload}
  repeated SerializedSchema declared_input_row_types;  // assertion targets (D6)
  SerializedSchema lm_output_row_type;    // set iff leaf_kind references an LM child (D10)
}
```

**Java (`analytics-engine`):**
- `AnnotationStripper` — extract `FragmentConversionDriver.strip` + `IntraOperatorDelegationBytes` verbatim into a standalone class (the delegation resolver's behavior is on the DO-NOT-TOUCH list; this is a move, not a rewrite).
- `StageConversionDriver` — per stage: strip whole fragment → one `DataFusionFragmentConvertor.convertFragment` call (no layering) → build `StageMeta` → batch FFM call → store `planBytes` (+ `debugSubstrait`, D11) on `StagePlan`.
- `FragmentExecutionRequest` v2 fields per D8/D14, format routed per D12 in `PlannerImpl`/`StageExecutionFactory`; data-node DF_PROTO route in `AnalyticsSearchService`/`ShardTaskRunner` calling `execute_stage_task`.

## 6. Work plan

Execute strictly in order. Each phase merges independently; a phase is done when every checkbox passes in CI.

### Phase 0 — Two spikes (independent; both must pass before Phase 1)
**0a. Codec round-trip.** Add `datafusion-proto` (workspace-pinned). Test: build a physical plan containing stub `OpenSearchShardScanExec` + `StageReadExec` + `AggregateExec(Partial)`/`AggregateExec(Final)` pair with a forced mode; encode each stage; decode in a **fresh** `SessionContext` with the standard UDF/UDAF registration; execute against an in-memory partition stream.
- [ ] Decoded `displayable()` == encoded `displayable()`.
- [ ] Result batches identical to direct execution.
- [ ] A `delegated_predicate` call inside the scan node's stored expression round-trips by name.

**0b. Engine-native UDAF.** Pick one `isEngineNativeMerge` aggregate. Implement per D7 (binary state column).
- [ ] Partial→Final across two sessions via proto round-trip produces results identical to the legacy `reduce_eval` path for the same inputs.

### Phase 1 — Rust finalizer + codec + exec nodes
Build §5 Rust artifacts. Test inputs: the existing Substrait test fixtures reorganized as `(fragment, StageMeta)` pairs.
- [ ] Codec inventory complete (every `impl ExecutionPlan` has an arm or a justification).
- [ ] Shapes finalize correctly: scan; filter+scan; partial-agg shard + final-agg reduce pair; TopK; Union; Values; LM-fed reduce (schema from `lm_output_row_type`).
- [ ] D5 verified on the agg pair: `StageReadExec.schema` equals the shard plan's actual output schema including state columns; D6 hard-error fires on an injected mismatch.
- [ ] Forced modes visible in `displayable()`; no `CombinePartialFinalAggregate` merging.
- [ ] Pushdown-stub parity test: stub claims == real `IndexedTableProvider` claims for the same filter set.
- [ ] Post-finalization invariant test: zero marker-UDF calls outside the scan node.
- [ ] Round-trip `displayable()` assertion wired as a debug-build step inside `finalize_query_plan` and as a CI test over the full PPL fixture corpus.

### Phase 2a — Reduce/coordinator stages to proto; shard stages byte-identical
Implement §5 Java artifacts; `plan_format=reduce_proto` per D12. Shard stages continue through today's path untouched: same `fragmentBytes`, same instructions, same `DelegationDescriptor`, same `ShardScanWithDelegationHandler`, same `createSessionContextForIndexedExecution`. Reduce-stage `StageReadExec` schemas via relocated `derive_schema_from_partial_plan` (D5).
- [ ] Multi-node integ: all existing PPL/SQL integ + property suites green under `reduce_proto`.
- [ ] Full delegation integ suite green (must be trivially true — shard path unmodified; treat any failure as a routing bug in D12).
- [ ] D8 mismatch path tested: version skew → legacy re-plan, query succeeds.
- [ ] Legacy remains default.

### Phase 2b — Shard stages to proto
Pushdown-stub leaf rewrite live for `SHARD_SCAN`; `OpenSearchShardScanExec.execute()` builds the indexed session from node config; D13 classifier entry point; `index_filter` routing signal re-pointed; D11 stats re-pointing; `attachPartialAggOnTop` path no longer exercised under `full_proto`.
- [ ] `indexed_table/tests_e2e` fuzz harness green with the bool tree sourced from the proto entry point.
- [ ] Differential test: `BoolNode` tree from proto-deserialized expression structurally identical to `substrait_to_tree` output across the fuzz corpus.
- [ ] Full delegation + QTF/row-id integ suites green under `full_proto` and under `reduce_proto` (both routes must keep working).
- [ ] `FilterDelegationHandle` upcall-count parity on a fixed set of delegation queries (legacy vs `full_proto`).
- [ ] Stats/slow-log fields populated identically under both formats.

### Phase 3 — Parity and default flip
- [ ] Full suites diffed across `legacy` / `reduce_proto` / `full_proto`; zero unexplained result differences (order-insensitive where applicable).
- [ ] Plan-size and latency deltas recorded in the PR description (informational; no gate).
- [ ] Default flips to `reduce_proto`, then `full_proto` after one green soak of the nightly suite each. Legacy stays selectable for one release.

### Phase 4 — Deletion (strictly after the `full_proto` flip soaks)
Delete, in this order, verifying the full suite (including delegation fuzz/e2e) after each group:
1. Reduce-side: `convertReduceNode`, reduce usage of `attachFragmentOnTop`, `convertSchemaOnlyRead`, `populatePostDecorationSchemas`, `WireFormat`, `convertLateMaterializationStage`, the `derive_schema_from_partial_plan` arm in `register_partition_stream`.
2. Shard-side: `attachPartialAggOnTop`, buried-partial-agg finder, `InstructionNode` SPI + `DataFusionInstructionHandlerFactory` + the three instruction handlers, `DelegationDescriptor` wire fields (the record type may remain as an internal carrier if `ShardScanExecutionContext` still uses it), data-node substrait decode path, data-node `agg_mode` invocation, `SubstraitPlanProtoRewriter` + pojo rewriter, `forceAggregatePhase`.
3. `FragmentConversionDriver` itself (its `strip` already lives in `AnnotationStripper`); collapse `FragmentConvertor` SPI to `convertFragment` only.
- [ ] Nothing on the §3 DO-NOT-TOUCH list was modified at any point (verify via `git log` on those paths).
- [ ] Build + all suites green with legacy gone; D8 mismatch now fails the query with the version pair in the message.

## 7. Standing CI invariants (added in Phase 1, never removed)

1. Every finalized stage plan round-trips the codec with identical `displayable()` output (full PPL corpus).
2. No `delegated_predicate`/`delegation_possible` call exists outside `OpenSearchShardScanExec` in any finalized plan.
3. Pushdown-claim parity: stub provider == `IndexedTableProvider` for the corpus's filter sets.
4. `BoolNode` source parity across the fuzz corpus (Phase 2b onward).
5. D6 schema rule: any non-nullability mismatch at a `StageReadExec` boundary is a hard error with both schemas printed.

## 8. File pointers

Preserve (cutting): `planner/dag/DAGBuilder.java`, `DistributedAggregateRewriter.java`, `planner/rules/*SplitRule.java`, `planner/rel/OpenSearchExchangeReducer.java`.
Replace (conversion): `planner/dag/FragmentConversionDriver.java` — extract `strip`+delegation resolver first (move, don't rewrite), then per Phase 4.
Relocate (don't rewrite): `rust/src/agg_mode.rs` → `stage_finalizer.rs`; `derive_schema_from_partial_plan` (`api.rs` ~1509) → finalizer.
Reuse: `DataFusionFragmentConvertor.convertFragment` (whole-fragment isthmus pass); `execute_local_plan` / `session_context.rs` / `partition_stream.rs` plumbing.
Delegation reading list (before any Phase 2b work): `DelegatedPredicateCombiner.java`, `spi/FilterDelegationHandle.java`, `spi/DelegationDescriptor.java`, `LuceneFilterDelegationHandle.java`, `ShardScanWithDelegationHandler.java`, `rust/src/indexed_table/substrait_to_tree.rs` (its module doc states the current contract: "The substrait plan is the wire format" — this spec changes that contract for the *transport*, while keeping the classifier and everything below it intact), `bool_tree.rs`, `indexed_table/eval/`.
Codec reference (pattern only; no dependency): `datafusion-contrib/datafusion-distributed` `src/protobuf/distributed_codec.rs`.
