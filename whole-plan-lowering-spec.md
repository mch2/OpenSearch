# Implementation Spec: Whole-Plan Lowering with Calcite Boundary Markers

**Audience:** implementing agent. Every decision is final; there are no open questions. If a step appears to require deviating from a decision or touching a DO-NOT-TOUCH file, stop and report.

**Supersedes:** the per-fragment/graft spec (`df-proto-migration-implementation-spec.md`). The graft, mode relocation, and binding-skeleton mechanics are retired by this design. The deterministic-distribution-planner spec (`deterministic-distribution-planner-spec.md`) remains a valid, independent workstream and is unchanged.

**Scope:** `sandbox/plugins/analytics-engine`, `sandbox/plugins/analytics-backend-datafusion`, `sandbox/libs/analytics-framework`.

---

## 1. Mission

Calcite remains the **complete** distributed planner: logical optimization, split rules, `DistributedAggregateRewriter`, ExchangeReducer placement, backend/capability resolution. What changes is everything downstream of it. The entire optimized distributed tree — boundaries included — is converted to Substrait **once**, lowered into **one** DataFusion physical plan on the coordinator, and **cut at the boundary markers** Calcite placed. Each cut piece ships as a serialized DataFusion physical plan (`datafusion-proto` + `OpenSearchExtensionCodec`). Data nodes decode and execute; nothing is re-derived, re-lowered, mode-forced, or schema-reconciled anywhere.

There is no graft, no aggregate-mode forcing, no binding skeleton, and no schema copying between separately-planned trees — those existed only because the previous design lowered per-fragment. One tree, cut after planning, makes boundary schemas correct by construction at the exact point of the cut.

## 1.1 Mental model (read this first)

- **Calcite cuts; the cut travels.** Java stamps each `OpenSearchExchangeReducer` with a `boundary_id` and converts the whole tree to one Substrait plan in which the ER is a boundary-marker extension relation. `DAGBuilder` continues cutting the Java-side tree for scheduling exactly as today; the two sides correlate by `boundary_id`.
- **One lowering, one physical plan, then the cut.** Rust lowers the whole Substrait plan (markers become barrier execs via standard DataFusion extension points), physical-plans it once, then walks the tree: at each barrier, the subtree below is that boundary's stage plan and the barrier is replaced in the parent by `StageReadExec{boundary_id, schema := barrier.input().schema()}`. The schema is read off one tree at the cut point — there is nothing to reconcile.
- **No aggregate phases exist below Calcite.** The distributed plan is honest relational algebra (§4). `AggregateMode` never crosses to Rust; `agg_mode.rs` is not relocated, not invoked, untouched (legacy path only, deleted with legacy).
- **Routing is per-query, not per-stage.** A query either lowers whole-plan (all of its stages ship proto, including shard stages) or runs entirely legacy. There is no mixed-format query (D9).

## 2. Locked decisions

| # | Decision | Resolution |
|---|---|---|
| D1 | Planner ownership | Calcite, completely: split rules, `DistributedAggregateRewriter`, `FinalAggCallBuilder`, ER placement, backend selection all stay. The `EnsureDistribution` workstream (other spec) refactors *how* Calcite places exchanges; this spec consumes its output identically either way. |
| D2 | Boundary representation | A Substrait **extension relation** named `os_stage_boundary`, emitted by the convertor wherever the tree contains an `OpenSearchExchangeReducer`. Detail payload (JSON in the extension's `detail` Any): `{boundary_id: int, exchange_type: "GATHER"}`. Java assigns `boundary_id` (pre-order index over ERs) before conversion and stamps the same id on the ER rel for `DAGBuilder` correlation. `exchange_type` is an enum-by-string with exactly one value in v1; §8 fixes its evolution. (Substrait's native `ExchangeRel` is noted as a possible future alignment; not used in v1 — the extension keeps `boundary_id` and isthmus support under our control.) |
| D3 | Rust marker lowering | Standard DataFusion extension points, no forks: a `SerializerRegistry`/consumer override maps `os_stage_boundary` → `StageBoundaryNode: UserDefinedLogicalNode`; an `ExtensionPlanner` lowers it to `StageBoundaryExec` (the barrier). Both live in a new `stage_boundary.rs`. |
| D4 | Barrier exec hygiene (`StageBoundaryExec`) | Single-child passthrough with: schema := child schema verbatim; `output_partitioning()` := child's partitioning (v1; see §8 for why this declaration becomes load-bearing under MPP); `with_new_children` correct; `benefits_from_input_partitioning` = false; it must act as an optimization fence — DataFusion physical optimizer rules must not push projections/filters/limits/sorts through it or eliminate it. Enforced by the §6 hygiene test, not by hope. `execute()` is `unreachable!` — barriers never execute; they exist to be cut. |
| D5 | The cut | Post-physical-planning walk (in `plan_cutter.rs`): bottom-up; at each `StageBoundaryExec`, emit `Stage{boundary_id, plan := barrier.input()}` and replace the barrier in its parent with `StageReadExec{boundary_id, schema := barrier.input().schema()}`. The remaining root tree is the coordinator stage (`boundary_id = ROOT`). Output: `Vec<CutStage{boundary_id, plan_bytes, output_schema_ipc, child_boundary_ids}>`. The cut performs **no other tree surgery**. |
| D6 | Schema assertions | Calcite's declared rowTypes are now truthful everywhere (state UDAFs declare `VARBINARY`, §4), so one uniform rule replaces the old agg-boundary exemption: at every cut, assert barrier-input schema vs. the ER's Calcite rowType — nullable-widening is the only silent acceptance; anything else hard-errors with both schemas and the `boundary_id`. Additionally Java cross-checks the returned stage DAG (ids + child edges) against `DAGBuilder`'s own cut; any mismatch is a hard plan-time error (the two cuts are of the same tree and must agree). |
| D7 | Aggregates | Honest algebra per §4. `CombinePartialFinalAggregate` stays **enabled** — each stage's aggregate is an ordinary single aggregate that DataFusion may internally pair-split for intra-stage parallelism, entirely within the stage. Nothing reads or writes `AggregateMode` outside DataFusion's own planner. |
| D8 | Non-decomposable aggregates | Explicit state/merge UDAF pairs (§4.2), replacing the `reduce_eval` convention for whole-plan queries. One pair per function, single opaque `Binary` state column, registered on every node (same `.so`). v1 ships `approx_distinct_state`/`approx_distinct_merge`. Queries using a non-decomposable aggregate without a ported pair route legacy (D9). |
| D9 | Routing (per-query) | Setting `analytics.engine.plan_format = legacy (default) \| whole_plan`. Under `whole_plan`, a query routes to the new path **iff** its plan contains: no late materialization, no delegated predicates (until Phase 3 gates pass), no `reduce_eval`/engine-native aggregate lacking a D8 pair, and no `WireFormat.OPAQUE` alternative selection. Otherwise it runs fully legacy. The predicate lives in one method (`WholePlanRouting.eligible(RelNode)`) with one log line stating the disqualifier. The eligible set only ever widens. |
| D10 | Late materialization | LM queries route legacy in v1 (excluded by D9). An LM boundary-marker variant is a post-v1 phase, designed then; nothing in this spec blocks it (the marker payload is extensible per §8). The LM/QTF execution machinery is on the DO-NOT-TOUCH list. |
| D11 | Delegation | Whole-plan lowering includes real shard scans, so the pushdown-stub provider and `OpenSearchShardScanExec` (already in the branch) are required v1 infrastructure — for **non-delegating** scans first. Delegation-bearing queries route legacy until Phase 3, which activates the embedded-filter path under the full §5 preservation contract (identical to the superseded spec's §3.6: pushdown-claim parity, marker-UDF-never-evaluated invariant, shared `BoolNode` classifier with a proto-expression entry point, stats re-pointing). |
| D12 | Java↔Rust metadata | JSON (Jackson/XContent ↔ serde_json), not protobuf — in-process, same-deployment, low-volume. Input `QueryPlanInput`: `{query_id, substrait_b64, scans: [{table, tree_shape, requests_row_ids, delegated: [{annotation_id, backend_id, payload_b64}]}]}` (delegated empty until Phase 3). Output `QueryPlanOutput`: `{stages: [{boundary_id, child_boundary_ids, plan_bytes_b64, output_schema_ipc_b64}]}`. Schemas as Arrow IPC (`schema_ipc.rs`, kept). The branch's `stage.proto`/`proto.rs`/`ProtoWriter`/`ProtoReader`/`StageMetaCodec` are deleted. |
| D13 | Wire format & versioning | Per-query whole-plan routing means shard stages ship proto from the first end-to-end phase. The branch's DF_PROTO `FragmentExecutionRequest` form, `PlanFormatCompatibility`, and `PlanFormatMismatchException` are kept and become live in Phase 2 (they were premature in the per-stage design; they are on-schedule here). Version skew → typed rejection → coordinator re-plans the query legacy. §8 reclassifies this handshake as a distributed-correctness invariant under MPP. |
| D14 | Inter-stage transport | Unchanged: existing partition streams over OpenSearch transport; `StageReadExec` resolves its stream from the `StageInputRegistry` task-context extension. Gather-only is a property of the transport/scheduler layer, not of this architecture (§8). |
| D15 | Explain/debug | `StagePlan` retains coordinator-side `debugSubstrait` (now the single whole-plan blob, stored on the root stage) plus per-stage `displayable()` text for explain. Never shipped. |

## 3. DO-NOT-TOUCH (carried over verbatim from the superseded spec)

The Lucene delegation framework preservation contract applies unchanged: `DelegatedPredicateCombiner`, `DelegatedPredicateSerializer` impls, `FilterDelegationHandle` upcall protocol, `LuceneFilterDelegationHandle`, everything in `indexed_table/` below the classifier (`bool_tree.rs`, `table_provider.rs`, `eval/`, page pruner), `indexed_executor.rs`, the `tests_e2e/fuzz` harness, `FilterTreeShapeDeriver`, the marker-UDF never-physically-evaluated rule, all LM/QTF execution, and `register_partition_stream` transport. Phase 3 may only *add* the proto-expression entry point beside `substrait_to_tree` and re-point `ShardScanWithDelegationHandler` inputs; the classifier is never forked. Legacy-path files (`FragmentConversionDriver`, instruction handlers, `agg_mode.rs`, schema-derivation arms) are **untouched until the final deletion phase** — the legacy path must remain byte-identical while it is the routing fallback.

## 4. Aggregates: honest algebra (normative)

**4.1 Decomposable aggregates — already done.** `FinalAggCallBuilder` rewrites FINAL calls into genuine merge form (`SUM(sum_col)`, `SUM0(cnt_col)`, MIN/MAX over partials; AVG decomposed by the reduce rule). The whole distributed tree is therefore valid plain relational algebra: the shard-side aggregate really computes per-shard sums; the reduce-side aggregate really sums them. Both lower through Substrait as ordinary aggregates. No phase information exists below Calcite. The PARTIAL/FINAL annotations on `OpenSearchAggregate` remain Calcite-internal metadata (backend selection, stats); the convertor must not emit them into Substrait (delete `forceAggregatePhase` usage on this path).

**4.2 Non-decomposable aggregates — state/merge UDAF pairs.** For each function whose merge requires engine state, the split rule emits a named pair instead of `reduce_eval`:
- Shard side: `<fn>_state(args) → VARBINARY` — an ordinary aggregate whose `evaluate()` returns the serialized accumulator state as one binary value. For DF-backed functions, implemented by delegating to DataFusion's own accumulator and serializing its `state()` (multi-field states, e.g. a t-digest, pack into the one blob).
- Reduce side: `<fn>_merge(state) → result_type` — deserializes each incoming blob, merges, evaluates. (`approx_distinct_merge` exists in the branch and becomes load-bearing.)
- Calcite declares the boundary column truthfully as `VARBINARY`, which is what makes D6 uniform.
- Java side: a `StateMergeRegistry` maps Calcite aggregate function → (state name, merge name); `FinalAggCallBuilder` consults it where it currently emits `reduce_eval`. Functions absent from the registry disqualify the query per D9.

**4.3 What is forbidden.** No code may force, read, or depend on `AggregateMode` in any whole-plan stage. CI greps the new modules for `AggregateMode::Partial|Final` outside DataFusion-internal use; a hit fails the build.

## 5. Components

**Java (`analytics-engine`):**
- `WholePlanRouting.eligible(RelNode)` — the D9 predicate, one place.
- `BoundaryIdStamper` — pre-order `boundary_id` assignment onto ERs before conversion; `DAGBuilder` reads the same ids onto its `Stage`s (one field added to `Stage`).
- `WholePlanConversionDriver` (replaces the branch's `StageConversionDriver`): strip annotations on the **whole tree** (call the existing `FragmentConversionDriver.strip` via widened visibility — no `AnnotationStripper` copy), derive `FilterTreeShape` per scan before stripping, one whole-tree `convertFragment` call, build `QueryPlanInput` (D12), one FFM call, distribute `plan_bytes` onto stages by `boundary_id`, run the D6 DAG cross-check.
- Request/handshake (D13): branch's `FragmentExecutionRequest` DF_PROTO form, `PlanFormatCompatibility`, `PlanFormatMismatchException` — kept as written.
- Execution wiring: shard stages ship `{planFormatVersion, dfVersion, planBytes}` per shard (branch's `ShardFragmentStageExecutionFactory` branch, with `SHARD_PROTO_EXECUTION_READY` deleted — it is ready in Phase 2 or the phase doesn't merge); reduce stages execute coordinator-side via the branch's `local_executor` path.

**Rust (`analytics-backend-datafusion/rust/src/`):**
- `stage_boundary.rs` — D2 extension parsing, `StageBoundaryNode`, `ExtensionPlanner`, `StageBoundaryExec` (D4).
- `plan_cutter.rs` — D5 cut + D6 schema assertion.
- `whole_plan.rs` — FFM entry `plan_whole_query(QueryPlanInput) -> QueryPlanOutput`: register pushdown-stub providers for each scan (branch's `pushdown_stub.rs`), lower whole Substrait, physical-plan, cut, swap scan leaves to `OpenSearchShardScanExec` (branch's, minus delegation until Phase 3), encode each stage (branch's `os_codec.rs` + round-trip assertion).
- `execute_stage_task` (branch's, kept): decode, bind `ShardBindings`/`StageInputRegistry`, execute.
- `udaf/approx_distinct_state.rs` (new, ~100 lines) + `approx_distinct_merge.rs` (branch's, kept).
- Kept from branch unchanged: `os_codec.rs` (add `StageBoundaryExec`? — no: barriers never serialize, the cut removes them all; add a codec-time assertion that none remain), `stage_read_exec.rs`, `shard_scan_exec.rs`, `pushdown_stub.rs`, `schema_ipc.rs`, `local_executor.rs`, FFM/NativeBridge plumbing.
- Deleted from branch: all graft machinery (`graft_final_half*`, `replace_aggregate_subtree`, `rebase_aggregate_input`, `rename_if_name_drift`, `find_top_aggregate`, retained-Final threading), mode-relocation into the finalizer (revert `agg_mode.rs` to untouched), binding-skeleton registration, `convert_input_leaves` schema-matching, `StageMeta` agg/leaf-kind/child-substrait machinery, `stage.proto` + `proto.rs` + Java proto codec stack (D12).

## 6. Work plan (phases merge independently; checkboxes are the gates)

**Phase 0 — Two spikes.**
- (a) Marker round trip: Calcite tree with one ER → whole-plan Substrait with `os_stage_boundary` → DF logical with `StageBoundaryNode` → whole physical plan with `StageBoundaryExec` → cut → two plans → codec round-trip both → execute via in-memory partition stream → results equal single-node, for `SUM(x) GROUP BY k`.
- (b) `approx_distinct_state` implemented; state→merge across two sessions equals legacy `reduce_eval` results and equals single-node `approx_distinct`.
- [ ] Both green, including the D6 schema assertion firing on an injected ER rowType lie.

**Phase 1 — Rust whole-plan path.**
- `stage_boundary.rs`, `plan_cutter.rs`, `whole_plan.rs`; fixtures: scan, filter+scan, two-stage decomposed AVG, two-stage `approx_distinct` via the pair, TopK (Sort/Limit above shard aggregate — now just operators in the tree, no special case), Union under a reduce aggregate (no special case — assert so), multi-ER tree (3 stages) cut correctly.
- [ ] **Barrier hygiene test (load-bearing):** for every fixture, the physically-planned whole tree contains no DF-inserted `RepartitionExec`/`CoalescePartitionsExec` adjacent to a barrier, no operator pushed through a barrier (compare `displayable()` against the barrier-free plan of each fragment), and post-cut plans contain zero `StageBoundaryExec`.
- [ ] D7 verified: intra-stage `Final←Partial` pairs may appear *within* a stage and never straddle a cut.
- [ ] §4.3 grep gate wired into CI.

**Phase 2 — End-to-end for eligible queries (this is the first phase where proto ships to data nodes; handshake live).**
- Java components (§5), `whole_plan` routing for non-delegating/non-LM/ported-aggregate queries; shard proto execution via `execute_stage_task` with plain (non-indexed) shard scans; reduce stages coordinator-side.
- [ ] Multi-node integ: eligible-query suite green under `whole_plan`; full suite green (ineligible queries verifiably routed legacy — assert the log line).
- [ ] Full delegation + LM integ suites green (trivially — those queries route legacy; treat any failure as a D9 routing bug).
- [ ] D13 skew test: version mismatch → typed rejection → legacy re-plan → query succeeds.
- [ ] D6 DAG cross-check exercised (inject a boundary-id mismatch in a test).

**Phase 3 — Delegation activation (gated on the §3/§5 contract; this is the only phase touching anything near the delegation framework).**
- Embedded-filter `OpenSearchShardScanExec` path: pushdown-claim parity test vs `IndexedTableProvider`, proto-expression entry point beside `substrait_to_tree` (shared classifier), `index_filter` routing signal from scan config, stats/slow-log re-pointing.
- [ ] `indexed_table/tests_e2e` fuzz harness green via the new entry point; `BoolNode` differential test (substrait-source vs proto-source) across the corpus.
- [ ] Delegation + QTF/row-id integ suites green under both routes; `FilterDelegationHandle` upcall-count parity on the fixed query set.
- [ ] D9 predicate widened to admit delegation-bearing queries.

**Phase 4 — Coverage growth + flip.**
- Port remaining engine-native aggregates to D8 pairs (registry-driven, one small PR each); optionally the LM marker (separate mini-design per D10).
- [ ] Parity harness: full PPL/SQL suites diffed legacy vs `whole_plan` for the eligible set; zero unexplained diffs. Default flips; legacy selectable one release.

**Phase 5 — Deletion (after flip soaks).** `FragmentConversionDriver` (strip extracted at last), instruction SPI + handlers, `DelegationDescriptor` wire fields, data-node substrait path, `agg_mode.rs`, `derive_schema_from_partial_plan`, `reduce_eval` convention, `forceAggregatePhase`, schema stubs, `WireFormat`, `SubstraitPlanProtoRewriter`. Verify no DO-NOT-TOUCH file was modified (`git log` over those paths).
- [ ] Build + all suites (incl. fuzz) green with legacy gone.

## 7. Standing CI invariants

1. Every shipped stage plan round-trips the codec with identical `displayable()` (full corpus).
2. No `StageBoundaryExec` survives a cut; no marker UDF appears outside a scan node (Phase 3 onward).
3. Barrier hygiene (§6 Phase 1 test) on the full fixture corpus.
4. D6 both checks: ER rowType vs cut schema (nullable-widening only) and Java/Rust DAG agreement.
5. §4.3: no `AggregateMode` dependence in whole-plan modules.
6. Eligible/ineligible routing is logged and asserted in integ tests — a query silently taking the wrong path is a test failure.

## 8. MPP forward-compatibility contracts (documented now, implemented later)

These five contracts make distributed joins/shuffle a growth, not a rewrite. Do not paint over them.
1. **Boundary descriptor** (D2) evolves to `{boundary_id, exchange_type: GATHER | HASH{keys, M} | BROADCAST}`; Calcite's distribution lattice (the `EnsureDistribution` workstream) grows HASH/BROADCAST and join/agg co-partitioning requirements. Broadcast-vs-shuffle is the first genuine cost decision — the camp-2 trigger; until statistics exist it is a deterministic size-threshold heuristic in the visitor.
2. **The cut grows a writer half:** non-gather boundaries split into `ShuffleWriteExec(keys, M)` appended at the producer root + `StageReadExec(Hash(keys, M))` at the consumer; gather folds in as M=1. `plan_cutter.rs` is written so the barrier-replacement is the single extension point.
3. **Barrier partitioning declaration becomes correctness-critical:** `StageBoundaryExec.output_partitioning()` must declare the boundary's true semantics so DF's `EnforceDistribution` neither double-shuffles nor under-satisfies a join; the §6 hygiene test grows a shuffle-join fixture asserting zero DF-inserted repartitions across boundaries.
4. **D13 reclassifies:** identical DataFusion version across nodes stops being a decode concern and becomes a *distributed join correctness invariant* (both sides of a co-partitioned join must compute identical `hash(key) % M`). The handshake message and docs must say so.
5. **Honest algebra extends unchanged:** `PARTIAL → exchange(HASH keys) → FINAL` is exactly correct plain algebra per hash slice; §4 needs no MPP amendments. The genuinely new MPP work is N×M transport and task-aware scheduling — the D14 layer — and is out of scope here by design.

## 9. Branch reconciliation (mch2:df-proto-migration)

**Keep as-is:** `os_codec.rs`, `stage_read_exec.rs`, `shard_scan_exec.rs`, `pushdown_stub.rs`, `schema_ipc.rs`, `local_executor.rs`, `udaf/approx_distinct_merge.rs`, FFM/`api.rs`/`NativeBridge` entry shapes, `FragmentExecutionRequest` DF_PROTO form + `PlanFormatCompatibility` + `PlanFormatMismatchException` (+ their tests), `ReduceProtoFormatIT` (adapt to whole-plan routing), settings scaffolding (`PlanFormat` values become `legacy|whole_plan`), `ExchangeSinkContext`/`FragmentConvertor` SPI additions (audit: keep only what `WholePlanConversionDriver` calls).
**Delete:** graft + mode-relocation + binding-skeleton portions of `stage_finalizer.rs` (the file dissolves into `plan_cutter.rs`/`whole_plan.rs`; the codec/encode/decode helpers and their tests move there), `agg_mode.rs` diff reverted, `AnnotationStripper.java` (widen `strip` visibility instead), `stage.proto`, `proto.rs`, `ProtoWriter`, `ProtoReader`, `StageMetaCodec` (+tests), `SHARD_PROTO_EXECUTION_READY`, the spec markdown at repo root.
**Add:** `stage_boundary.rs`, `plan_cutter.rs`, `whole_plan.rs`, `udaf/approx_distinct_state.rs`, `WholePlanRouting`, `BoundaryIdStamper` (+ `Stage.boundaryId`), `WholePlanConversionDriver`, JSON metadata types, the §6 test suites.
