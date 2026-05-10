# Phase D session — final status

## Test results

| Suite | Baseline (before session) | Final |
|---|---|---|
| Unit tests (~250) | All pass | **All pass** |
| JoinCommandIT (cross-table) | Fails | **Pass** |
| JoinWindowIntegrationIT | 18/20 | **18/20** |
| Other PPL command ITs | 10 failures | **3 failures** |

The 3 remaining IT failures (DslClickBenchIT, MathScalarFunctionsIT × 2) are pre-existing on the branch — they were failing before this session started. **Every Phase D-induced regression is resolved, and we additionally fixed 7 pre-existing failures (Append + AppendPipe commands)** as a side effect of the single-shot conversion bypassing the missing Substrait Set-rel handler.

## Commits

1. `989eb50a6bd` — **Phase D: scans always declare RANDOM distribution**
   - `OpenSearchTableScan` drops `shardCount`, always RANDOM (root cause: single-shard scans were lying about being SINGLETON, breaking join gather convert)
   - `OpenSearchJoinGatherRule` simplified to `convert(input, SINGLETON)` + trait-based matches (no manual ER wrapping)
   - `OpenSearchSort` overrides `isEnforcer()=false` and adds infinite-cost penalty on non-SINGLETON inputs (correctness — no merge-sort exchange yet)
   - Tests updated for the new always-2-stage shape: `BasePlannerRulesTests.assertPipelineViableBackends` skips ERs unless explicitly listed; `findStageWithFragment` helper finds the operator stage in multi-stage DAGs

2. `d59f7fc7115` — **Push Project/Filter to data-node side via cost penalty; fix test fallout**
   - `OpenSearchProject` and `OpenSearchFilter` charge a finite cost (10) when at SINGLETON so Volcano picks the RANDOM-side variant + ER-above plan, pushing column pruning and predicate filtering to the data-node fragment

3. **(staged, awaiting commit)** — **Fix layered conversion + Sort-no-collation cost**
   - `SubstraitPlanRewriter.rewrite` is now applied in `attachFragmentOnTop` / `attachPartialAggOnTop` / `attachJoinFragment` (was only in `convertToSubstrait`)
   - `FragmentConversionDriver.convertReduceFragment` does single-shot conversion of the stripped tree via `convertFinalAggFragment`, instead of recursing operator-by-operator with `attachFragmentOnTop`. Coord-side joins with compound branches still recurse via the preserved path
   - `OpenSearchSort.computeSelfCost` skips the SINGLETON penalty when the Sort has empty collation (a pure LIMIT) — for `head N from K`, partition-local fetch is correct and avoids a DataFusion Arrow buffer mis-slice

## Commit blocked

GPG signing fails inside the agent's tmux pinentry. Run from a real terminal:

```
cd /Users/handalm/OpenSearch-SecondClone/OpenSearch && git commit -F /tmp/single-shot-commit-msg.txt
```
