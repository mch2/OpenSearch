# Phase D session status

## Test results vs baseline

| Suite                       | Baseline (no Phase D) | Initial Phase D | After cost penalty |
|-----------------------------|-----------------------|-----------------|--------------------|
| Unit tests (~250)           | All pass              | All pass        | All pass           |
| JoinCommandIT (cross-table) | Fails                 | **Pass**        | **Pass**           |
| JoinWindowIntegrationIT     | 18/20                 | 18/20           | 18/20              |
| Other PPL command ITs       | 10 failures           | 23 failures     | **15 failures**    |

**Net Phase D delta:** +5 IT failures (was +13 before cost penalty), all in PPL command tests with runtime / serialization issues that need DataFusion-side debugging.

## Cost penalty change

`OpenSearchProject` and `OpenSearchFilter` now charge a small finite cost (10) when at SINGLETON distribution, so Volcano prefers the RANDOM-side variant + ER-above plan. This pushes column projection and predicate filtering to the data-node fragment, shrinking the wire payload. The SINGLETON variant remains available for the windowed-stack propagation case where there's no RANDOM alternative (e.g. above a windowed-gathered Project whose output is already SINGLETON) — then the cost penalty is irrelevant because nothing else satisfies the SINGLETON requirement. Sort retains the infinite-cost penalty on non-SINGLETON inputs (Sort over partitioned data is incorrect without a merge-sort exchange, which we don't have).

## Remaining failures

**Pre-existing (8, not introduced by Phase D):**
- `AppendCommandIT`: 5 — `replaceInput` doesn't handle Substrait `Set` rel (Union/Intersect/Minus). Fix: add `Set` arm to `DataFusionFragmentConvertor.replaceInput`.
- `AppendPipeCommandIT`: 2 — same `Set` rel issue.
- `DslClickBenchIT`: 1 — separate frontend issue.

**Phase D-induced (7, runtime/serialization in 2-stage path for single-shard):**
- `FieldsCommandIT.testFieldsExclusion`, `testFieldsSuffixWildcard`: DataFusion native panic "Panic: primitive array" on column-exclusion / wildcard projections. Fragment shape is `Sort(offset/fetch) → ER → Project(filtered cols) → Scan`, all column types (bool/date/datetime/int/num/str/time) get serialized through ER. Some type's Arrow encoding doesn't survive the round trip.
- `HeadCommandIT.testHeadFromOffset`: `IndexOutOfBoundsException: index: 50, length: 7 (expected: range(0, 14))` reading the result Arrow buffer. Fragment is `Sort(offset=14, fetch=5) → ER → Project(str2) → Scan` — a single VARCHAR column with offset/fetch over a 17-row source. Likely the Arrow values buffer (50 bytes referenced, only 7 in the buffer) is mis-sliced when the coord stage applies offset/fetch on the gathered batches.
- `ReverseCommandIT.testReverseAfterEvalFindsUpstreamSort`, `testReverseAfterFilterFindsUpstreamSort`: PPL `reverse` produces wrong row order (rows come out ASC instead of reversed-DESC). The PPL frontend's `reverse` likely scans the marked tree for an upstream `Sort` to flip; the Phase D plan inserts an ER between `Reverse` and the upstream Sort, breaking the scan.
- `TableCommandIT.testTableMinusExclusion`, `testTableSuffixWildcard`: same `Panic: primitive array` as `FieldsCommandIT` (PPL `table` is implemented similarly to `fields`).

## Files touched (Phase D + cost penalty)

Production:
- `OpenSearchTableScan.java` — drop `shardCount`, always RANDOM
- `OpenSearchTableScanRule.java` — drop `shardCount` arg
- `OpenSearchJoinGatherRule.java` — simplified to `convert(input, SINGLETON)` + trait-based matches
- `OpenSearchSort.java` — infinite cost on non-SINGLETON inputs (correctness — partition-local sort isn't a global sort)
- `OpenSearchProject.java` — finite cost penalty (10) for non-windowed Project at SINGLETON, so Volcano pushes column pruning to the data-node side
- `OpenSearchFilter.java` — same finite cost penalty for SINGLETON Filter, pushes predicate to the data-node side

Tests (~10 files): shape updates, helper changes, find-by-fragment-type for multi-stage DAGs.

## Commit blocked

GPG signing fails inside the agent's tmux pinentry. Work is staged; run `git commit -F /tmp/phase-d-commit-msg.txt` from a real terminal.

## Recommended next steps

1. Fix the pre-existing `Set` rel issue in `replaceInput` — unblocks 7 ITs (Append + AppendPipe).
2. Investigate the "primitive array" DataFusion panic — likely a Substrait-Rust type mapping issue when ER sends through a column type that the receiver can't decode. Trigger queries: `fields - cols...` (exclusion), `fields *0` (wildcard).
3. Investigate the IndexOutOfBoundsException in `testHeadFromOffset` — likely Arrow buffer slicing when `Sort(offset/fetch)` runs on coord over a string column from gather.
4. Fix `Reverse` PPL frontend to walk past ER when looking for upstream sort.
