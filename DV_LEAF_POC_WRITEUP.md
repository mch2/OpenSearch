# Doc-Values Leaf PoC — Results & Writeup

Branch: `dv-leaf-poc`. Implements `spec.md`: distributed analytics queries with **Lucene doc
values as the only storage** — no parquet anywhere in the read path. Lucene executes the scan
(delegated filters select doc IDs), Java bulk-decodes doc values into Arrow batches, DataFusion
pulls them out of the JVM (the pre-existing `openFragment` → `JAVA_CURSOR` leaf mode) and does all
compute: residual filters → partial agg → shuffle → final agg → gather.

## What was built

| Piece | Where | Notes |
|---|---|---|
| J1 executor | `be.lucene.dv.DocValuesFragmentExecutor` (+ `ParallelDocValuesFragmentExecutor`) | per-segment `Weight`/`Scorer` scan, explicit liveDocs, docid batches (never span segments), Arrow C-Data export per pull |
| J2 seam | `be.lucene.dv.ColumnBatchSource` | the PoC-2 contract; Arrow-only outputs, ascending-docid invariant, per-column bulk-vs-fallback counters |
| J2 impl | `be.lucene.dv.LuceneColumnBatchSource` | numeric bulk decode via Lucene 10.5 `NumericDocValues.longValues` (dense-run gate) + per-doc fallback for nulls; keyword utf8 + dictionary modes |
| J3 schema | `be.lucene.dv.DvColumnSpec` | derives decode kinds from mappings against the coordinator-advertised projected schema (shipped through a new schema-ptr arg on the `openFragment` upcall); typed `DocValuesLeafUnsupportedException` for v1 exclusions |
| J4 routing | `DistributedLeafBridge` | `index.composite.primary_data_format: lucene` routes to `JAVA_CURSOR`; parquet leaves untouched. Lucene `DataFormat` gained numeric/date/boolean/`_version`/`_doc_count` COLUMNAR_STORAGE caps + doc-values field factories |
| R1 | `leaf_bridge.rs` / `leaf_stream.rs` | schema-ptr on openFragment; optional per-batch schema on leafNext (dictionary batches import + cast to advertised types). ShardScanExec/shuffle/joins/cancellation untouched |

Settings: `analytics.dv.segment_parallelism` (0 = cores/2), `analytics.dv.batch_size` (8192),
`analytics.dv.keyword_encoding` (`utf8` default | `dictionary`).

## Test status (definition of done)

- **Decode unit tests** (suite 1): per type, nulls, segment boundaries, deleted docs, multi-valued
  rejection, bulk-vs-fallback counter split, dictionary round-trip — 13 tests green.
- **Differential harness** (suite 2): `DocValuesLeafIT`, 18 queries — dv-path vs parquet-path under
  the same distributed engine (grouped agg, full-scan agg, delegated keyword + residual numeric
  mixes, isnull/isnotnull, ranges, doubles, dates, multi-key group-by, exact `distinct_count`) —
  exact match, both keyword modes (`integTest` + `integTestDvDictionary` clusters), 12 tests green each.
- **Distributed E2E** (suite 3): 2 nodes × 2 shards; repeated-query lease-release gated by
  force-merge; parallel-scan close/cancel leak tests assert zero leaked exports/threads (allocator
  balance zero). Parquet `DistributedEngineIT` (incl. joins) still green — zero behavioral change
  to the existing path.
- **Fallback tests** (suite 4): v1-excluded types (`ip`) rejected at index creation with a clear
  error naming the field (the earliest possible failure point on a lucene-primary index); the typed
  exception backstops mapped-but-undecodable cases. Never a silent wrong answer.

## Benchmark (hits-style, 200k docs, 2 shards, 2 nodes, median of 5 after warmup)

`DocValuesLeafBenchIT` (opt-in via `-Dtests.dv_bench=true`). Comparisons: dv-vs-parquet under one
engine isolates the **storage** term; dv-vs-classic `_search` on the same index isolates the
**engine** term.

**utf8 keyword mode:**

| shape | dv-leaf | parquet | `_search` agg |
|---|---|---|---|
| selective predicate + group-by | 90ms | 81ms | 82ms |
| low-selectivity + group-by | 86ms | 89ms | n/a |
| high-cardinality string group-by (~50k keys) | **275ms** | 89ms | 340ms |
| full-scan aggregation | 69ms | 71ms | n/a |

**dictionary keyword mode (`analytics.dv.keyword_encoding=dictionary`):**

| shape | dv-leaf | parquet | `_search` agg |
|---|---|---|---|
| selective predicate + group-by | 90ms | 89ms | 84ms |
| low-selectivity + group-by | 96ms | 90ms | n/a |
| high-cardinality string group-by | **111ms** | 99ms | 172ms |
| full-scan aggregation | 78ms | 79ms | n/a |

### Readings

1. **The doc-values leaf is at parity with parquet for numeric shapes** (±10% on
   selective/low-selectivity/full-scan) at this scale — the storage term is small once the engine
   term is held constant.
2. **The item-9 (dictionary execution) instrument answered decisively:** utf8 keyword
   materialization is the bottleneck on high-cardinality string group-by (275ms vs parquet's 89ms);
   dictionary encoding recovers 2.5× (111ms), landing within ~12% of parquet. Per-row `lookupOrd`
   term materialization is the cost; per-batch dictionaries amortize it to one lookup per distinct
   term per batch. **Recommendation: dictionary-encoded keyword transport should be the default
   once dictionary-native group-by lands in the engine** (today the consumer casts back to Utf8 at
   the leaf boundary — the remaining gap vs parquet is that cast; dictionary-native hash aggregation
   would eliminate it and plausibly beat parquet's re-decoded strings).
3. `_search` agg comparison: the distributed DV path beats classic `_search` on the
   high-cardinality shape in both modes (275/111 vs 340/172) and matches on selective group-by.

### Bulk-decode counters (the addendum's non-optional gate)

Numerics engage Lucene 10.5's stock `NumericDocValues.longValues(size, docs[], values[], default)`
bulk API on dense runs (`docIDRunEnd()` probe) and are counted per column (`bulkDecodeBatches` vs
`perDocFallbackBatches` — unit-asserted both ways). **Fork inventory:** stock 10.5 has bulk
`longValues` (heap `long[]`, not `longValuesInto(MemorySegment)`) and `rangeIntoBitSet`; it has
**no** bulk `ordValues` on `SortedDocValues` — keyword decode is per-doc in both modes, which is
exactly why the fork's `ordValues` (the natural substrate for dictionary indices) is the highest-
value codec item for a follow-up. Building against the fork was not required for this PoC.

## What PoC 2 needs from these interfaces

- `ColumnBatchSource` is the swap point: docid selection, schema derivation, and stream framing all
  stay. A Rust reader implements `decodeBatch` semantics behind FFM (interface outputs are
  Arrow-only; no Lucene types escape).
- Since stock-Lucene bulk numeric decode already avoids per-doc dispatch (and the fork's
  `longValuesInto` would eliminate the heap `long[]` hop too), **PoC 2's remaining payoff is
  eliminating the JVM thread + upcall from the path, not the decode itself** — this materially
  lowers PoC 2's expected gain for numeric-heavy shapes and raises the relative value of
  codec-level `ordValues` + dictionary-native execution for string shapes.

## Known limits (v1 scope, as specced)

- Types: long/integer/short/byte, double/float, date, keyword, boolean. Multi-valued, text, ip,
  geo, binary, nested → typed rejection (mostly at index creation).
- Delegation: conjunctive trees only (INTERLEAVED → typed error); numerics don't delegate (no
  Lucene Index capability) and run as residual DataFusion filters, as designed.
- Leaf advertises one partition (TopK/sort-head optimization out of scope).
- Dictionary mode currently forces a sequential scan (per-batch schema isn't plumbed through the
  parallel queue yet) and the consumer casts dictionary → Utf8 at the leaf boundary.
- Benchmark scale is IT-sized (200k docs); a ClickBench-scale run on a real cluster is the next
  validation step (`docs/` counters + `partition_statistics` are wired for it).

## Reproduce

```bash
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-amzn
export PROTOC=~/.local/protoc/bin/protoc   # substrait crate build.rs needs modern protoc

# correctness
./gradlew -Dsandbox.enabled=true -PrustDebug \
  :sandbox:qa:analytics-engine-rest:integTest --tests "org.opensearch.analytics.qa.DocValuesLeafIT"
./gradlew -Dsandbox.enabled=true -PrustDebug :sandbox:qa:analytics-engine-rest:integTestDvDictionary

# benchmark (utf8, then dictionary)
./gradlew -Dsandbox.enabled=true -PrustDebug -Dtests.dv_bench=true \
  :sandbox:qa:analytics-engine-rest:integTest --tests "org.opensearch.analytics.qa.DocValuesLeafBenchIT"
./gradlew -Dsandbox.enabled=true -PrustDebug -Dtests.dv_bench=true \
  :sandbox:qa:analytics-engine-rest:integTestDvDictionary --tests "org.opensearch.analytics.qa.DocValuesLeafBenchIT"
```
