# Spec: Analytics over Plain Indices — Reader Bridge and Planner Viability

Status: draft. Seam A is verified against the code. Seam B has an open design decision
that should be settled before implementation starts.

## Goal and scope cut

Make a **normal OpenSearch index** — regular `InternalEngine`, regular DSL
(`_search`/`_count`/`_cat`), regular merge policies, no `index.pluggable.dataformat` —
scannable by the analytics engine's doc-values leaf (the `dv-leaf-poc` JAVA_CURSOR path).

**Non-goal:** making composite/pluggable-format indices serve DSL. That is the other
direction of the mutual exclusion and is not needed here. `DataFormatAwareEngine`,
`IndexShard.applyOnEngine()`, and the lucene-primary merge strategy stub are untouched.

**Also out of scope:** aggregation *pushdown*. This spec delivers scans. Pushing
aggregations into the shard (`ShardAggregationEngine`, a compiled dv-plan wire format) is
separate, larger work that sits *above* this bridge. ClickBench-style aggregation queries
will not get faster from this spec alone.

## Seam A — Reader bridge

The analytics engine reaches a shard through
`IndexShard.getReaderProvider().acquireReader()`. Today `getReaderProvider()` returns the
`Indexer`, and for a plain index that is `EngineBackedIndexer`, whose `acquireReader()`
throws `UnsupportedOperationException`. That is the gap.

### Where the code goes

**In `IndexShard`, not in `EngineBackedIndexer`.**

`EngineBackedIndexer` holds a single field, `private final Engine engine`. It has no
`IndexShard` reference, so it cannot call `IndexShard.acquireSearcher(...)`. Implementing
the bridge there would force the adapter to go at the `Engine` directly, which loses the
shard's reader wrappers — see "Why acquireSearcher" below.

So: change `IndexShard.getReaderProvider()`, currently

```java
public IndexReaderProvider getReaderProvider() {
    return getIndexer();
}
```

to return a shard-aware `IndexReaderProvider` when the indexer is engine-backed, and the
indexer itself otherwise (composite path unchanged). The adapter is an inner class of
`IndexShard` so `acquireSearcher` is directly in reach. `EngineBackedIndexer` is not
modified at all — its `acquireReader()` stub simply stops being on this path.

**Check before implementing:** confirm no existing caller of `getReaderProvider()` casts
the result back to `Indexer`. The declared return type is `IndexReaderProvider`, which
only exposes `acquireReader()`, so this should be safe — but verify rather than assume.

### Why `acquireSearcher`

Acquire through `IndexShard.acquireSearcher("analytics-dv")`, not the `Engine`:

- It inherits every reader wrapper the shard applies — soft-deletes filtering, and any
  security plugin document-level/field-level wrappers. Analytics must not see documents
  `_search` would hide.
- It gives analytics the same NRT refresh point as `_search`, because it is literally the
  same call. Analytics sees data as of the last refresh. Document this; it is correct and
  expected.

### The `Reader` contract

`IndexReaderProvider.Reader` has three methods. All three need an answer:

```java
CatalogSnapshot catalogSnapshot();
Object reader(DataFormat format);
<R> R getReader(DataFormat format, Class<R> readerType);
```

- `getReader(luceneFormat, LuceneReader.class)` → a `LuceneReader` built over the acquired
  searcher's `DirectoryReader`. Its `searcher(queryCache, cachingPolicy)` builds the
  `IndexSearcher` with the analytics engine's cache and policy. Return `null` (or throw,
  matching existing composite behavior) for any other format.
- `reader(format)` → the same object, untyped.
- `catalogSnapshot()` → `EngineBackedIndexer.acquireSnapshot()` already yields a
  `SegmentInfosCatalogSnapshot`; use that. Verify what the dv leaf actually does with the
  snapshot — if it ignores it on this path, say so explicitly in a comment rather than
  leaving it implied.

`LuceneReader`'s constructor also takes a `generationToSegmentName` map. Pass `Map.of()`.
Writer generations are a composite concept; the dv read path (`dv/`,
`LuceneScanInstructionHandler`) contains no references to them. Stated explicitly because
if something downstream *does* read it, an empty map fails as a wrong answer rather than
an exception.

### Lease discipline

This is the part most likely to bite. The returned `GatedCloseable<Reader>`'s `close()`
must release the engine searcher exactly once, on every path. The existing dv-leaf
lifecycle already ties reader release to `leaf_close` (stream drop), so the bridge slots
into that contract unchanged.

A leaked engine searcher blocks shard close. That is how this bug presents in production,
so the tests assert on shard close, not just on refcounts.

### Deletes

The PoC's explicit liveDocs check in the scan loop stays load-bearing — scorers do not
apply deletions. Test on a plain index that has actual deletes.

## Seam B — Planner viability (open decision)

`FieldStorageResolver` derives per-field backend viability from
`index.composite.primary_data_format` / `secondary_data_formats`. A plain index has
neither, so nothing resolves as scannable and the planner never routes to the lucene
backend.

The fallback needs, per field: does it have doc values (→ columnar-scannable via the dv
leaf), and is it indexed (→ delegation-capable, since the predicate can become a Lucene
query through the existing serialized-`QueryBuilder` → `MappedFieldType.toQuery` path).
Fields failing both — `text`, `doc_values:false` — resolve as unsupported and feed the
existing typed-rejection behavior.

**The open decision is where that lookup happens.** The factory today is
`Function<IndexMetadata, FieldStorageResolver>`, and the planner runs coordinator-side.

- **Option 1 — parse mappings from `IndexMetadata`.** Fits the existing signature and
  works on the coordinator. Cost: `IndexMetadata` carries raw mapping JSON, so this
  re-implements mapper semantics — per-type doc_values defaults, `text` having none,
  `keyword` with `doc_values:false`, multi-fields, aliases, dynamic templates. It will
  drift from `MapperService`, and the drift is silent (wrong routing, not an exception).
  If chosen, pin tests against mapper defaults for every supported type.

- **Option 2 — resolve field storage shard-side**, where `MapperService.fieldType(name)
  .hasDocValues()` is available and authoritative. Accurate by construction. Cost: the
  planner needs the information at planning time, so this means plumbing or a protocol
  change.

The existing composite path avoids this entirely because it reads *settings*, not
*mappings* — so this fallback is a genuinely different kind of lookup, and reusing the
same input type is what makes Option 1 fragile.

**Settle this before writing code.** The first task is to determine what field-level
information planning already has access to shard-side; that answer picks the option.

### Routing

`DistributedLeafBridge.open()` gains a third leg beside native-parquet and
composite-lucene: plain index → Seam A reader → the same `openDocValuesLeaf` flow. The
`isDocValuesPrimary` check generalizes to "composite with lucene primary, OR plain index
with analytics enabled."

### Opt-in

A dynamic index setting (`index.analytics.scan.enabled`, default `false`) gates the path,
so rollout is per-index and reversible. The end state is automatic — any plain index with
qualifying mappings is scannable, zero configuration — so design the resolver such that
flipping the default is a one-line change, and say so in the code comment so the setting
does not ossify into architecture.

## Tests

1. **Differential:** the existing 18-query dv-leaf IT corpus passes against a plain index,
   results matching `_search` equivalents where comparable.
2. **Coexistence:** on the same plain index, interleave `_search`, `_count`, `_cat` and
   analytics queries. All succeed; DSL behavior is unchanged with the opt-in enabled.
3. **Deletes:** analytics results exclude deleted docs, matching `_search`.
4. **Lease hygiene:** the leak suite on the plain-index route — repeated queries,
   cancel-mid-query, error-mid-open — each asserting the shard can be closed afterward.
5. **Resolver:** unit tests for the capability matrix (doc_values on/off × index on/off ×
   field type), plus typed rejection for unsupported fields. Scope depends on which
   Seam B option is chosen; Option 1 needs substantially more of these.
6. **Regression:** composite-path ITs (parquet and composite-lucene dv) unchanged, green.

## Known risks

Listed as risks, not as resolved items.

- **Security wrappers.** The reason Seam A goes through `IndexShard.acquireSearcher`. Any
  path that reaches the `Engine` directly is a review-blocking finding — and note this is
  exactly why the bridge lives in `IndexShard` rather than `EngineBackedIndexer`.
- **Seam B accuracy.** Unresolved until the option above is picked. Option 1's failure
  mode is silent misrouting, which is worse than a crash and needs test coverage
  proportional to that.
- **`QueryShardContext` on plain indices.** `buildMinimalQueryShardContext` must work
  without composite settings. The mapper service and named-writeable registry it needs are
  standard shard facilities, so this should hold — unverified.
- **`catalogSnapshot()` semantics.** What the dv leaf does with the snapshot on this path
  is not yet established.
- **No index sort.** Plain log indices typically lack `index.sort`. The dv path works
  regardless — delegation and exact doc IDs do not need it — but sparse-index/skipper
  advantages do not apply until users adopt sort settings. One doc line so nobody expects
  them for free.
- **Segment count.** Plain indices under active ingestion have many small segments. The
  per-segment scan handles it, but segment-parallelism tuning will matter more here than
  on force-merged test fixtures.

## Definition of done

Both seams implemented; the six test groups green; the dv-leaf benchmark harness runnable
against a plain index with one added leg — plain-index dv vs composite-lucene dv on
identical data. The delta should be near zero; that is itself the evidence the bridge is
thin. Plus a short README section stating the coexistence model: *normal index, normal
DSL, normal merges — analytics is an additional reader, not a different engine.*
