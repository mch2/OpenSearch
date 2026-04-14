# How Parquet Data is Mocked for Query Testing

Since parquet indexing is not yet fully wired, mock parquet data is injected at the `DatafusionReaderManager` level so the real reader pipeline is exercised during testing. All mock code is tagged with `[indexing-mock]` for easy removal once parquet indexing is wired.

## Flow

### 1. Shard startup (`IndexShard` constructor)

When the shard starts, `DataFormatAwareEngine` is created with a `CatalogSnapshotManager` (empty initial snapshot) and reader managers for each registered data format (including `DatafusionReaderManager` for parquet).

### 2. Refresh bridge (`IndexShard.newEngineConfig`)

A Lucene `RefreshListener` is registered that bridges Lucene's refresh cycle to the data format reader managers. On every Lucene refresh:
- Commits a new empty `CatalogSnapshot` (no real parquet segments)
- Calls `afterRefresh(didRefresh, snapshot)` on each reader manager

### 3. Mock injection (`DatafusionReaderManager.afterRefresh`)

When `afterRefresh` fires, it checks `catalogSnapshot.getSearchableFiles("parquet")`. Since parquet indexing isn't wired, this returns empty. The reader manager then:
1. Creates the shard's `<dataPath>/parquet/` directory
2. Copies `clickbench_hits_100.parquet` from the plugin's classpath into it (once)
3. Creates a real `DatafusionReader` pointing to that file
4. Stores the reader in the `readers` map keyed by the `CatalogSnapshot`

### 4. Query time (`AnalyticsSearchService.executeFragment`)

When a query arrives:
- Calls `compositeEngine.acquireReader()` → `CatalogSnapshotManager.acquireSnapshot()` → gets the latest snapshot
- `DatafusionReaderManager.getReader(snapshot)` → returns the real `DatafusionReader` created in step 3
- `DataFusionAnalyticsExtension.createSearchExecEngine()` gets the reader from `ctx.getReader()` and executes the Substrait plan against the mock parquet file via the native DataFusion engine
