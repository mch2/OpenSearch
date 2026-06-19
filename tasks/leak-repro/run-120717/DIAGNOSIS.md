# Leak isolation — coordinator REDUCE pool

Query: source=clickbench | stats count() by WatchID, ClientIP  (cancel_after=200ms, 8 concurrent)

| stage | reduce alloc | query | datafusion-native | alive_tasks |
|-------|-------------|-------|-------------------|-------------|
| baseline        | 0     | 0   | 53MB(idle) | 0 |
| t+0 (drained)   | 19MB  | 0   | 53MB | 0 (spawned=688) |
| t+15/30/60/150  | 19MB  | 0   | 53MB | 0  (FLAT - stuck) |
| after 2nd cycle | 29MB  | 0   | 53MB | 0  (ACCUMULATES) |

## Verdict
- LEAK IS ON THE COORDINATOR REDUCE PATH (pool.reduce), NOT data-node scan (pool.query) and NOT the DataFusion native pool.
- query pool peaked 319MB then freed to 0 cleanly. datafusion native pool never grew. flight freed.
- reduce pool: allocated stays elevated forever after drain, grows per cancelled-query batch.
- Cancelled queries took 17-40s to return despite 200ms cancel -> cleanup lags badly.
- All 8 returned TaskCancelled (6x500) / rejected (2x429).

## Where to look
Coordinator reduce allocator = CoordinatorAllocatorHandle, child of POOL_REDUCE,
created in AnalyticsPlugin.createComponents. Consumed by DefaultPlanExecutor reduce path.
On task cancellation, the reduce-stage Arrow buffers (VectorSchemaRoot / batches held by
the reduce sink) are not closed -> child allocator retains them.
