# Analytics Engine REST Integration Tests

REST-based integration tests for the analytics engine, running against a live OpenSearch cluster.

## Test Classes

| Test | Backend | Description |
|------|---------|-------------|
| `LuceneScanRestIT` | Lucene | PPL queries through the Lucene scan path |
| `ClickBenchRestIT` | DataFusion | ClickBench workload queries (PPL, legacy) |
| `DslClickBenchIT` | DataFusion | ClickBench Q1 via DSL `_search` |
| `PplClickBenchIT` | DataFusion | ClickBench Q1 via PPL `/_plugins/_ppl` |

## Prerequisites

### OpenSearch Core

Publish OpenSearch core to your local Maven repository:

```bash
./gradlew publishToMavenLocal -x test -x javadoc
```

### SQL Plugin (PPL tests only)

The analytics engine depends on the SQL plugin built from the `feature/mustang-ppl-integration` branch: https://github.com/vinaykpud/sql/commits/feature/mustang-ppl-integration/

Clone and publish it to your local Maven repository:

```bash
git clone -b feature/mustang-ppl-integration https://github.com/vinaykpud/sql.git
cd sql
./gradlew publishToMavenLocal -x test -x javadoc
```

## Running ClickBench Tests (DSL & PPL)

ClickBench tests use the standard [ClickBench](https://github.com/ClickHouse/ClickBench) dataset schema (103 fields) with 100 rows of generated data matching `clickbench_hits_100.parquet`.

| Test | Type | Description |
|------|------|-------------|
| `DslClickBenchIT` | DSL | Runs ClickBench Q1 via `_search` DSL |
| `PplClickBenchIT` | PPL | Runs ClickBench Q1 via `/_plugins/_ppl` |

Both extend `ClickBenchBaseIT` which creates the `parquet_hits` index and bulk-ingests 100 documents.

### Test Resources

```
src/test/resources/clickbench/
  parquet_hits_mapping.json   # Index mapping (matches ClickBench ES schema)
  bulk.json                   # 100 documents (same data as clickbench_hits_100.parquet)
  dsl/q1.json ... q43.json   # 43 ClickBench DSL queries
  ppl/q1.ppl ... q43.ppl     # 43 ClickBench PPL queries
```

### Running DSL ClickBench

```bash
# Build
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:parquet-data-format:assemble :sandbox:plugins:dsl-query-executor:assemble -x javadoc -x test

# Start cluster
./gradlew run -PinstalledPlugins="['analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', 'dsl-query-executor']"

# Run test (separate terminal)
./gradlew :sandbox:qa:analytics-engine-rest:restTest \
  --tests "org.opensearch.analytics.qa.DslClickBenchIT" \
  -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=runTask
```

### Running PPL ClickBench

Requires the same [prerequisites](#prerequisites) as DataFusionScanRestIT (OpenSearch Core + SQL Plugin published to mavenLocal).

```bash
# Build
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:parquet-data-format:assemble -x javadoc -x test

# Start cluster
./gradlew run -PinstalledPlugins="['opensearch-job-scheduler', 'opensearch-sql-plugin', 'parquet-data-format', 'analytics-engine', 'analytics-backend-datafusion']"

# Run test (separate terminal)
./gradlew :sandbox:qa:analytics-engine-rest:restTest \
  --tests "org.opensearch.analytics.qa.PplClickBenchIT" \
  -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=runTask
```

## Manual Testing — PPL Queries

Requires [prerequisites](#prerequisites) (OpenSearch Core + SQL Plugin published to mavenLocal).

### 1. Build and start

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:parquet-data-format:assemble -x javadoc -x test

./gradlew run -PinstalledPlugins="['opensearch-job-scheduler', 'opensearch-sql-plugin', 'parquet-data-format', 'analytics-engine', 'analytics-backend-datafusion']"
```

### 2. Create the index and ingest data

```bash
curl -X PUT "http://localhost:9200/parquet_hits" -H "Content-Type: application/json" \
  -d @sandbox/qa/analytics-engine-rest/src/test/resources/clickbench/parquet_hits_mapping.json

curl -X POST "http://localhost:9200/parquet_hits/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" \
  --data-binary @sandbox/qa/analytics-engine-rest/src/test/resources/clickbench/bulk.json
```

### 3. Run queries

```bash
# COUNT(*)
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source = parquet_hits | stats count()"}'

# SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source = parquet_hits | stats sum(AdvEngineID), count(), avg(ResolutionWidth)"}'

# COUNT(*) GROUP BY BrowserCountry
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source = parquet_hits | stats count() as c by BrowserCountry | sort - c | head 10"}'

# MIN/MAX EventDate
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source = parquet_hits | stats min(EventDate), max(EventDate)"}'

# Filter + Aggregation
curl -X POST "http://localhost:9200/_plugins/_ppl" -H "Content-Type: application/json" \
  -d '{"query": "source = parquet_hits | where AdvEngineID != 0 | stats count() by AdvEngineID | sort - count()"}'
```

## Manual Testing — DSL Queries

### 1. Build and start

```bash
./gradlew :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:parquet-data-format:assemble :sandbox:plugins:dsl-query-executor:assemble -x javadoc -x test

./gradlew run -PinstalledPlugins="['analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', 'dsl-query-executor']"
```

### 2. Create the index and ingest data

```bash
curl -X PUT "http://localhost:9200/parquet_hits" -H "Content-Type: application/json" \
  -d @sandbox/qa/analytics-engine-rest/src/test/resources/clickbench/parquet_hits_mapping.json

curl -X POST "http://localhost:9200/parquet_hits/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" \
  --data-binary @sandbox/qa/analytics-engine-rest/src/test/resources/clickbench/bulk.json
```

### 3. Run queries

```bash
# SUM(GoodEvent) — count equivalent
curl -s "http://localhost:9200/parquet_hits/_search" -H "Content-Type: application/json" \
  -d '{"size": 0, "aggs": {"count": {"sum": {"field": "GoodEvent"}}}}'

# SUM(AdvEngineID)
curl -s "http://localhost:9200/parquet_hits/_search" -H "Content-Type: application/json" \
  -d '{"size": 0, "aggs": {"total": {"sum": {"field": "AdvEngineID"}}}}'

# AVG(ResolutionWidth)
curl -s "http://localhost:9200/parquet_hits/_search" -H "Content-Type: application/json" \
  -d '{"size": 0, "aggs": {"avg_width": {"avg": {"field": "ResolutionWidth"}}}}'

# SUM(Age) with filter
curl -s "http://localhost:9200/parquet_hits/_search" -H "Content-Type: application/json" \
  -d '{"size": 0, "query": {"range": {"Age": {"gt": 30}}}, "aggs": {"total_age": {"sum": {"field": "Age"}}}}'
```

### Notes

- Mock parquet data (`clickbench_hits_100.parquet`) is injected at the `DatafusionReaderManager` level during Lucene refresh — no real parquet indexing needed.
- The bulk.json data and the mock parquet file contain identical rows.
- DSL queries go through: `_search` → dsl-query-executor → Calcite planning → Substrait → DataFusion.
- PPL queries go through: `/_plugins/_ppl` → SQL plugin → analytics-engine → Calcite → Substrait → DataFusion.
