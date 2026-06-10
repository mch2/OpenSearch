# SQL-plugin → analytics-engine repro suite

Reproduces every failing upstream `opensearch-project/sql` `calcite.remote.*` / `ppl.*` test against
our analytics-engine route (`POST /_plugins/_ppl`, parquet+lucene, `cluster.pluggable.dataformat=composite`)
to confirm the failure and bucket the root cause. Repro ITs: `src/test/.../Calcite*ReproIT.java`
(base `CalciteReproTestCase`); datasets: `src/test/resources/datasets/`. **Diagnosis only — no engine changes.**

## Status — all 229 classified
| | count |
|---|---|
| 🔴 reproduced + root-caused | 144 |
| ⚪ pass on AE route | 69 |
| ⏭ skipped per policy (geo_point / nested-in-query / can't-disable-calcite) | 16 |

Per-test table: `PROGRESS.tsv`. Bucket descriptions: `FAILURES.md`.

## Run
JDK 25 + protoc 25 required (system protoc is 2.5 and fails the substrait build):
```
JAVA_HOME=~/.sdkman/candidates/java/25.0.3-amzn PROTOC=/local/home/handalm/.local/protoc/bin/protoc \
PATH=/local/home/handalm/.local/protoc/bin:$PATH \
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true --tests "...ReproIT"
```
- If the cluster won't boot (`NativeBridge.<clinit> NoSuchElementException`), the native `.so` is
  stale — rebuild: `./gradlew :sandbox:libs:dataformat-native:buildRustLibrary` (slow LTO link).
- randomizedtesting only runs `test`-prefixed methods; the gradle "Tests with failures" log is
  authoritative (the per-class XML can show stale/duplicate entries).
- Cluster boot (~2-3 min) dominates — batch many `--tests` per invocation.

## Highest-leverage buckets (fix these first)
- **A** (17) full-table schema columns come back alphabetical, not declaration order — one planner spot.
- **R** (14) + **V** (11): unsupported scalar/agg functions — uniform "add a DataFusion binding" fixes.
- **D** (10) implicit post-source filter dropped; **H** (10) error message/status divergence.
