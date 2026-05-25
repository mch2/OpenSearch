# ClickBench manual harness

Adjacent helper for ingesting and querying the ClickBench dataset against a
running OpenSearch cluster (e.g. `./gradlew :run`). Used for hand-driving
shard-fanout correctness checks without booting the full integTest fixture.

## Files

- `mapping.json` — index template; carries the `composite` / `parquet` settings inline.
- `bulk.json` — 1000 docs, NDJSON action+source pairs.
- `ppl/q*.ppl` — one PPL query per file; source is the literal name `clickbench` (the script rewrites it to whatever index you target).
- `sql/q*.sql` — same naming convention for SQL queries (drop them in to enable `-d sql`).
- `run.sh` — the harness. Self-locating; run it from anywhere.

## Prereqs

- Cluster on `localhost:9200` (or override `HOST`).
- Cluster has the analytics-backend plugin loaded so the parquet data format is registered.
- `jq` and `curl` on `$PATH`. `compare` also needs `python3`.

## Subcommands

```
./run.sh ingest [-i INDEX] [-s SHARDS]
./run.sh dual   [-l LEFT_SHARDS] [-r RIGHT_SHARDS] [-p PREFIX]
./run.sh query  [-i INDEX] [-q QNUM] [-d ppl|sql]
./run.sh compare [-q QSPEC] [-l L] [-r R] [-p PREFIX] [-d ppl|sql]
```

- `ingest` — drop+recreate one index, bulk-load, force-flush, wait for green.
- `dual` — same but builds two indices in one shot (default `clickbench_1` with 1 shard and `clickbench_4` with 4). Named `<prefix>_<shards>`.
- `query` — POST `ppl/qN.ppl` (or `sql/qN.sql`) to `/_plugins/_ppl` (or `/_plugins/_sql`) and pretty-print.
- `compare` — runs `dual`, then runs each query against both sides and reports `q$N: match` / `q$N: DIFFER` plus a unified diff snippet. `QSPEC` accepts `13`, `1-10`, or `1,3,5`; omit to run every query file present.

## Examples

```bash
# Single index, run one query
./run.sh ingest
./run.sh query -q 13

# 1-shard ground truth vs 4-shard distributed, all PPL queries
./run.sh compare

# Just a few queries, SQL dialect
./run.sh compare -q 1-10 -d sql

# Custom shard counts / prefix
./run.sh compare -l 1 -r 8 -p mytest

# Different host
HOST=127.0.0.1:9300 ./run.sh ingest
```

## Adding a SQL suite

`sql/` doesn't exist yet. To add it:

```bash
mkdir sql
echo "SELECT count(*) FROM clickbench" > sql/q1.sql
```

Use the literal name `clickbench` as the table — the script substitutes the
target index per invocation. Once any `sql/qN.sql` is present, `compare -d sql`
picks them up automatically.

## Output shape

`compare` diffs `.rows` (falls back to `.data` or the whole body) extracted by
`jq -c`. If the response shape ever changes, edit the two `run_query | jq -c …`
lines in `run.sh`.
