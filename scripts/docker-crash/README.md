# Reproduce the EC2 OOM-kill in a memory-limited Linux container

macOS has no cgroup OOM-killer, so a host `gradle run` only thrashes — it won't
reproduce the clean "kernel kills the node" you see on EC2. This kit runs the
analytics node inside a Linux container with a hard `--memory` limit so the
cgroup OOM-killer fires exactly like the EC2 instance.

## What it does
1. `Dockerfile` — JDK 25 + Rust toolchain (the build environment).
2. `entrypoint.sh` (inside the container): builds the **Linux** native `.so`,
   starts the node via `gradle run` with the analytics plugins + feature flags,
   waits for it, then runs `scripts/crash-node.sh` to hammer it with concurrent
   high-cardinality `stats … by user_id, phrase` queries (clickbench-q19 shape).
3. `run.sh` (on the host): builds the image and `docker run --memory=<MEM>` it.

The repo is **bind-mounted**; build outputs go to container-local
`.docker-gradle-home` / `.docker-cargo-target` so the host's macOS artifacts are
never touched.

## Run it
```bash
# default 6g (lets the build + idle node fit; load pushes past it)
./scripts/docker-crash/run.sh

# crash faster on a warm cache:
./scripts/docker-crash/run.sh 3g 32
```

## Confirming the crash
`run.sh` uses `--rm`; to capture the OOM signal, run without it and inspect:
```bash
docker run --name analytics-crash --memory=4g --memory-swap=4g \
  -v "$PWD":/workspace analytics-crash:latest
# after it dies:
docker inspect --format '{{.State.OOMKilled}}' analytics-crash   # → true
```
On the Docker VM you can also see `dmesg | grep -i 'killed process'`.

## Tuning toward a crash
- **`--memory`**: start at 6g; once caches are warm, drop to 3–4g so the load
  crosses the limit quickly.
- **`CONCURRENCY`** / **`CARDINALITY`** / **`DOCS`** (env on `docker run`): more
  concurrency and higher cardinality = faster native-memory growth.
- The node heap is left default and the gradle launcher is capped (`-Xmx512m`)
  so the dominant growing memory is the **DataFusion/Arrow native pool +
  ungated CPU-executor tasks** — the actual EC2 failure axis.

## Caveats
- First run is slow (full Java + Rust build in-container).
- arm64 on Apple silicon (EC2 is often amd64) — OOM behavior is arch-independent,
  but add `--platform linux/amd64` to `docker build`/`run` if you want to match.
- This intentionally runs `gradle run` (not an assembled distribution) to avoid
  packaging the experimental sandbox plugins into a distro. The trade-off is the
  gradle launcher's small constant overhead inside the limit.
