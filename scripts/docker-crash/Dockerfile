# Builder image: JDK 25 + Rust toolchain to compile the LINUX native lib and run
# the analytics node inside a memory-limited Linux container (faithful EC2 OOM-kill).
# The repo is bind-mounted at run time (see run.sh) — this image is just the toolchain.
FROM eclipse-temurin:25-jdk

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl git pkg-config libssl-dev ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

# Rust (for cargo build -p opensearch-native-lib --release → produces a linux .so)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
ENTRYPOINT ["/workspace/scripts/docker-crash/entrypoint.sh"]
