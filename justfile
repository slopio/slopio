TEMPO_CONFIG := "tempo.yaml"

# What Miri needs to get through the simulated ring. `disable-isolation` is for
# the clock; `permissive-provenance` and `tree-borrows` go together, because the
# buf-ring ABI parks buffer addresses in a u64 and reads them back as pointers —
# Stacked Borrows refuses the resulting retag, Tree Borrows allows it.
MIRIFLAGS := "-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows"

# Every suite that runs without a kernel. Naming them is not optional: with no
# `--test`, Miri also builds the lib's inline unit tests, which open a real
# io_uring and take the whole run down with an unsupported syscall.
MIRI_SUITES := "--test channels --test channels_unbounded --test channels_bounded --test channels_vec --test channels_mpsc --test fake_ring --test simulated_runtime --test cancellation --test kill --test kill_drop --test task_free --test multishot_rearm --test root_waker --test buf_ring_reserve --test dst_echo --test actors --test spawn_next"

# Launch Tempo + Grafana, then run the HTTP example
dev: observability
    cargo run --example http

# Start Tempo and Grafana (idempotent)
observability: tempo grafana

# Start Tempo container
tempo:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker network inspect observability &>/dev/null; then
        docker network create observability
    fi
    if ! [ -f {{TEMPO_CONFIG}} ]; then
        cat > {{TEMPO_CONFIG}} <<'EOF'
    server:
      http_listen_port: 3200

    distributor:
      receivers:
        otlp:
          protocols:
            http:
              endpoint: 0.0.0.0:4318

    storage:
      trace:
        backend: local
        local:
          path: /tmp/tempo/blocks
        wal:
          path: /tmp/tempo/wal
    EOF
    fi
    if [ -z "$(docker ps --filter name=^tempo$ --filter status=running -q)" ]; then
        docker run -d --name tempo --network observability \
            -p 4318:4318 -p 3200:3200 \
            -v "$(pwd)/{{TEMPO_CONFIG}}:/etc/tempo/tempo.yaml" \
            grafana/tempo:latest -config.file=/etc/tempo/tempo.yaml
        echo "Tempo started"
    else
        echo "Tempo already running"
    fi

# Start Grafana container
grafana:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "$(docker ps --filter name=^grafana$ --filter status=running -q)" ]; then
        docker run -d --name grafana --network observability \
            -p 3000:3000 \
            -e GF_AUTH_ANONYMOUS_ENABLED=true \
            -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
            grafana/grafana:latest
        echo "Grafana started → http://localhost:3000"
    else
        echo "Grafana already running"
    fi

# Stop and remove Tempo + Grafana containers
down:
    docker rm -f tempo grafana 2>/dev/null || true
    docker network rm observability 2>/dev/null || true

# Install the nightly toolchain and Miri (idempotent)
miri-setup:
    rustup toolchain install nightly
    rustup component add miri --toolchain nightly

# Every kernel-free suite against the simulated ring, without Miri
test-sim *args:
    cargo test -p runtime-core --features simulation {{MIRI_SUITES}} {{args}}

# Check every Miri-able suite for undefined behaviour and leaks
miri *args:
    MIRIFLAGS="{{MIRIFLAGS}}" cargo +nightly miri test -p runtime-core --features simulation {{MIRI_SUITES}} {{args}}

# One suite, optionally one test:  just miri-one cancellation a_multishot
miri-one suite filter="":
    MIRIFLAGS="{{MIRIFLAGS}}" cargo +nightly miri test -p runtime-core --features simulation --test {{suite}} {{filter}}

# The channels alone, which need neither a ring nor a clock
miri-channels:
    MIRIFLAGS="-Zmiri-tree-borrows" cargo +nightly miri test -p runtime-core --test channels

# Run the thread_per_core multithread stress test with eBPF balancer
test-multithread:
    cargo test test_http3_multithread_stress --features test-monitor --no-run 2>&1 \
        | grep -oP 'Executable tests/http3_test.rs \(\K[^)]+' \
        | xargs -I{} sudo {} test_http3_multithread_stress --nocapture
