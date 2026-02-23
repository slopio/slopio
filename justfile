TEMPO_CONFIG := "tempo.yaml"

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

# Run the thread_per_core multithread stress test with eBPF balancer
test-multithread:
    cargo test test_http3_multithread_stress --features test-monitor --no-run 2>&1 \
        | grep -oP 'Executable tests/http3_test.rs \(\K[^)]+' \
        | xargs -I{} sudo {} test_http3_multithread_stress --nocapture
