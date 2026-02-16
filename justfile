# Run the thread_per_core multithread stress test with eBPF balancer
test-multithread:
    cargo test test_http3_multithread_stress --features test-monitor --no-run 2>&1 \
        | grep -oP 'Executable tests/http3_test.rs \(\K[^)]+' \
        | xargs -I{} sudo {} test_http3_multithread_stress --nocapture
