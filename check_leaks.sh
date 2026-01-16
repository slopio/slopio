#!/bin/bash
# Simple memory leak checker with valgrind
# Usage: ./check_leaks.sh

set -e

echo "=== Building tests ==="
cargo test --no-run --quiet
echo ""

echo "=== Running Valgrind on all test suites ==="
echo ""

TOTAL=0
PASSED=0
FAILED=0

# List of all test suites to check
TESTS=(
    "basic_async_test"
    "task_ordering"
    "joins_test"
    "memory_leak_test"
    "multi_recv_test"
    "sockets_test"
    "file_io_test"
    "stress_tests"
    "io_stress_tests"
)

for test_name in "${TESTS[@]}"; do
    TOTAL=$((TOTAL + 1))
    echo "[$TOTAL/${#TESTS[@]}] Checking $test_name..."

    # Find the test binary
    binary=$(find target/debug/deps -name "${test_name}-*" -type f -executable ! -name '*.so' 2>/dev/null | head -1)

    if [ -z "$binary" ]; then
        echo "  ⚠ SKIP (not found)"
        TOTAL=$((TOTAL - 1))
        continue
    fi

    # Run valgrind
    output=$(valgrind \
        --leak-check=full \
        --errors-for-leak-kinds=definite \
        --error-exitcode=1 \
        --quiet \
        "$binary" --test-threads=1 2>&1)

    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "  ✓ OK"
        PASSED=$((PASSED + 1))
    else
        # Check if this is the expected leak in stress_tests
        if [ "$test_name" = "stress_tests" ]; then
            # stress_tests has 1 intentional leak in test_task_panic
            leak_count=$(echo "$output" | grep -oP 'definitely lost: \K\d+' | head -1)
            if [ "$leak_count" -lt 100 ]; then
                echo "  ✓ OK (expected small leak from test_task_panic)"
                PASSED=$((PASSED + 1))
            else
                echo "  ✗ LEAK (unexpected: $leak_count bytes)"
                echo "$output" | grep -A 5 "LEAK SUMMARY"
                FAILED=$((FAILED + 1))
            fi
        else
            echo "  ✗ LEAK"
            echo "$output" | grep -A 5 "LEAK SUMMARY"
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo ""
echo "=== Summary ==="
echo "Total:  $TOTAL"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "✓ No memory leaks detected!"
    exit 0
else
    echo "✗ Memory leaks found!"
    exit 1
fi
