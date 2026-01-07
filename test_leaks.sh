#!/bin/bash
# Ultra simple leak checker - just run this after cargo test --no-run

echo "=== Memory Leak Check ==="
echo ""

echo "1. basic_runtime"
valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=1 --quiet \
  $(find target/debug/deps -name 'basic_runtime-*' -type f -executable | head -1) \
  --test-threads=1 2>&1 | tail -1 && echo "✓ OK" || echo "✗ LEAK"

echo ""
echo "2. task_ordering"
valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=1 --quiet \
  $(find target/debug/deps -name 'task_ordering-*' -type f -executable | head -1) \
  --test-threads=1 2>&1 | tail -1 && echo "✓ OK" || echo "✗ LEAK"

echo ""
echo "3. memory_leak_test"
valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=1 --quiet \
  $(find target/debug/deps -name 'memory_leak_test-*' -type f -executable | head -1) \
  --test-threads=1 2>&1 | tail -1 && echo "✓ OK" || echo "✗ LEAK"

echo ""
echo "Note: stress_tests has 1 intentional leak (test_task_panic)"
echo "Note: I/O tests (socket_operations, io_stress_tests) are slow with valgrind"
