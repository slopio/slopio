#!/bin/bash

# Quick valgrind test for a specific test or test binary
# Usage: ./valgrind_quick.sh [test_name]
# Example: ./valgrind_quick.sh memory_leak_test

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TEST_NAME=${1:-"memory_leak_test"}

echo -e "${BLUE}=== Quick Valgrind Test: ${TEST_NAME} ===${NC}"
echo ""

# Build the test
echo -e "${YELLOW}Building ${TEST_NAME}...${NC}"
cargo test --test "$TEST_NAME" --no-run 2>&1 | grep -E "Finished|Executable" || true
echo ""

# Find the test binary
BINARY=$(find target/debug/deps -type f -executable -name "${TEST_NAME}-*" ! -name '*.so' | head -1)

if [ -z "$BINARY" ]; then
    echo -e "${RED}Error: Test binary not found for ${TEST_NAME}${NC}"
    exit 1
fi

echo -e "${BLUE}Running Valgrind on: $(basename $BINARY)${NC}"
echo ""

# Run valgrind with detailed output
valgrind \
    --leak-check=full \
    --show-leak-kinds=all \
    --track-origins=yes \
    --verbose \
    --error-exitcode=1 \
    "$BINARY" \
    2>&1 | tee /tmp/valgrind_output.log

# Check results
echo ""
echo -e "${BLUE}=== Summary ===${NC}"

if grep -q "definitely lost: 0 bytes" /tmp/valgrind_output.log; then
    echo -e "${GREEN}✓ No memory leaks detected!${NC}"

    # Show summary
    echo ""
    grep -A 5 "LEAK SUMMARY" /tmp/valgrind_output.log

    exit 0
else
    echo -e "${RED}✗ Memory leaks detected!${NC}"

    # Show summary
    echo ""
    grep -A 5 "LEAK SUMMARY" /tmp/valgrind_output.log

    exit 1
fi
