#!/bin/bash
# Run all examples and fail if any fail
# This script tests all clickhouse-datafusion examples with proper feature flags
# Examples and their features are automatically discovered from Cargo.toml

set -e  # Exit immediately if any command fails

echo "🚀 Running all clickhouse-datafusion examples..."
echo "================================================"
echo ""

# Parse Cargo.toml to extract examples and their required features
examples_data=$(grep -A 2 '^\[\[example\]\]' Cargo.toml | \
    grep -E 'name =|required-features' | \
    paste - - | \
    sed 's/name = "\(.*\)"/\1/' | \
    sed 's/required-features = \[//' | \
    sed 's/\]//' | \
    sed 's/"//g')

# Count total examples
total=$(echo "$examples_data" | wc -l)

# Track success/failure
passed=0
failed=0
current=0

# Run each example
while IFS=$'\t' read -r example features; do
    current=$((current + 1))

    echo "[$current/$total] Running example: $example (features: $features)"
    echo "---------------------------------------------------"

    if cargo run --example "$example" --features "$features" --release > /dev/null 2>&1; then
        echo "✅ PASS: $example"
        passed=$((passed + 1))
    else
        echo "❌ FAIL: $example"
        failed=$((failed + 1))
        echo ""
        echo "Example $example failed. Aborting..."
        exit 1
    fi

    echo ""
done <<< "$examples_data"

# Summary
echo "================================================"
echo "✅ All examples passed! ($passed/$total)"
echo "================================================"

exit 0
