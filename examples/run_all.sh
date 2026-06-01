#!/bin/bash
# Run all examples and fail if any fail
# This script tests all clickhouse-datafusion examples with proper feature flags
# Examples and their features are automatically discovered from Cargo.toml

set -e  # Exit immediately if any command fails

# Start total timer
total_start=$(date +%s)

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

# Run each example.
#
# Split build and run so we can report each separately. `cargo run` does both
# in one invocation, which means a slow release compile looks identical to a
# hung example. With `cargo build` then a direct binary execution, output is:
#
#   ✅ PASS: 02_aggregations (run 3s, build 24s)
#
# A warm cache will report build as 0–1s; a cold cache shows the real cost.
while IFS=$'\t' read -r example features; do
    current=$((current + 1))

    echo "[$current/$total] Running example: $example (features: $features)"
    echo "---------------------------------------------------"

    # --- Build ---
    build_start=$(date +%s)
    if ! cargo build --example "$example" --features "$features" --release > /dev/null 2>&1; then
        build_end=$(date +%s)
        build_elapsed=$((build_end - build_start))
        echo "❌ FAIL (build): $example (build ${build_elapsed}s)"
        failed=$((failed + 1))
        echo ""
        echo "Example $example failed to build. Aborting..."
        exit 1
    fi
    build_end=$(date +%s)
    build_elapsed=$((build_end - build_start))

    # --- Run ---
    # Execute the binary directly so we time only runtime, not cargo's
    # up-to-date check.
    run_start=$(date +%s)
    if "./target/release/examples/$example" > /dev/null 2>&1; then
        run_end=$(date +%s)
        run_elapsed=$((run_end - run_start))
        echo "✅ PASS: $example (run ${run_elapsed}s, build ${build_elapsed}s)"
        passed=$((passed + 1))
    else
        run_end=$(date +%s)
        run_elapsed=$((run_end - run_start))
        echo "❌ FAIL (run): $example (run ${run_elapsed}s, build ${build_elapsed}s)"
        failed=$((failed + 1))
        echo ""
        echo "Example $example failed at runtime. Aborting..."
        exit 1
    fi

    echo ""
done <<< "$examples_data"

# Summary
total_end=$(date +%s)
total_elapsed=$((total_end - total_start))

echo "================================================"
echo "✅ All examples passed! ($passed/$total)"
echo "⏱️  Total time: ${total_elapsed}s"
echo "================================================"

exit 0
