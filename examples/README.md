# ClickHouse-DataFusion Examples

This directory contains standalone examples demonstrating how to use `clickhouse-datafusion`.

**📖 See Also**: [Complete Implementation Guide](../CLICKHOUSE_DATAFUSION_COMPLETE_GUIDE_REVIEWED.md#9-examples--testing) - Section 9 covers all examples and testing in detail.

## Prerequisites

1. **Docker** - Required to run ClickHouse
2. **Rust** - Version 1.75 or later

## Running Examples

### Step 1: Start ClickHouse

Start a ClickHouse container in the background:

```bash
docker run -d --name clickhouse-example \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=password \
  clickhouse/clickhouse-server:latest
```

### Step 2: Run an Example

```bash
# Basic filtering
cargo run --example 01_basic_filtering --features test-utils

# Aggregations
cargo run --example 02_aggregations --features test-utils

# Joins
cargo run --example 03_joins --features test-utils

# Window functions
cargo run --example 04_window_functions --features test-utils

# Federation (requires federation feature)
cargo run --example 05_federation --features "test-utils federation"
```

### Step 3: Clean Up

Stop and remove the container when done:

```bash
docker stop clickhouse-example && docker rm clickhouse-example
```

## Examples Overview

| Example | Description | Features Required |
|---------|-------------|-------------------|
| `01_basic_filtering.rs` | Filter data using DataFrame API and SQL | `test-utils` |
| `02_aggregations.rs` | GROUP BY and aggregation functions | `test-utils` |
| `03_joins.rs` | Join multiple ClickHouse tables | `test-utils` |
| `04_window_functions.rs` | Window functions and CTEs | `test-utils` |
| `05_federation.rs` | Join ClickHouse with local data sources | `test-utils federation` |

## Tips

- Each example is self-contained and can run independently
- Examples automatically create databases and tables as needed
- Data is inserted programmatically, no manual setup required
- All examples include comments explaining each step
- Press `Ctrl+C` to stop a running example

## Troubleshooting

**Port already in use:**
```bash
# Stop existing container
docker stop clickhouse-example
docker rm clickhouse-example
```

**Connection refused:**
```bash
# Check if container is running
docker ps | grep clickhouse-example

# View container logs
docker logs clickhouse-example
```

**Compilation errors:**
Make sure you have the required features enabled. For federation examples:
```bash
cargo run --example 05_federation --features "test-utils federation"
```
