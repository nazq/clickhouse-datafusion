#![allow(unused_crate_dependencies)]
// Basic DataFrame Filtering Example
//
// This example shows how to filter data from ClickHouse using DataFusion's DataFrame API.
//
// # Start ClickHouse (run in terminal):
// ```bash
// docker run -d --name clickhouse-example \
//   -p 9001:9000 -p 8124:8123 \
//   -e CLICKHOUSE_USER=default \
//   -e CLICKHOUSE_PASSWORD=password \
//   clickhouse/clickhouse-server:latest
// ```
//
// # Run this example:
// ```bash
// cargo run --example 01_basic_filtering --features test-utils
// ```
//
// # Stop container when done:
// ```bash
// docker stop clickhouse-example && docker rm clickhouse-example
// ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Basic Filtering Example\n");

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Connect to ClickHouse
    let clickhouse = ClickHouseBuilder::new("localhost:9001")
        .configure_client(|c| c.with_username("default").with_password("password"))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Define schema for users table
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("department", DataType::Utf8, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    // Create database and table
    let clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created database and table");

    // Insert sample data
    ctx.sql(
        "INSERT INTO clickhouse.example_db.users (user_id, name, age, department, salary) VALUES \
         (1, 'Alice', 30, 'Engineering', 75000.0), (2, 'Bob', 25, 'Engineering', 65000.0), (3, \
         'Carol', 35, 'Sales', 80000.0), (4, 'Dave', 28, 'Sales', 70000.0), (5, 'Eve', 40, \
         'Engineering', 95000.0)",
    )
    .await?
    .collect()
    .await?;

    println!("✓ Inserted sample data\n");

    // Wait for ClickHouse replication
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: Simple filter (age > 25)
    println!("Example 1: Filter users where age > 25\n");
    let df = ctx.table("clickhouse.example_db.users").await?.filter(col("age").gt(lit(25)))?;

    let results = df.collect().await?;
    print_batches(&results)?;

    // Example 2: Multiple conditions
    println!("\nExample 2: Filter Engineering department with salary > 70000\n");
    let df = ctx
        .table("clickhouse.example_db.users")
        .await?
        .filter(col("department").eq(lit("Engineering")).and(col("salary").gt(lit(70000))))?;

    let results = df.collect().await?;
    print_batches(&results)?;

    // Example 3: Using SQL instead
    println!("\nExample 3: Same query using SQL\n");
    let df =
        ctx.sql("SELECT * FROM clickhouse.example_db.users WHERE age BETWEEN 30 AND 40").await?;

    let results = df.collect().await?;
    print_batches(&results)?;

    println!("\n✅ Example completed successfully!");

    Ok(())
}

fn print_batches(batches: &[RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    datafusion::arrow::util::pretty::print_batches(batches)?;
    Ok(())
}
