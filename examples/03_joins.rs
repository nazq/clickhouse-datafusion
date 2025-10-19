#![allow(unused_crate_dependencies)]
// Table Joins Example
//
// This example shows how to join multiple ClickHouse tables using DataFusion.
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
// cargo run --example 03_joins --features test-utils
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
use datafusion::logical_expr::JoinType;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Joins Example\n");

    let ctx = SessionContext::new();

    let clickhouse = ClickHouseBuilder::new("localhost:9001")
        .configure_client(|c| c.with_username("default").with_password("password"))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Clean up any existing tables from previous runs
    let _ = ctx.sql("DROP TABLE IF EXISTS clickhouse.example_db.users").await;
    let _ = ctx.sql("DROP TABLE IF EXISTS clickhouse.example_db.orders").await;

    // Create users table
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, true),
    ]));

    let clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, users_schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?;

    // Create orders table
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("total", DataType::Float64, false),
    ]));

    let _clickhouse = clickhouse
        .with_new_table("orders", ClickHouseEngine::MergeTree, orders_schema)
        .update_create_options(|opts| opts.with_order_by(&["order_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created tables");

    // Insert users
    ctx.sql(
        "INSERT INTO clickhouse.example_db.users (user_id, name, department) VALUES (1, 'Alice', \
         'Engineering'), (2, 'Bob', 'Sales'), (3, 'Carol', 'Marketing'), (4, 'Dave', 'Sales')",
    )
    .await?
    .collect()
    .await?;

    // Insert orders
    ctx.sql(
        "INSERT INTO clickhouse.example_db.orders (order_id, user_id, total) VALUES (101, 1, \
         250.50), (102, 1, 150.00), (103, 2, 500.75), (104, 3, 99.99), (105, 1, 75.25)",
    )
    .await?
    .collect()
    .await?;

    println!("✓ Inserted sample data\n");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: INNER JOIN using DataFrame API
    println!("Example 1: INNER JOIN - Users with their orders\n");
    let users = ctx.table("clickhouse.example_db.users").await?;
    let orders = ctx.table("clickhouse.example_db.orders").await?;

    let df = users
        .join(orders, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![col("name"), col("department"), col("order_id"), col("total")])?;

    print_batches(&df.collect().await?)?;

    // Example 2: Aggregated JOIN using SQL
    println!("\nExample 2: Total spending per user (using SQL)\n");
    let df = ctx
        .sql(
            "SELECT u.name, u.department, COUNT(o.order_id) as order_count, SUM(o.total) as \
             total_spent FROM clickhouse.example_db.users u LEFT JOIN \
             clickhouse.example_db.orders o ON u.user_id = o.user_id GROUP BY u.name, \
             u.department ORDER BY total_spent DESC",
        )
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 3: Filter after JOIN
    println!("\nExample 3: Users with total spending > 200\n");
    let df = ctx
        .sql(
            "SELECT u.name, SUM(o.total) as total_spent FROM clickhouse.example_db.users u INNER \
             JOIN clickhouse.example_db.orders o ON u.user_id = o.user_id GROUP BY u.name HAVING \
             SUM(o.total) > 200 ORDER BY total_spent DESC",
        )
        .await?;

    print_batches(&df.collect().await?)?;

    println!("\n✅ Example completed successfully!");

    Ok(())
}

fn print_batches(batches: &[RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    datafusion::arrow::util::pretty::print_batches(batches)?;
    Ok(())
}
