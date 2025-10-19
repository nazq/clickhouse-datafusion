// Aggregations and GROUP BY Example
//
// This example demonstrates grouping and aggregating data with ClickHouse and DataFusion.
//
// # Start ClickHouse (run in terminal):
// ```bash
// docker run -d --name clickhouse-example \
//   -p 9000:9000 -p 8123:8123 \
//   -e CLICKHOUSE_USER=default \
//   -e CLICKHOUSE_PASSWORD=password \
//   clickhouse/clickhouse-server:latest
// ```
//
// # Run this example:
// ```bash
// cargo run --example 02_aggregations --features test-utils
// ```
//
// # Stop container when done:
// ```bash
// docker stop clickhouse-example && docker rm clickhouse-example
// ```

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::expr_fn::{avg, count, max};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Aggregations Example\n");

    let ctx = SessionContext::new();

    let clickhouse = ClickHouseBuilder::new("localhost:9000")
        .configure_client(|c| {
            c.with_username("default")
                .with_password("password")
        })
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    let _clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("employees", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created database and table");

    ctx.sql(
        "INSERT INTO clickhouse.example_db.employees (user_id, name, department, salary) VALUES \
         (1, 'Alice', 'Engineering', 75000.0), \
         (2, 'Bob', 'Engineering', 65000.0), \
         (3, 'Carol', 'Sales', 80000.0), \
         (4, 'Dave', 'Sales', 70000.0), \
         (5, 'Eve', 'Engineering', 95000.0), \
         (6, 'Frank', 'Marketing', 60000.0), \
         (7, 'Grace', 'Marketing', 72000.0)"
    )
    .await?
    .collect()
    .await?;

    println!("✓ Inserted sample data\n");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: COUNT all rows
    println!("Example 1: Total number of employees\n");
    let df = ctx
        .table("clickhouse.example_db.employees")
        .await?
        .aggregate(vec![], vec![count(lit(1)).alias("total_employees")])?;

    print_batches(&df.collect().await?)?;

    // Example 2: GROUP BY with multiple aggregates
    println!("\nExample 2: Department statistics (count, avg salary, max salary)\n");
    let df = ctx
        .table("clickhouse.example_db.employees")
        .await?
        .aggregate(
            vec![col("department")],
            vec![
                count(lit(1)).alias("employee_count"),
                avg(col("salary")).alias("avg_salary"),
                max(col("salary")).alias("max_salary"),
            ],
        )?;

    print_batches(&df.collect().await?)?;

    // Example 3: Using SQL for aggregation
    println!("\nExample 3: Find departments with average salary > 70000 (using SQL)\n");
    let df = ctx
        .sql(
            "SELECT department, AVG(salary) as avg_salary, COUNT(*) as count \
             FROM clickhouse.example_db.employees \
             GROUP BY department \
             HAVING AVG(salary) > 70000 \
             ORDER BY avg_salary DESC"
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
