// Window Functions Example
//
// This example demonstrates using window functions with ClickHouse and DataFusion.
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
// cargo run --example 04_window_functions --features test-utils
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
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Window Functions Example\n");

    let ctx = SessionContext::new();

    let clickhouse = ClickHouseBuilder::new("localhost:9000")
        .configure_client(|c| {
            c.with_username("default")
                .with_password("password")
        })
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("employee_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("employees", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["employee_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created database and table");

    ctx.sql(
        "INSERT INTO clickhouse.example_db.employees (employee_id, name, department, salary) VALUES \
         (1, 'Alice', 'Engineering', 75000.0), \
         (2, 'Bob', 'Engineering', 65000.0), \
         (3, 'Carol', 'Sales', 80000.0), \
         (4, 'Dave', 'Sales', 70000.0), \
         (5, 'Eve', 'Engineering', 95000.0), \
         (6, 'Frank', 'Sales', 85000.0), \
         (7, 'Grace', 'Marketing', 72000.0), \
         (8, 'Henry', 'Marketing', 68000.0)"
    )
    .await?
    .collect()
    .await?;

    println!("✓ Inserted sample data\n");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Example 1: ROW_NUMBER - Rank employees by salary within each department
    println!("Example 1: Rank employees by salary within their department\n");
    let df = ctx
        .sql(
            "SELECT name, department, salary, \
                    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank \
             FROM clickhouse.example_db.employees \
             ORDER BY department, rank"
        )
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 2: AVG - Calculate department average alongside individual salaries
    println!("\nExample 2: Compare individual salary to department average\n");
    let df = ctx
        .sql(
            "SELECT name, department, salary, \
                    AVG(salary) OVER (PARTITION BY department) as dept_avg_salary, \
                    salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg \
             FROM clickhouse.example_db.employees \
             ORDER BY department, salary DESC"
        )
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 3: Running total
    println!("\nExample 3: Running total of salaries within each department\n");
    let df = ctx
        .sql(
            "SELECT name, department, salary, \
                    SUM(salary) OVER (PARTITION BY department ORDER BY salary) as running_total \
             FROM clickhouse.example_db.employees \
             ORDER BY department, salary"
        )
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 4: Top earners per department
    println!("\nExample 4: Top 2 earners in each department (using CTE)\n");
    let df = ctx
        .sql(
            "WITH ranked_employees AS ( \
                 SELECT name, department, salary, \
                        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank \
                 FROM clickhouse.example_db.employees \
             ) \
             SELECT name, department, salary, rank \
             FROM ranked_employees \
             WHERE rank <= 2 \
             ORDER BY department, rank"
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
