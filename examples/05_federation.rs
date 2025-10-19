#![allow(unused_crate_dependencies)]
// Federation Example - Joining ClickHouse with Local Data
//
// This example demonstrates federating ClickHouse tables with local in-memory tables.
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
// cargo run --example 05_federation --features "test-utils federation"
// ```
//
// # Stop container when done:
// ```bash
// docker stop clickhouse-example && docker rm clickhouse-example
// ```

#[cfg(feature = "federation")]
use std::sync::Arc;

#[cfg(feature = "federation")]
use clickhouse_arrow::prelude::ClickHouseEngine;
#[cfg(feature = "federation")]
use clickhouse_datafusion::federation::FederatedContext;
#[cfg(feature = "federation")]
use clickhouse_datafusion::prelude::*;
#[cfg(feature = "federation")]
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
#[cfg(feature = "federation")]
use datafusion::arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "federation")]
use datafusion::datasource::MemTable;
#[cfg(feature = "federation")]
use datafusion::prelude::*;

#[cfg(feature = "federation")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ClickHouse-DataFusion: Federation Example\n");

    // Enable federation
    let ctx = SessionContext::new().federate();

    // Connect to ClickHouse
    let clickhouse = ClickHouseBuilder::new("localhost:9001")
        .configure_client(|c| c.with_username("default").with_password("password"))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Create ClickHouse table with sales data
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("revenue", DataType::Float64, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("sales", ClickHouseEngine::MergeTree, sales_schema.clone())
        .update_create_options(|opts| opts.with_order_by(&["product_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("✓ Created ClickHouse table");

    // Insert sales data into ClickHouse
    ctx.sql(
        "INSERT INTO clickhouse.example_db.sales (product_id, quantity, revenue) VALUES (1, 100, \
         5000.0), (2, 150, 7500.0), (3, 80, 4000.0), (4, 200, 10000.0)",
    )
    .await?
    .collect()
    .await?;

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    println!("✓ Inserted data into ClickHouse");

    // Create local in-memory table with product information
    let product_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let product_batch = RecordBatch::try_new(product_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        Arc::new(StringArray::from(vec!["Widget A", "Widget B", "Gadget X", "Gadget Y"])),
        Arc::new(StringArray::from(vec!["Widgets", "Widgets", "Gadgets", "Gadgets"])),
    ])?;

    let mem_table = MemTable::try_new(product_schema, vec![vec![product_batch]])?;
    ctx.register_table("products", Arc::new(mem_table))?;

    println!("✓ Created local in-memory table\n");

    // Example 1: Federated JOIN - ClickHouse sales + local product info
    println!("Example 1: Join ClickHouse sales data with local product catalog\n");
    let df = ctx
        .sql(
            "SELECT p.product_name, p.category, s.quantity, s.revenue FROM \
             clickhouse.example_db.sales s JOIN products p ON s.product_id = p.product_id ORDER \
             BY s.revenue DESC",
        )
        .await?;

    print_batches(&df.collect().await?)?;

    // Example 2: Aggregated federated query
    println!("\nExample 2: Total revenue by category (federated aggregation)\n");
    let df = ctx
        .sql(
            "SELECT p.category, SUM(s.quantity) as total_quantity, SUM(s.revenue) as \
             total_revenue, AVG(s.revenue) as avg_revenue FROM clickhouse.example_db.sales s JOIN \
             products p ON s.product_id = p.product_id GROUP BY p.category ORDER BY total_revenue \
             DESC",
        )
        .await?;

    print_batches(&df.collect().await?)?;

    println!("\n✅ Example completed successfully!");
    println!("\n💡 This example showed how to join ClickHouse tables with local in-memory data.");
    println!(
        "   You can also federate with Parquet files, CSV files, and other DataFusion sources!"
    );

    Ok(())
}

#[cfg(not(feature = "federation"))]
fn main() {
    eprintln!("❌ This example requires the 'federation' feature.");
    eprintln!(
        "   Run with: cargo run --example 05_federation --features \"test-utils federation\""
    );
    std::process::exit(1);
}

#[cfg(feature = "federation")]
fn print_batches(batches: &[RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
    datafusion::arrow::util::pretty::print_batches(batches)?;
    Ok(())
}
