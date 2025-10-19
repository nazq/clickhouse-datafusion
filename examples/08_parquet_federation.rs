#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Federated queries joining Parquet files with ClickHouse tables
//!
//! Demonstrates both SQL and DataFrame API approaches:
//! - Registering Parquet files as DataFusion tables
//! - Creating ClickHouse tables
//! - Joining Parquet + ClickHouse via SQL
//! - Joining Parquet + ClickHouse via DataFrame API
//! - Diverse data types across sources
//!
//! # Start ClickHouse:
//! ```bash
//! docker run -d --name clickhouse-example \
//!   -p 9001:9000 -p 8124:8123 \
//!   -e CLICKHOUSE_USER=default \
//!   -e CLICKHOUSE_PASSWORD=password \
//!   clickhouse/clickhouse-server:latest
//! ```
//!
//! # Run:
//! ```bash
//! cargo run --example 08_parquet_federation --features "test-utils federation"
//! ```

use std::fs;
use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::federation::FederatedContext;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Parquet-ClickHouse Federation Example\n");

    // Create Parquet files
    println!("1️⃣  Creating Parquet files...\n");

    let products_path = "/tmp/products.parquet";
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let products_batch = RecordBatch::try_new(products_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![101, 102, 103])),
        Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Keyboard"])),
        Arc::new(Float64Array::from(vec![1499.99, 19.99, 149.99])),
    ])?;

    let file = fs::File::create(products_path)?;
    let mut writer = ArrowWriter::try_new(file, products_schema, None)?;
    writer.write(&products_batch)?;
    writer.close()?;
    println!("   ✅ Created products.parquet");

    let transactions_path = "/tmp/transactions.parquet";
    let transactions_schema = Arc::new(Schema::new(vec![
        Field::new("transaction_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("product_id", DataType::Int32, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("total", DataType::Float64, false),
    ]));

    let transactions_batch = RecordBatch::try_new(transactions_schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1001, 1002, 1003, 1004])),
        Arc::new(Int32Array::from(vec![1, 2, 1, 3])),
        Arc::new(Int32Array::from(vec![101, 102, 103, 101])),
        Arc::new(Int32Array::from(vec![2, 1, 3, 1])),
        Arc::new(Float64Array::from(vec![2999.98, 19.99, 449.97, 1499.99])),
    ])?;

    let file = fs::File::create(transactions_path)?;
    let mut writer = ArrowWriter::try_new(file, transactions_schema, None)?;
    writer.write(&transactions_batch)?;
    writer.close()?;
    println!("   ✅ Created transactions.parquet");

    // Setup DataFusion with federation
    println!("\n2️⃣  Setting up federation...\n");

    let ctx = SessionContext::new().federate();
    ctx.register_parquet("products", products_path, Default::default()).await?;
    ctx.register_parquet("transactions", transactions_path, Default::default()).await?;
    println!("   ✅ Registered Parquet files");

    // Create ClickHouse table with users
    let clickhouse = ClickHouseBuilder::new("localhost:9001")
        .configure_client(|c| c.with_username("default").with_password("password"))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    let db = "federation_demo";
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema(db)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, users_schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    let _ = ctx
        .sql(&format!(
            "INSERT INTO clickhouse.{}.users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', \
             'bob@example.com'), (3, 'Charlie', 'charlie@example.com')",
            db
        ))
        .await?
        .collect()
        .await?;
    println!("   ✅ Created ClickHouse users table");

    // Run federated queries - SQL approach
    println!("\n3️⃣  Running federated queries (SQL)...\n");

    println!("   SQL Query 1: Transactions with user details (Parquet + ClickHouse)");
    let _ = ctx
        .sql(&format!(
            "SELECT t.transaction_id, u.name, u.email, t.product_id, t.total FROM transactions t \
             JOIN clickhouse.{}.users u ON t.user_id = u.user_id ORDER BY t.transaction_id",
            db
        ))
        .await?
        .show()
        .await?;

    println!("\n   SQL Query 2: Complete order details (3-way join)");
    let _ = ctx
        .sql(&format!(
            "SELECT u.name, p.name as product, t.quantity, p.price, t.total FROM transactions t \
             JOIN clickhouse.{}.users u ON t.user_id = u.user_id JOIN products p ON t.product_id \
             = p.product_id ORDER BY t.transaction_id",
            db
        ))
        .await?
        .show()
        .await?;

    // Run federated queries - DataFrame API approach
    println!("\n4️⃣  Running federated queries (DataFrame API)...\n");

    println!("   DataFrame Query 1: Same join using DataFrame API");
    let transactions_df = ctx.table("transactions").await?;
    let users_df = ctx.table(&format!("clickhouse.{}.users", db)).await?;

    let joined = transactions_df
        .clone()
        .join(users_df, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![
            col("transaction_id"),
            col("name"),
            col("email"),
            col("product_id"),
            col("total"),
        ])?
        .sort(vec![col("transaction_id").sort(true, false)])?;

    joined.show().await?;

    println!("\n   DataFrame Query 2: Aggregation with GROUP BY");
    let aggregated = transactions_df.aggregate(vec![], vec![
        count(col("transaction_id")).alias("total_orders"),
        sum(col("total")).alias("total_revenue"),
        avg(col("total")).alias("avg_order"),
    ])?;

    aggregated.show().await?;

    // Cleanup
    fs::remove_file(products_path)?;
    fs::remove_file(transactions_path)?;
    println!("\n✅ Example completed successfully!");
    println!("\n💡 Key takeaways:");
    println!("   • Both SQL and DataFrame API support federated queries");
    println!("   • SQL: More concise for complex joins and familiar syntax");
    println!("   • DataFrame API: More type-safe and composable for Rust code");
    println!("   • Federation works seamlessly across Parquet and ClickHouse");
    println!("   • Use .federate() on SessionContext to enable federation");

    Ok(())
}
