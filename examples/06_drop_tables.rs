#![allow(unused_crate_dependencies)]
#![allow(clippy::doc_markdown)]
//! Example demonstrating DROP TABLE operations with ClickHouse
//!
//! **KNOWN LIMITATION**: DROP TABLE via ctx.sql() does NOT work with ClickHouse tables.
//! DataFusion does not forward DROP TABLE commands to the remote ClickHouse server.
//!
//! This example demonstrates:
//! - Creating tables using the catalog builder
//! - Attempting to drop tables via SQL (shows the limitation)
//! - Workarounds for table cleanup
//!
//! For production use, drop tables using:
//! 1. Direct ClickHouse client/driver
//! 2. ClickHouse HTTP interface
//! 3. External management tools
//!
//! # Start ClickHouse (run in terminal):
//! ```bash
//! docker run -d --name clickhouse-example \
//!   -p 9001:9000 -p 8124:8123 \
//!   -e CLICKHOUSE_USER=default \
//!   -e CLICKHOUSE_PASSWORD=password \
//!   clickhouse/clickhouse-server:latest
//! ```
//!
//! # Run this example:
//! ```bash
//! cargo run --example 06_drop_tables --features test-utils
//! ```
//!
//! # Stop container when done:
//! ```bash
//! docker stop clickhouse-example && docker rm clickhouse-example
//! ```

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::prelude::*;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗑️  ClickHouse DROP TABLE Example\n");

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Connect to ClickHouse (using port 9001 to avoid conflicts)
    let clickhouse = ClickHouseBuilder::new("localhost:9001")
        .configure_client(|c| c.with_username("default").with_password("password"))
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    let db = "drop_example";

    // ============================================================
    // Example 1: Create a table and drop it
    // ============================================================
    println!("1️⃣  Creating and dropping a simple table...\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema(db)
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, schema.clone())
        .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    println!("   ✅ Created table 'users'");

    // Insert some data using SQL
    let _ = ctx
        .sql(&format!(
            "INSERT INTO clickhouse.{}.users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, \
             'Charlie')",
            db
        ))
        .await?
        .collect()
        .await?;
    println!("   ✅ Inserted 3 rows");

    // Verify table exists
    let count = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM clickhouse.{}.users", db))
        .await?
        .collect()
        .await?;
    println!("   ✅ Table has {} rows", count[0].column(0).as_primitive::<Int64Type>().value(0));

    // Attempt to drop the table using SQL
    println!("\n   🗑️  Attempting to drop table 'users' via SQL...");
    let drop_result = ctx.sql(&format!("DROP TABLE clickhouse.{}.users", db)).await;
    match drop_result {
        Ok(df) => match df.collect().await {
            Ok(_) => println!("   ✅ Table dropped successfully"),
            Err(e) => {
                println!("   ❌ DROP TABLE failed: {}", e);
                println!(
                    "   ⚠️  KNOWN LIMITATION: DataFusion doesn't forward DROP TABLE to ClickHouse"
                );
                println!("   💡 Use direct ClickHouse client or HTTP API for DROP operations");
            }
        },
        Err(e) => {
            println!("   ❌ SQL parsing/execution failed: {}", e);
            println!("   ⚠️  DROP TABLE commands are not properly supported");
        }
    }

    // ============================================================
    // Example 2: DROP TABLE IF EXISTS (also doesn't work)
    // ============================================================
    println!("\n2️⃣  Testing DROP TABLE IF EXISTS...\n");

    println!("   Attempting DROP IF EXISTS on non-existent table...");
    let drop_if_result =
        ctx.sql(&format!("DROP TABLE IF EXISTS clickhouse.{}.phantom_table", db)).await;
    match drop_if_result {
        Ok(df) => match df.collect().await {
            Ok(_) => println!("   ✅ IF EXISTS prevented error"),
            Err(e) => println!("   ❌ Even IF EXISTS fails: {}", e),
        },
        Err(e) => println!("   ❌ SQL failed: {}", e),
    }

    // ============================================================
    // Example 3: Summary of DROP TABLE limitation
    // ============================================================
    println!("\n3️⃣  Summary and Recommendations...\n");

    println!("   📋 What we learned:");
    println!("      • DROP TABLE commands don't reach ClickHouse");
    println!("      • Tables created via catalog builder remain in ClickHouse");
    println!("      • DataFusion doesn't forward DDL DROP commands");
    println!();
    println!("   🔧 Production workarounds:");
    println!("      • Use clickhouse-arrow conn.execute() for DDL");
    println!("      • Use ClickHouse HTTP API");
    println!("      • Use native ClickHouse clients");
    println!();
    println!("   ✅ Use clickhouse-datafusion for:");
    println!("      • Federated analytics across data sources");
    println!("      • Complex SELECT queries");
    println!("      • INSERT operations");
    println!("      • CREATE TABLE (catalog builder)");

    println!("\n⚠️  Example demonstrates current library limitation\n");
    println!("💡 Key findings:");
    println!("   • DROP TABLE via ctx.sql() does NOT forward to ClickHouse");
    println!("   • This is a known limitation of the current implementation");
    println!("   • Tables remain in ClickHouse even after SQL DROP commands");
    println!();
    println!("🔧 Workarounds for production:");
    println!("   1. Use clickhouse-arrow directly:");
    println!("      conn.execute(\"DROP TABLE table_name\").await");
    println!("   2. Use ClickHouse HTTP API:");
    println!("      curl 'http://localhost:8123' -d 'DROP TABLE table_name'");
    println!("   3. Use official ClickHouse clients (clickhouse-rs, etc.)");
    println!();
    println!("📋 Supported operations in clickhouse-datafusion:");
    println!("   ✅ CREATE TABLE (via catalog builder)");
    println!("   ✅ INSERT INTO");
    println!("   ✅ SELECT queries");
    println!("   ✅ Federated queries");
    println!("   ❌ DROP TABLE (use external tools)");

    Ok(())
}
