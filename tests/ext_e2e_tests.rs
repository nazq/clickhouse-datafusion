#![allow(unused_crate_dependencies)]
#![cfg(not(feature = "mocks"))]

//! Extended E2E Tests - Validates all usage examples from the Complete Guide
//!
//! This test suite validates the examples marked with ** in the guide,
//! proving they actually work with real ClickHouse containers.

mod common;

use std::sync::Arc;

use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_datafusion::ClickHouseBuilder;
#[cfg(feature = "federation")]
use clickhouse_datafusion::federation::FederatedContext;
use clickhouse_datafusion::prelude::datafusion::arrow::array::{Int64Array, StringArray};
use clickhouse_datafusion::prelude::datafusion::arrow::datatypes::{DataType, Field, Schema};
use clickhouse_datafusion::prelude::datafusion::arrow::record_batch::RecordBatch;
use clickhouse_datafusion::prelude::datafusion::catalog::MemTable;
use clickhouse_datafusion::prelude::datafusion::common::arrow;
use clickhouse_datafusion::prelude::datafusion::error::Result;
use clickhouse_datafusion::prelude::datafusion::functions_aggregate::expr_fn::*;
use clickhouse_datafusion::prelude::datafusion::functions_window::expr_fn::row_number;
use clickhouse_datafusion::prelude::datafusion::logical_expr::JoinType;
use clickhouse_datafusion::prelude::datafusion::prelude::*;
use futures_util::StreamExt;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    ("hyper", "error"),
    ("clickhouse_arrow", "error"),
    ("datafusion", "info"),
];

// DataFrame API Tests
#[cfg(feature = "test-utils")]
e2e_test!(dataframe_filtering, tests::test_dataframe_filtering, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_projection, tests::test_dataframe_projection, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_aggregations, tests::test_dataframe_aggregations, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_sorting, tests::test_dataframe_sorting, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_limits, tests::test_dataframe_limits, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_joins, tests::test_dataframe_joins, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(
    dataframe_window_functions,
    tests::test_dataframe_window_functions,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_distinct, tests::test_dataframe_distinct, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(dataframe_union, tests::test_dataframe_union, TRACING_DIRECTIVES, None);

// SQL Interface Tests
#[cfg(feature = "test-utils")]
e2e_test!(sql_joins, tests::test_sql_joins, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(sql_subqueries, tests::test_sql_subqueries, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(sql_ctes, tests::test_sql_ctes, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(sql_window_functions, tests::test_sql_window_functions, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(sql_union, tests::test_sql_union, TRACING_DIRECTIVES, None);

// DDL Operations Tests
#[cfg(feature = "test-utils")]
e2e_test!(ddl_operations, tests::test_ddl_operations, TRACING_DIRECTIVES, None);

// Complex Use Cases
#[cfg(feature = "test-utils")]
e2e_test!(insert_operations, tests::test_insert_operations, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(streaming_queries, tests::test_streaming_queries, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(complex_joins_window, tests::test_complex_joins_window, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(multi_database_workflows, tests::test_multi_database_workflows, TRACING_DIRECTIVES, None);

// Federation Tests
#[cfg(all(feature = "test-utils", feature = "federation"))]
e2e_test!(
    federation_cross_source_joins,
    tests::test_federation_cross_source,
    TRACING_DIRECTIVES,
    None
);

#[cfg(feature = "test-utils")]
mod tests {
    use super::*;

    /// Helper to setup test database with sample data
    async fn setup_test_db(ch: &ClickHouseContainer, db: &str, ctx: &SessionContext) -> Result<()> {
        let builder = common::helpers::create_builder(ctx, ch).await?;

        // Create users table
        let users_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("department", DataType::Utf8, true),
            Field::new("salary", DataType::Float64, true),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("users", ClickHouseEngine::MergeTree, users_schema)
            .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
            .create(ctx)
            .await?;

        // Create orders table
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("total", DataType::Float64, false),
            Field::new("order_date", DataType::Utf8, false),
        ]));

        let _clickhouse = clickhouse
            .with_new_table("orders", ClickHouseEngine::MergeTree, orders_schema)
            .update_create_options(|opts| opts.with_order_by(&["order_id".to_string()]))
            .create(ctx)
            .await?
            .build(ctx)
            .await?;

        // Insert test data using SQL
        ctx.sql(&format!(
            "INSERT INTO clickhouse.{db}.users (user_id, name, age, department, salary) VALUES \
             (1, 'Alice', 30, 'Engineering', 75000.0), (2, 'Bob', 25, 'Engineering', 65000.0), \
             (3, 'Carol', 35, 'Sales', 80000.0), (4, 'Dave', 28, 'Sales', 70000.0), (5, 'Eve', \
             40, 'Engineering', 95000.0)"
        ))
        .await?
        .collect()
        .await?;

        ctx.sql(&format!(
            "INSERT INTO clickhouse.{db}.orders (order_id, user_id, total, order_date) VALUES (1, \
             1, 100.0, '2024-01-01'), (2, 1, 200.0, '2024-01-15'), (3, 2, 150.0, '2024-01-10'), \
             (4, 3, 300.0, '2024-01-20'), (5, 3, 250.0, '2024-01-25')"
        ))
        .await?
        .collect()
        .await?;

        // Wait for ClickHouse async replication
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        Ok(())
    }

    // ==================== DataFrame API Tests ====================

    pub(super) async fn test_dataframe_filtering(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_filtering";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: WHERE age > 25
        let df =
            ctx.table(&format!("clickhouse.{db}.users")).await?.filter(col("age").gt(lit(25)))?;

        let results = df.collect().await?;
        assert!(!results.is_empty());
        assert!(results[0].num_rows() >= 3); // Alice, Carol, Eve

        // Test: Complex filter with AND
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .filter(col("age").gt(lit(25)).and(col("department").eq(lit("Engineering"))))?;

        let results = df.collect().await?;
        assert!(!results.is_empty());
        assert!(results[0].num_rows() >= 2); // Alice, Eve

        eprintln!("✓ DataFrame filtering tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_projection(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_projection";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: SELECT name, age
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .select(vec![col("name"), col("age")])?;

        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 2);

        // Test: Computed column
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .select(vec![col("name"), (col("age") * lit(2)).alias("double_age")])?;

        let results = df.collect().await?;
        assert_eq!(results[0].num_columns(), 2);

        eprintln!("✓ DataFrame projection tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_aggregations(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_aggregations";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: COUNT(*)
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .aggregate(vec![], vec![count(lit(1)).alias("count")])?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Test: GROUP BY with aggregates
        let df = ctx.table(&format!("clickhouse.{db}.users")).await?.aggregate(
            vec![col("department")],
            vec![avg(col("salary")).alias("avg_salary"), count(lit(1)).alias("count")],
        )?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame aggregation tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_sorting(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_sorting";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: ORDER BY age DESC
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .sort(vec![col("age").sort(false, false)])?; // descending

        let results = df.collect().await?;
        assert!(!results.is_empty());

        // Test: Multiple column sort
        let df = ctx.table(&format!("clickhouse.{db}.users")).await?.sort(vec![
            col("department").sort(true, true), // ascending
            col("salary").sort(false, false),   // descending
        ])?;

        let results = df.collect().await?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame sorting tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_limits(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_limits";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: LIMIT 2
        let df = ctx.table(&format!("clickhouse.{db}.users")).await?.limit(0, Some(2))?;

        let results = df.collect().await?;
        assert_eq!(results[0].num_rows(), 2);

        // Test: LIMIT with offset
        let df = ctx.table(&format!("clickhouse.{db}.users")).await?.limit(2, Some(2))?; // Skip 2, take 2

        let results = df.collect().await?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame limit tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_joins(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_joins";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: INNER JOIN
        let users = ctx.table(&format!("clickhouse.{db}.users")).await?;
        let orders = ctx.table(&format!("clickhouse.{db}.orders")).await?;

        let df = users.join(orders, JoinType::Inner, &["user_id"], &["user_id"], None)?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Test: LEFT JOIN
        let users = ctx.table(&format!("clickhouse.{db}.users")).await?;
        let orders = ctx.table(&format!("clickhouse.{db}.orders")).await?;

        let df = users.join(orders, JoinType::Left, &["user_id"], &["user_id"], None)?;

        let results = df.collect().await?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame join tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_window_functions(
        ch: Arc<ClickHouseContainer>,
    ) -> Result<()> {
        let db = "test_df_window";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: ROW_NUMBER with PARTITION BY
        let df = ctx.table(&format!("clickhouse.{db}.users")).await?.select(vec![
            col("name"),
            col("department"),
            col("salary"),
            row_number()
                .partition_by(vec![col("department")])
                .order_by(vec![col("salary").sort(false, false)])
                .build()?
                .alias("rank"),
        ])?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame window function tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_distinct(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_distinct";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: SELECT DISTINCT department
        let df = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .select(vec![col("department")])?
            .distinct()?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());
        assert!(results[0].num_rows() == 2); // Engineering, Sales

        eprintln!("✓ DataFrame distinct tests passed");
        Ok(())
    }

    pub(super) async fn test_dataframe_union(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_df_union";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: UNION ALL
        let df1 = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .filter(col("department").eq(lit("Engineering")))?;

        let df2 = ctx
            .table(&format!("clickhouse.{db}.users"))
            .await?
            .filter(col("department").eq(lit("Sales")))?;

        let df = df1.union(df2)?;
        let results = df.collect().await?;
        assert!(!results.is_empty());

        eprintln!("✓ DataFrame union tests passed");
        Ok(())
    }

    // ==================== SQL Interface Tests ====================

    pub(super) async fn test_sql_joins(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_sql_joins";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: INNER JOIN
        let df = ctx
            .sql(&format!(
                "SELECT u.name, o.order_id, o.total FROM clickhouse.{db}.users u INNER JOIN \
                 clickhouse.{db}.orders o ON u.user_id = o.user_id"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Test: LEFT JOIN with aggregation
        let df = ctx
            .sql(&format!(
                "SELECT u.name, COALESCE(SUM(o.total), 0) as total_spent FROM \
                 clickhouse.{db}.users u LEFT JOIN clickhouse.{db}.orders o ON u.user_id = \
                 o.user_id GROUP BY u.name"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ SQL join tests passed");
        Ok(())
    }

    pub(super) async fn test_sql_subqueries(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_sql_subqueries";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: Subquery in WHERE
        let df = ctx
            .sql(&format!(
                "SELECT name, salary FROM clickhouse.{db}.users WHERE salary > (SELECT \
                 AVG(salary) FROM clickhouse.{db}.users)"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Test: Subquery in FROM
        let df = ctx
            .sql(&format!(
                "SELECT department, avg_salary FROM (SELECT department, AVG(salary) as avg_salary \
                 FROM clickhouse.{db}.users GROUP BY department) subq WHERE avg_salary > 70000"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ SQL subquery tests passed");
        Ok(())
    }

    pub(super) async fn test_sql_ctes(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_sql_ctes";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: CTE
        let df = ctx
            .sql(&format!(
                "WITH high_earners AS ( SELECT * FROM clickhouse.{db}.users WHERE salary > 70000 \
                 ), department_stats AS ( SELECT department, COUNT(*) as count, AVG(salary) as \
                 avg_salary FROM high_earners GROUP BY department ) SELECT * FROM department_stats"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ SQL CTE tests passed");
        Ok(())
    }

    pub(super) async fn test_sql_window_functions(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_sql_window";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: Window functions
        let df = ctx
            .sql(&format!(
                "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department \
                 ORDER BY salary DESC) as rank, AVG(salary) OVER (PARTITION BY department) as \
                 dept_avg FROM clickhouse.{db}.users"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ SQL window function tests passed");
        Ok(())
    }

    pub(super) async fn test_sql_union(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_sql_union";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: UNION ALL
        let df = ctx
            .sql(&format!(
                "SELECT user_id, name FROM clickhouse.{db}.users WHERE department = 'Engineering' \
                 UNION ALL SELECT user_id, name FROM clickhouse.{db}.users WHERE department = \
                 'Sales'"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ SQL union tests passed");
        Ok(())
    }

    // ==================== DDL Operations Tests ====================

    pub(super) async fn test_ddl_operations(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_ddl_ops";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?;

        // Test: CREATE TABLE
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("test_table", ClickHouseEngine::MergeTree, schema)
            .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
            .create(&ctx)
            .await?;

        // Build catalog to register the created table
        let _catalog = clickhouse.build(&ctx).await?;

        // Verify the table was created and can be queried
        let df = ctx.sql(&format!("SELECT * FROM clickhouse.{db}.test_table")).await?;
        let results = df.collect().await?;
        assert!(results.is_empty()); // Table should be empty initially

        // Test: INSERT data
        ctx.sql(&format!(
            "INSERT INTO clickhouse.{db}.test_table (id, name, value) VALUES (1, 'test', 42.0)"
        ))
        .await?
        .collect()
        .await?;

        // Wait for replication
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Verify INSERT worked
        let df = ctx.sql(&format!("SELECT * FROM clickhouse.{db}.test_table WHERE id = 1")).await?;
        let results = df.collect().await?;
        assert!(!results.is_empty());

        // Note: CREATE TABLE, ALTER TABLE, TRUNCATE, and DROP TABLE with ClickHouse-specific
        // syntax (ENGINE clause) are not supported via DataFusion SQL.
        // Use the builder pattern (with_new_table) to create tables as shown above.

        eprintln!("✓ DDL operations tests passed");
        Ok(())
    }

    // ==================== Complex Use Cases ====================

    pub(super) async fn test_insert_operations(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_insert_ops";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?;

        // Create table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let clickhouse = builder
            .with_schema(db)
            .await?
            .with_new_table("users", ClickHouseEngine::MergeTree, schema.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".to_string()]))
            .create(&ctx)
            .await?;

        // Build catalog
        let _catalog = clickhouse.build(&ctx).await?;

        // Create local source data
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ])?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("source_data", Arc::new(mem_table))?;

        // Test: INSERT via SQL SELECT
        ctx.sql(&format!("INSERT INTO clickhouse.{db}.users (id, name) SELECT * FROM source_data"))
            .await?
            .collect()
            .await?;

        // Verify insert
        let df = ctx.sql(&format!("SELECT * FROM clickhouse.{db}.users")).await?;
        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(results[0].num_rows() >= 3);

        // Test: INSERT via VALUES
        ctx.sql(&format!("INSERT INTO clickhouse.{db}.users (id, name) VALUES (4, 'Dave')"))
            .await?
            .collect()
            .await?;

        // Verify
        let df = ctx.sql(&format!("SELECT COUNT(*) as cnt FROM clickhouse.{db}.users")).await?;
        let results = df.collect().await?;
        assert!(!results.is_empty());

        eprintln!("✓ INSERT operations tests passed");
        Ok(())
    }

    pub(super) async fn test_streaming_queries(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_streaming";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: Streaming query
        let df = ctx.sql(&format!("SELECT * FROM clickhouse.{db}.users")).await?;
        let stream = df.execute_stream().await?;

        // Process batches
        tokio::pin!(stream);
        let mut batch_count = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            batch_count += 1;
            eprintln!("Received batch with {} rows", batch.num_rows());
        }

        assert!(batch_count > 0);

        eprintln!("✓ Streaming query tests passed");
        Ok(())
    }

    pub(super) async fn test_complex_joins_window(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_complex_join_window";

        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Test: Complex join with window function
        let df = ctx
            .sql(&format!(
                "WITH user_purchases AS ( SELECT u.user_id, u.name, o.order_id, o.total, \
                 ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY o.order_date DESC) as \
                 purchase_rank FROM clickhouse.{db}.users u INNER JOIN clickhouse.{db}.orders o \
                 ON u.user_id = o.user_id ) SELECT user_id, name, order_id, total FROM \
                 user_purchases WHERE purchase_rank <= 2"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        eprintln!("✓ Complex join with window function tests passed");
        Ok(())
    }

    pub(super) async fn test_multi_database_workflows(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let ctx = SessionContext::new();
        #[cfg(feature = "federation")]
        let ctx = ctx.federate();

        let builder = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?;

        // Create tables in different databases
        let events_schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("event_type", DataType::Utf8, false),
        ]));

        let summary_schema = Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));

        let _clickhouse = builder
            .with_schema("analytics")
            .await?
            .with_new_table("events", ClickHouseEngine::MergeTree, events_schema)
            .update_create_options(|opts| opts.with_order_by(&["event_id".to_string()]))
            .create(&ctx)
            .await?
            .with_schema("reporting")
            .await?
            .with_new_table("summary", ClickHouseEngine::MergeTree, summary_schema)
            .update_create_options(|opts| opts.with_order_by(&["event_type".to_string()]))
            .create(&ctx)
            .await?
            .build(&ctx)
            .await?;

        // Insert test data using SQL
        ctx.sql(
            "INSERT INTO clickhouse.analytics.events (event_id, event_type) VALUES (1, 'click'), \
             (2, 'view')",
        )
        .await?
        .collect()
        .await?;

        // Wait for ClickHouse async replication
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Query across databases
        let df = ctx
            .sql(
                "SELECT event_type, COUNT(*) as count FROM clickhouse.analytics.events GROUP BY \
                 event_type",
            )
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Insert aggregated results
        ctx.sql(
            "INSERT INTO clickhouse.reporting.summary SELECT event_type, COUNT(*) as count FROM \
             clickhouse.analytics.events GROUP BY event_type",
        )
        .await?
        .collect()
        .await?;

        eprintln!("✓ Multi-database workflow tests passed");
        Ok(())
    }

    // ==================== Federation Tests ====================

    #[cfg(feature = "federation")]
    pub(super) async fn test_federation_cross_source(ch: Arc<ClickHouseContainer>) -> Result<()> {
        let db = "test_federation";

        // Create federated context
        let ctx = SessionContext::new().federate();

        setup_test_db(&ch, db, &ctx).await?;

        let _clickhouse = ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| common::helpers::configure_client(c, &ch))
            .build_catalog(&ctx, Some("clickhouse"))
            .await?
            .build(&ctx)
            .await?;

        // Create local CSV data
        let csv_data = "user_id,segment\n1,premium\n2,standard\n3,premium";
        std::fs::write("/tmp/test_segments.csv", csv_data)?;

        ctx.register_csv(
            "local_segments",
            "/tmp/test_segments.csv",
            datafusion::execution::options::CsvReadOptions::default(),
        )
        .await?;

        // Test: Cross-source join
        let df = ctx
            .sql(&format!(
                "SELECT u.name, u.salary, s.segment FROM clickhouse.{db}.users u JOIN \
                 local_segments s ON u.user_id = s.user_id"
            ))
            .await?;

        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        assert!(!results.is_empty());

        // Cleanup
        std::fs::remove_file("/tmp/test_segments.csv")?;

        eprintln!("✓ Federation cross-source tests passed");
        Ok(())
    }
}
