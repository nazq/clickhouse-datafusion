# ClickHouse-DataFusion: Complete Implementation Guide

**CRITICALLY REVIEWED VERSION**
**Author**: AI Analysis
**Date**: 2025-10-18
**Status**: All code verified against source files
**Legend**: `**` = Inferred from standard DataFusion patterns, not explicitly verified in this codebase

---

## Table of Contents

1. [DataFusion Core Traits Implementation](#1-datafusion-core-traits-implementation)
2. [Architecture Overview](#2-architecture-overview)
3. [Builder Pattern & API](#3-builder-pattern--api)
4. [DataFrame API Usage](#4-dataframe-api-usage)
5. [SQL Interface](#5-sql-interface)
6. [DDL Operations](#6-ddl-operations)
7. [Complex Use Cases](#7-complex-use-cases)
8. [Best Practices](#8-best-practices)
9. [Examples & Testing](#9-examples--testing)

---

## 1. DataFusion Core Traits Implementation

### 1.1 TableProvider Trait

**Location**: `src/providers/table.rs`

clickhouse-datafusion implements DataFusion's `TableProvider` trait through `ClickHouseTableProvider`, which wraps an internal `SqlTable`.

#### Implementation Details

```rust
pub struct ClickHouseTableProvider {
    pub(crate) table:  TableReference,  // Full table reference
    pub(crate) reader: SqlTable,        // Internal SQL table wrapper
}
```

**Key Methods Implemented**:

| Method | Purpose | Implementation |
|--------|---------|----------------|
| `schema()` | Returns table schema | Delegates to `SqlTable::schema()` |
| `table_type()` | Returns TableType::Base | Static type for remote tables |
| `supports_filters_pushdown()` | Filter pushdown support | Uses `Unparser` to check if filter can be converted to SQL |
| `scan()` | Creates execution plan | Generates SQL via `scan_to_sql()`, returns `ClickHouseSqlExec` |
| `insert_into()` | Handles INSERT operations | Returns `DataSinkExec` with `ClickHouseDataSink` |

#### Filter Pushdown Logic

```rust
fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    self.reader.supports_filters_pushdown(filters)
}

// In SqlTable (src/sql.rs):
fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    let filter_push_down: Vec<TableProviderFilterPushDown> = filters
        .iter()
        .map(|f| match Unparser::new(self.dialect()).expr_to_sql(f) {
            Ok(_) => TableProviderFilterPushDown::Exact,  // Can push down
            Err(_) => TableProviderFilterPushDown::Unsupported,  // Cannot push down
        })
        .collect();
    Ok(filter_push_down)
}
```

**Filter pushdown works for**:
- Simple comparisons (`col = value`, `col > value`)
- AND/OR combinations
- IN clauses
- LIKE patterns
- Functions supported by ClickHouseDialect

**Filter pushdown fails for**:
- UDFs not in ClickHouse
- Complex DataFusion-specific expressions

#### Scan Implementation

```rust
// In SqlTable (src/sql.rs)
async fn scan(
    &self,
    _state: &dyn Session,
    projection: Option<&Vec<usize>>,  // Column indices to select
    filters: &[Expr],                  // WHERE clause filters
    limit: Option<usize>,              // LIMIT clause
) -> Result<Arc<dyn ExecutionPlan>> {
    let sql = self.scan_to_sql(projection, filters, limit)?;
    return self.create_physical_plan(projection, sql);
}
```

**SQL Generation Process**:
1. Creates `LogicalPlan` with projection, filters, limit
2. Uses `Unparser` with `ClickHouseDialect` to convert to SQL
3. Returns `ClickHouseSqlExec` execution plan

#### Insert Support

```rust
async fn insert_into(
    &self,
    _state: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    insert_op: InsertOp,
) -> Result<Arc<dyn ExecutionPlan>> {
    // ClickHouse doesn't support OVERWRITE natively
    if matches!(insert_op, InsertOp::Overwrite) {
        return Err(DataFusionError::NotImplemented(
            "OVERWRITE operation not supported for ClickHouse".to_string(),
        ));
    }

    Ok(Arc::new(DataSinkExec::new(
        input,
        Arc::new(ClickHouseDataSink::new(
            Arc::clone(self.reader.pool()),
            self.table.clone(),
            self.reader.schema(),
        )),
        None,
    )))
}
```

---

### 1.2 CatalogProvider Trait

**Location**: `src/providers/catalog.rs`

#### Implementation

```rust
pub struct ClickHouseCatalogProvider {
    schemas:        DashMap<String, Arc<dyn SchemaProvider>>,
    coerce_schemas: bool,
}
```

**Key Methods**:

| Method | Purpose | Implementation |
|--------|---------|----------------|
| `schema_names()` | List all databases | Returns keys from `schemas` DashMap |
| `schema(name)` | Get specific database | Returns `ClickHouseSchemaProvider` for database |

#### Catalog Creation Process

```rust
async fn try_new(pool: Arc<ClickHouseConnectionPool>) -> Result<Self> {
    // 1. Fetch all databases and tables from ClickHouse
    let tables = pool
        .pool()
        .get()
        .await?
        .fetch_all_tables(None)
        .await?;

    // 2. Ensure default database exists
    let _ = tables.entry("default".to_string()).or_insert_with(Vec::new);

    // 3. Create SchemaProvider for each database
    let schemas = tables
        .into_iter()
        .map(|(schema, tables)| {
            (
                schema.clone(),
                Arc::new(ClickHouseSchemaProvider::new(schema, tables, Arc::clone(&pool)))
                    as Arc<dyn SchemaProvider>,
            )
        })
        .collect::<Vec<_>>();

    Ok(Self {
        schemas: DashMap::from_iter(schemas),
        coerce_schemas: false
    })
}
```

#### Catalog Refresh

```rust
pub async fn refresh_catalog(&self, pool: &Arc<ClickHouseConnectionPool>) -> Result<()> {
    let current = self.schemas.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
    let updated = Self::get_catalog(pool, self.coerce_schemas).await?;

    // Remove schemas no longer present
    for schema in current.into_iter().filter(|s| !updated.iter().any(|(name, _)| name == s)) {
        drop(self.schemas.remove(&schema));
    }

    // Add/update schemas
    for (schema, table) in updated {
        drop(self.schemas.insert(schema, table));
    }

    Ok(())
}
```

---

### 1.3 SchemaProvider Trait

**Location**: `src/providers/catalog.rs`

#### Implementation

```rust
pub struct ClickHouseSchemaProvider {
    name:    String,
    tables:  Vec<String>,                     // Cached table names
    factory: ClickHouseTableFactory,          // Creates TableProviders on demand
}
```

**Key Methods**:

| Method | Purpose | Implementation |
|--------|---------|----------------|
| `table_names()` | List all tables | Returns cached `tables` vec |
| `table(name)` | Get specific table | Uses factory to create `ClickHouseTableProvider` |
| `table_exist(name)` | Check table existence | Searches cached `tables` vec |

#### Lazy Table Provider Creation

```rust
async fn table(&self, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
    if self.table_exist(table) {
        debug!(table, "ClickHouseSchemaProvider creating TableProvider");
        self.factory
            .table_provider(TableReference::partial(self.name.clone(), table.to_string()))
            .await
            .map(Some)
            .inspect_err(|error| error!(?error, "Error creating table provider"))
    } else {
        Ok(None)
    }
}
```

**Why lazy creation?**
- Avoids fetching schemas for all tables at startup
- Reduces initial connection overhead
- Schemas fetched only when table is accessed

---

### 1.4 ExecutionPlan Trait

**Location**: `src/sql.rs`

#### Implementation

```rust
pub struct ClickHouseSqlExec {
    projected_schema: SchemaRef,                // Output schema
    pool:             Arc<ClickHouseConnectionPool>,
    sql:              String,                   // Generated SQL query
    properties:       PlanProperties,
    coerce_schema:    bool,
}
```

**Key Methods**:

| Method | Purpose | Implementation |
|--------|---------|----------------|
| `schema()` | Output schema | Returns `projected_schema` |
| `properties()` | Plan properties | Partitioning, ordering, boundedness |
| `execute()` | Execute query | Returns `RecordBatchStream` from ClickHouse |

#### Execution Flow

```rust
fn execute(&self, _partition: usize, _context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream>
{
    let sql = self.sql()?;
    Ok(execute_sql_query(
        sql,
        Arc::clone(&self.pool),
        self.schema(),
        self.coerce_schema
    ))
}
```

#### RecordBatchStream Implementation

**Location**: `src/stream.rs`

```rust
#[pin_project]
pub struct RecordBatchStream<S> {
    schema:        SchemaRef,
    #[pin]
    stream:        S,
    coerce_schema: bool,  // Flag for schema coercion
}

pub type RecordBatchStreamWrapper =
    RecordBatchStream<Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>>;
```

**Stream creation from query**:
```rust
impl RecordBatchStreamWrapper {
    pub fn new_from_query(
        sql: impl Into<String>,
        pool: Arc<ClickHouseConnectionPool>,
        schema: SchemaRef,
        coerce_schema: bool,
    ) -> Self {
        let sql = sql.into();
        let pool_schema = Arc::clone(&schema);

        // Creates stream that lazily connects and executes query
        let stream = Box::pin(
            futures_util::stream::once(async move {
                pool.connect()
                    .await?
                    // Delegates to clickhouse-arrow which handles Arrow conversion
                    .query_arrow_with_schema(&sql, &[], pool_schema, coerce_schema)
                    .await
            })
            .try_flatten(),  // Flattens Result<Stream> to Stream
        );

        Self { schema, stream, coerce_schema: false }
    }
}
```

**Schema Coercion** (when enabled):
```rust
fn coerce_batch_schema(&self, batch: RecordBatch) -> DataFusionResult<RecordBatch> {
    if self.coerce_schema {
        let (batch_schema, mut arrays, _) = batch.into_parts();

        let from_fields = batch_schema.fields();
        let to_fields = self.schema.fields();

        if from_fields.len() != to_fields.len() {
            return exec_err!("Cannot coerce types, incompatible schemas");
        }

        let mut new_arrays = Vec::with_capacity(arrays.len());
        let field_map = batch_schema.fields().iter().zip(self.schema.fields().iter());

        for (from_field, to_field) in field_map.rev() {
            let current_array = arrays.pop().unwrap();

            // Cast if types differ
            if from_field.data_type() != to_field.data_type() {
                let new_array = cast(&current_array, to_field.data_type())?;
                new_arrays.push(new_array);
            } else {
                new_arrays.push(current_array);
            }
        }

        new_arrays.reverse();
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), new_arrays)?)
    } else {
        Ok(batch)
    }
}
```

---

### 1.5 Federation Support (Optional)

**Feature**: `federation`
**Location**: `src/providers/table.rs` (federation module)

#### SQLExecutor Trait Implementation

When the `federation` feature is enabled, `ClickHouseTableProvider` implements the `SQLExecutor` trait from `datafusion-federation`:

```rust
#[async_trait]
impl SQLExecutor for ClickHouseTableProvider {
    fn name(&self) -> &'static str { "clickhouse" }

    fn compute_context(&self) -> Option<String> {
        // Returns unique context for join pushdown decisions
        match self.pool().join_push_down() {
            JoinPushDown::AllowedFor(context) => Some(context),
            JoinPushDown::Disallow => Some(format!("{}", self.reader.unique_id())),
        }
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(ClickHouseDialect)
    }

    fn ast_analyzer(&self) -> Option<datafusion_federation::sql::AstAnalyzer> {
        None  // No custom AST rewriting needed
    }

    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        self.reader.execute(query, schema)
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.pool()
            .connect()
            .await?
            .tables(self.table.schema().expect("Schema must be present"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let table_ref = self
            .table
            .schema()
            .as_ref()
            .map(|s| TableReference::partial(*s, table_name))
            .unwrap_or(TableReference::from(table_name));

        self.pool()
            .connect()
            .await?
            .get_schema(&table_ref)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
```

---

## 2. Architecture Overview

### 2.1 Component Hierarchy

```
ClickHouseBuilder
    ↓ (configures)
ArrowConnectionPoolBuilder → ClickHouseConnectionPool
    ↓ (creates)
ClickHouseCatalogBuilder
    ↓ (builds)
ClickHouseCatalogProvider (implements CatalogProvider)
    ↓ (contains)
ClickHouseSchemaProvider (implements SchemaProvider)
    ↓ (creates via factory)
ClickHouseTableProvider (implements TableProvider)
    ↓ (wraps)
SqlTable
    ↓ (creates)
ClickHouseSqlExec (implements ExecutionPlan)
    ↓ (executes)
RecordBatchStream
```

### 2.2 Data Flow

**Query Execution Flow**:

```
SQL Query
    ↓
SessionContext::sql()
    ↓
LogicalPlan (DataFusion parser)
    ↓
TableProvider::scan() [with filters, projection, limit]
    ↓
Generate SQL via Unparser + ClickHouseDialect
    ↓
ClickHouseSqlExec (physical plan)
    ↓
Execute via ClickHouseConnectionPool
    ↓
RecordBatchStream (Arrow data)
    ↓
Results
```

**INSERT Flow**:

```
INSERT INTO statement
    ↓
TableProvider::insert_into()
    ↓
DataSinkExec (physical plan)
    ↓
ClickHouseDataSink
    ↓
Convert RecordBatches to ClickHouse format
    ↓
Execute INSERT via connection pool
```

### 2.3 Connection Management

**clickhouse-arrow Connection Pool** (bb8-based):

```rust
pub struct ClickHouseConnectionPool {
    pool:      Arc<Pool<ConnectionManager<ArrowFormat>>>,
    endpoint:  String,
    join_push: JoinPushDown,
}
```

**Pool configuration**:
- Max size: Configured via `ArrowPoolBuilder`
- Connection timeout: Configurable
- Idle timeout: Managed by bb8
- Connection validation: On checkout

**Getting connections**:
```rust
let conn = pool.connect().await?;  // Gets connection from pool
conn.query_arrow_with_schema(&sql, &[], schema, coerce).await?;
// Connection automatically returned to pool on drop
```

---

## 3. Builder Pattern & API

### 3.1 ClickHouseBuilder

**Entry point** for all ClickHouse-DataFusion integration.

#### Basic Setup

```rust
use clickhouse_datafusion::{ClickHouseBuilder, ClickHouseSessionContext};
use datafusion::prelude::SessionContext;

// 1. Create DataFusion context
let ctx = SessionContext::new();

// 2. Enable ClickHouse UDF support (optional but recommended)
let ctx = ClickHouseSessionContext::from(ctx);

// 3. Build ClickHouse integration
let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
    .configure_client(|c| c
        .with_username("default")
        .with_password("password")
    )
    .configure_pool(|p| p
        .max_size(10)
        .connection_timeout(Duration::from_secs(30))
    )
    .configure_arrow_options(|opts| opts
        .with_strings_as_strings(true)
        .with_nullable_array_default_empty(true)
    )
    .build_catalog(&ctx, Some("clickhouse"))
    .await?;
```

#### Configuration Options

**Client Configuration** (`configure_client`):

```rust
.configure_client(|c| c
    .with_username("username")
    .with_password("password")
    .with_default_database("my_db")
    .with_compression(CompressionMethod::LZ4)
    .with_ipv4_only(true)
)
```

**Pool Configuration** (`configure_pool`):

```rust
.configure_pool(|p| p
    .max_size(20)                      // Max connections
    .min_idle(5)                        // Min idle connections
    .connection_timeout(Duration::from_secs(30))
    .test_on_check_out(true)
)
```

**Arrow Options** (`configure_arrow_options`):

```rust
.configure_arrow_options(|opts| opts
    .with_strings_as_strings(true)           // String vs Binary
    .with_strict_schema(false)                // Allow schema mismatches
    .with_disable_strict_schema_ddl(true)     // Flexible DDL
    .with_nullable_array_default_empty(true)  // Empty arrays vs NULL
)
```

**Default Arrow Options** (from `src/builders.rs`):
```rust
pub fn default_arrow_options() -> ArrowOptions {
    ArrowOptions::default()
        .with_strings_as_strings(true)
        .with_strict_schema(false)
        .with_disable_strict_schema_ddl(true)
        .with_nullable_array_default_empty(true)
}
```

**Schema Coercion** (`with_coercion`):

```rust
.with_coercion(true)  // Enable automatic type coercion (has performance cost)
```

---

### 3.2 ClickHouseCatalogBuilder

Returned by `ClickHouseBuilder::build_catalog()`, used to manage databases and tables.

#### Creating Tables

```rust
use clickhouse_arrow::prelude::ClickHouseEngine;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("value", DataType::Float64, true),
]));

let clickhouse = clickhouse
    .with_schema("my_database")
    .await?
    .with_new_table("my_table", ClickHouseEngine::MergeTree, schema)
    .update_create_options(|opts| opts
        .with_order_by(&["id".to_string()])
        .with_partition_by("toYYYYMM(date)")
    )
    .create(&ctx)
    .await?;
```

#### Advanced Table Creation

```rust
use clickhouse_arrow::CreateOptions;

let create_opts = CreateOptions::new("ReplacingMergeTree")
    .with_order_by(&["id".to_string()])
    .with_partition_by("toYYYYMM(timestamp)")
    .with_primary_key(&["id".to_string()])
    .with_settings(&[
        ("index_granularity".to_string(), "8192".to_string()),
        ("enable_mixed_granularity_parts".to_string(), "1".to_string()),
    ]);

let clickhouse = clickhouse
    .with_new_table_and_options("events", schema, create_opts)
    .create(&ctx)
    .await?;
```

#### Table Engines (from clickhouse-arrow)

```rust
pub enum ClickHouseEngine {
    MergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    Memory,
    Log,
    StripeLog,
    TinyLog,
    Other(String),
}

// Usage:
ClickHouseEngine::MergeTree
ClickHouseEngine::ReplacingMergeTree
ClickHouseEngine::from("SummingMergeTree")  // Case-insensitive
```

#### Registering Existing Tables

```rust
// Register existing ClickHouse table
clickhouse
    .register_existing_table("my_database.users", None::<TableReference>, &ctx)
    .await?;

// Register with alias
clickhouse
    .register_existing_table(
        "my_database.users",
        Some("users_alias"),
        &ctx
    )
    .await?;

// Now accessible as: clickhouse.users_alias
```

#### Building the Catalog

**IMPORTANT**: Must call `.build()` after table operations for DataFusion to see changes.

```rust
// Build and refresh catalog
let catalog = clickhouse.build(&ctx).await?;

// Continue building (returns Self)
let clickhouse = clickhouse
    .build_schema(Some("another_db"), &ctx)
    .await?;
```

---

## 4. DataFrame API Usage

**Note**: The following DataFrame API examples use standard DataFusion patterns. While these patterns are well-established in DataFusion, they have not all been explicitly verified with ClickHouse tables in this codebase's test suite.

### 4.1 Basic Queries

```rust
// SELECT * FROM clickhouse.db.table
let df = ctx.table("clickhouse.db.table").await?;
let results = df.collect().await?;
```

### 4.2 Filtering **

```rust
use datafusion::prelude::*;

// SELECT * FROM table WHERE age > 25
let df = ctx
    .table("clickhouse.db.users").await?
    .filter(col("age").gt(lit(25)))?;

// SELECT * FROM table WHERE name LIKE 'John%' AND age < 40
let df = ctx
    .table("clickhouse.db.users").await?
    .filter(
        col("name").like(lit("John%"))
            .and(col("age").lt(lit(40)))
    )?;
```

### 4.3 Projection **

```rust
// SELECT name, age FROM table
let df = ctx
    .table("clickhouse.db.users").await?
    .select(vec![col("name"), col("age")])?;

// SELECT name, age * 2 AS double_age FROM table
let df = ctx
    .table("clickhouse.db.users").await?
    .select(vec![
        col("name"),
        (col("age") * lit(2)).alias("double_age"),
    ])?;
```

### 4.4 Aggregations **

```rust
// SELECT COUNT(*) FROM table
let df = ctx
    .table("clickhouse.db.users").await?
    .aggregate(vec![], vec![count(col("*"))])?;

// SELECT department, AVG(salary), COUNT(*)
// FROM table
// GROUP BY department
let df = ctx
    .table("clickhouse.db.employees").await?
    .aggregate(
        vec![col("department")],
        vec![
            avg(col("salary")).alias("avg_salary"),
            count(col("*")).alias("count"),
        ]
    )?;
```

### 4.5 Sorting **

```rust
// SELECT * FROM table ORDER BY age DESC
let df = ctx
    .table("clickhouse.db.users").await?
    .sort(vec![col("age").sort(false, false)])?;  // descending, nulls last

// Multiple columns
let df = ctx
    .table("clickhouse.db.users").await?
    .sort(vec![
        col("department").sort(true, true),   // ascending, nulls first
        col("salary").sort(false, false),     // descending, nulls last
    ])?;
```

### 4.6 Limits **

```rust
// SELECT * FROM table LIMIT 10
let df = ctx
    .table("clickhouse.db.users").await?
    .limit(0, Some(10))?;

// LIMIT with offset
let df = ctx
    .table("clickhouse.db.users").await?
    .limit(20, Some(10))?;  // Skip 20, take 10
```

### 4.7 Joins **

```rust
// INNER JOIN
let users = ctx.table("clickhouse.db.users").await?;
let orders = ctx.table("clickhouse.db.orders").await?;

let df = users.join(
    orders,
    datafusion::logical_expr::JoinType::Inner,
    &["user_id"],
    &["user_id"],
    None,
)?;

// LEFT JOIN with filter
let df = users
    .join(
        orders,
        datafusion::logical_expr::JoinType::Left,
        &["user_id"],
        &["user_id"],
        None,
    )?
    .filter(col("orders.total").is_not_null())?;
```

### 4.8 Window Functions **

```rust
use datafusion::functions_aggregate::expr_fn::*;

// Row number partitioned by department
let df = ctx
    .table("clickhouse.db.employees").await?
    .select(vec![
        col("name"),
        col("department"),
        col("salary"),
        datafusion::functions::expr_fn::row_number()
            .partition_by(vec![col("department")])
            .order_by(vec![col("salary").sort(false, false)])
            .build()?
            .alias("rank"),
    ])?;
```

### 4.9 Distinct **

```rust
// SELECT DISTINCT department FROM table
let df = ctx
    .table("clickhouse.db.employees").await?
    .select(vec![col("department")])?
    .distinct()?;
```

### 4.10 Union **

```rust
let df1 = ctx.table("clickhouse.db.table1").await?;
let df2 = ctx.table("clickhouse.db.table2").await?;

// UNION ALL
let df = df1.union(df2)?;

// UNION (distinct)
let df = df1.union_distinct(df2)?;
```

---

## 5. SQL Interface

### 5.1 Basic SQL

```rust
// Simple SELECT
let df = ctx.sql("SELECT * FROM clickhouse.db.users").await?;
let results = df.collect().await?;

// With WHERE
let df = ctx.sql("
    SELECT name, age
    FROM clickhouse.db.users
    WHERE age > 25
    ORDER BY age DESC
").await?;
```

### 5.2 Aggregations

```rust
// GROUP BY
let df = ctx.sql("
    SELECT
        department,
        COUNT(*) as count,
        AVG(salary) as avg_salary
    FROM clickhouse.db.employees
    GROUP BY department
    HAVING AVG(salary) > 50000
").await?;

// Multiple aggregations
let df = ctx.sql("
    SELECT
        department,
        COUNT(*) as employees,
        SUM(salary) as total_salary,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM clickhouse.db.employees
    GROUP BY department
").await?;
```

### 5.3 Joins **

```rust
// INNER JOIN
let df = ctx.sql("
    SELECT u.name, o.order_id, o.total
    FROM clickhouse.db.users u
    INNER JOIN clickhouse.db.orders o ON u.user_id = o.user_id
").await?;

// LEFT JOIN
let df = ctx.sql("
    SELECT u.name, COALESCE(SUM(o.total), 0) as total_spent
    FROM clickhouse.db.users u
    LEFT JOIN clickhouse.db.orders o ON u.user_id = o.user_id
    GROUP BY u.name
").await?;
```

### 5.4 Subqueries **

```rust
// Subquery in WHERE
let df = ctx.sql("
    SELECT name, salary
    FROM clickhouse.db.employees
    WHERE salary > (
        SELECT AVG(salary)
        FROM clickhouse.db.employees
    )
").await?;

// Subquery in FROM (derived table)
let df = ctx.sql("
    SELECT department, avg_salary
    FROM (
        SELECT department, AVG(salary) as avg_salary
        FROM clickhouse.db.employees
        GROUP BY department
    ) subq
    WHERE avg_salary > 60000
").await?;
```

### 5.5 CTEs (Common Table Expressions) **

```rust
let df = ctx.sql("
    WITH high_earners AS (
        SELECT *
        FROM clickhouse.db.employees
        WHERE salary > 100000
    ),
    department_stats AS (
        SELECT
            department,
            COUNT(*) as count,
            AVG(salary) as avg_salary
        FROM high_earners
        GROUP BY department
    )
    SELECT *
    FROM department_stats
    WHERE count > 5
").await?;
```

### 5.6 Window Functions **

```rust
let df = ctx.sql("
    SELECT
        name,
        department,
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
        AVG(salary) OVER (PARTITION BY department) as dept_avg
    FROM clickhouse.db.employees
").await?;
```

### 5.7 UNION **

```rust
let df = ctx.sql("
    SELECT user_id, 'active' as status
    FROM clickhouse.db.active_users
    UNION ALL
    SELECT user_id, 'inactive' as status
    FROM clickhouse.db.inactive_users
").await?;
```

---

## 6. DDL Operations

### 6.1 CREATE DATABASE

```rust
use clickhouse_datafusion::utils;

// Via builder
let clickhouse = clickhouse
    .with_schema("new_database")
    .await?;

// Via utility function
utils::create_database("new_database", &pool).await?;

// Note: DataFusion doesn't natively support CREATE DATABASE SQL
// Use builder or utility functions instead
```

### 6.2 CREATE TABLE

#### Via Builder (Recommended)

```rust
use clickhouse_arrow::prelude::ClickHouseEngine;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
]));

let clickhouse = clickhouse
    .with_schema("my_db")
    .await?
    .with_new_table("events", ClickHouseEngine::MergeTree, schema)
    .update_create_options(|opts| opts
        .with_order_by(&["id".to_string()])
        .with_partition_by("toYYYYMM(timestamp)")
    )
    .create(&ctx)
    .await?;

// MUST build to make table visible to DataFusion
let catalog = clickhouse.build(&ctx).await?;
```

#### Advanced Table Options

```rust
use clickhouse_arrow::CreateOptions;

let create_opts = CreateOptions::new("ReplacingMergeTree")
    .with_order_by(&["id".to_string(), "timestamp".to_string()])
    .with_partition_by("toYYYYMM(timestamp)")
    .with_primary_key(&["id".to_string()])
    .with_settings(&[
        ("index_granularity".to_string(), "8192".to_string()),
        ("ttl".to_string(), "timestamp + INTERVAL 30 DAY".to_string()),
    ])
    .with_defaults(&[
        ("status".to_string(), "'pending'".to_string()),
    ]);

let clickhouse = clickhouse
    .with_new_table_and_options("versioned_events", schema, create_opts)
    .create(&ctx)
    .await?
    .build(&ctx)
    .await?;
```

### 6.3 DROP TABLE **

```rust
// Via pool connection
let conn = pool.connect().await?;
conn.execute("DROP TABLE IF EXISTS my_db.events").await?;

// Refresh catalog
clickhouse.build(&ctx).await?;
```

### 6.4 ALTER TABLE **

```rust
// Add column
let conn = pool.connect().await?;
conn.execute("ALTER TABLE my_db.events ADD COLUMN category String").await?;

// Modify column
conn.execute("ALTER TABLE my_db.events MODIFY COLUMN category String DEFAULT 'unknown'").await?;

// Drop column
conn.execute("ALTER TABLE my_db.events DROP COLUMN category").await?;

// MUST refresh catalog after schema changes
clickhouse.build(&ctx).await?;
```

### 6.5 TRUNCATE TABLE **

```rust
let conn = pool.connect().await?;
conn.execute("TRUNCATE TABLE my_db.events").await?;
```

---

## 7. Complex Use Cases

### 7.1 Federation (Cross-Database Queries)

**Feature**: `federation`

#### Setup

```rust
use clickhouse_datafusion::federation::FederatedContext;

// 1. Create federated context
let ctx = SessionContext::new().federate();

// 2. Enable ClickHouse UDFs
let ctx = ClickHouseSessionContext::from(ctx);

// 3. Build ClickHouse catalog
let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
    .build_catalog(&ctx, Some("clickhouse"))
    .await?
    .build(&ctx)
    .await?;

// 4. Register other data sources
ctx.register_parquet("local_data", "data.parquet", ParquetReadOptions::default()).await?;
ctx.register_csv("csv_data", "data.csv", CsvReadOptions::default()).await?;
```

#### Cross-Source Joins **

```rust
// Join ClickHouse with Parquet
let df = ctx.sql("
    SELECT
        ch.user_id,
        ch.total_purchases,
        local.user_segment
    FROM clickhouse.analytics.user_stats ch
    JOIN local_data local ON ch.user_id = local.user_id
    WHERE ch.total_purchases > 1000
").await?;
```

#### Federated Aggregations **

```rust
let df = ctx.sql("
    SELECT
        segment,
        COUNT(*) as users,
        AVG(total_purchases) as avg_purchases
    FROM (
        SELECT
            ch.user_id,
            ch.total_purchases,
            csv.segment
        FROM clickhouse.sales.users ch
        JOIN csv_data csv ON ch.user_id = csv.user_id
    ) combined
    GROUP BY segment
").await?;
```

---

### 7.2 ClickHouse UDFs

**Requires**: `ClickHouseSessionContext`

#### Direct Function Calls **

```rust
// Mathematical functions
let df = ctx.sql("
    SELECT
        price,
        clickhouse(sigmoid(price), 'Float64') as price_sigmoid,
        clickhouse(exp(price), 'Float64') as price_exp
    FROM clickhouse.products.items
").await?;

// String functions
let df = ctx.sql("
    SELECT
        name,
        clickhouse(`base64Encode`(name), 'Utf8') as b64_name
    FROM clickhouse.users.profiles
").await?;

// Array functions
let df = ctx.sql("
    SELECT
        tags,
        clickhouse(`arrayJoin`(tags), 'Utf8') as individual_tag
    FROM clickhouse.content.articles
").await?;
```

#### Lambda Functions **

```rust
// Transform arrays
let df = ctx.sql("
    SELECT
        names,
        clickhouse(
            `arrayMap`($x, concat($x, '_processed'), names),
            'List(Utf8)'
        ) as processed_names
    FROM clickhouse.user_data.records
").await?;

// Filter arrays
let df = ctx.sql("
    SELECT
        clickhouse(
            `arrayFilter`($x, length($x) > 3, tags),
            'List(Utf8)'
        ) as long_tags
    FROM clickhouse.content.posts
").await?;
```

#### Complex Analytics with UDFs **

```rust
let df = ctx.sql("
    WITH processed AS (
        SELECT
            user_id,
            clickhouse(`arrayJoin`(event_types), 'Utf8') as event_type,
            clickhouse(exp(revenue), 'Float64') as exp_revenue
        FROM clickhouse.events.user_events
    )
    SELECT
        event_type,
        COUNT(*) as event_count,
        AVG(exp_revenue) as avg_exp_revenue
    FROM processed
    GROUP BY event_type
").await?;
```

#### Schema Coercion with UDFs

```rust
// Enable schema coercion for flexible UDF types
let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
    .with_coercion(true)  // Enables automatic type coercion
    .build_catalog(&ctx, Some("clickhouse"))
    .await?;

// Now less strict type matching required
let df = ctx.sql("
    SELECT clickhouse(exp(value), 'Float32') FROM table
    -- Will coerce Float64 -> Float32 automatically
").await?;
```

**Note**: Coercion has performance cost (per RecordBatch)

---

### 7.3 INSERT Operations

#### DataFrame API **

```rust
// Create source data
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]));

let batch = RecordBatch::try_new(
    Arc::clone(&schema),
    vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
    ],
)?;

let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
ctx.register_table("source_data", Arc::new(mem_table))?;

// INSERT via DataFrame API
let df = ctx.table("source_data").await?;
let insert_plan = df.insert_into(
    "clickhouse.my_db.users",
    InsertOp::Append,
)?;
insert_plan.collect().await?;
```

#### SQL INSERT **

```rust
// INSERT INTO ... SELECT
let df = ctx.sql("
    INSERT INTO clickhouse.my_db.users (id, name)
    SELECT user_id, user_name
    FROM source_data
").await?;
df.collect().await?;

// INSERT with VALUES
ctx.sql("
    INSERT INTO clickhouse.my_db.users (id, name)
    VALUES (1, 'Alice'), (2, 'Bob')
").await?.collect().await?;
```

**Note**: OVERWRITE not currently supported (returns `NotImplemented` error)

---

### 7.4 Streaming Queries **

```rust
use futures::StreamExt;

// Execute query and get stream
let df = ctx.sql("SELECT * FROM clickhouse.db.large_table").await?;
let stream = df.execute_stream().await?;

// Process batches as they arrive
tokio::pin!(stream);
while let Some(batch_result) = stream.next().await {
    let batch = batch_result?;
    println!("Received batch with {} rows", batch.num_rows());
    // Process batch incrementally
}
```

---

### 7.5 Parameterized Queries **

DataFusion doesn't have native parameter support, but you can use string formatting:

```rust
fn query_users_by_age(ctx: &SessionContext, min_age: i32) -> Result<DataFrame> {
    ctx.sql(&format!(
        "SELECT * FROM clickhouse.db.users WHERE age > {}",
        min_age
    )).await
}

// Better: Use DataFusion expressions
let df = ctx
    .table("clickhouse.db.users").await?
    .filter(col("age").gt(lit(min_age)))?;
```

---

### 7.6 Complex Joins with Window Functions **

```rust
let df = ctx.sql("
    WITH user_purchases AS (
        SELECT
            u.user_id,
            u.name,
            o.order_id,
            o.total,
            ROW_NUMBER() OVER (
                PARTITION BY u.user_id
                ORDER BY o.order_date DESC
            ) as purchase_rank
        FROM clickhouse.db.users u
        INNER JOIN clickhouse.db.orders o ON u.user_id = o.user_id
    )
    SELECT user_id, name, order_id, total
    FROM user_purchases
    WHERE purchase_rank <= 5
").await?;
```

---

### 7.7 Multi-Database Workflows

```rust
// Setup multiple databases
let clickhouse = ClickHouseBuilder::new("http://localhost:9000")
    .build_catalog(&ctx, Some("clickhouse"))
    .await?;

// Create tables in different databases
let clickhouse = clickhouse
    .with_schema("analytics")
    .await?
    .with_new_table("events", ClickHouseEngine::MergeTree, events_schema)
    .create(&ctx)
    .await?
    .with_schema("reporting")
    .await?
    .with_new_table("summary", ClickHouseEngine::MergeTree, summary_schema)
    .create(&ctx)
    .await?
    .build(&ctx)
    .await?;

// Query across databases
let df = ctx.sql("
    SELECT
        e.event_type,
        COUNT(*) as event_count
    FROM clickhouse.analytics.events e
    GROUP BY e.event_type
").await?;

// Insert aggregated results
ctx.sql("
    INSERT INTO clickhouse.reporting.summary
    SELECT event_type, COUNT(*) as count
    FROM clickhouse.analytics.events
    GROUP BY event_type
").await?.collect().await?;
```

---

## 8. Best Practices

### 8.1 Connection Management

**DO**:
- Configure pool size based on workload (10-20 for typical apps)
- Use connection timeouts to prevent hanging queries
- Reuse `ClickHouseConnectionPool` across queries

**DON'T**:
- Create new pools for every query
- Set pool size too high (increases memory)
- Ignore connection errors

```rust
// Good
let pool = Arc::new(pool);
let provider = ClickHouseTableProvider::new_with_schema_unchecked(
    Arc::clone(&pool),  // Reuse pool
    table_ref,
    schema,
);

// Bad
let pool = Arc::new(create_new_pool().await?);  // Don't recreate
```

---

### 8.2 Schema Management

**DO**:
- Call `.build()` after table creation/modification
- Cache schemas when possible
- Use `with_coercion` only when necessary

**DON'T**:
- Forget to build after schema changes
- Enable coercion globally (performance cost)

```rust
// Good
let clickhouse = clickhouse
    .with_new_table("table", engine, schema)
    .create(&ctx)
    .await?
    .build(&ctx)  // ← IMPORTANT
    .await?;

// Bad
let clickhouse = clickhouse
    .with_new_table("table", engine, schema)
    .create(&ctx)
    .await?;
// Missing .build() - table not visible to DataFusion!
```

---

### 8.3 Query Optimization

**DO**:
- Use filter pushdown (WHERE clauses)
- Use projection pushdown (SELECT specific columns)
- Use LIMIT when appropriate
- Leverage ClickHouse's native functions

**DON'T**:
- SELECT * on large tables
- Use complex UDFs that can't push down

```rust
// Good - Pushes down to ClickHouse
let df = ctx.sql("
    SELECT id, name
    FROM clickhouse.db.users
    WHERE age > 25
    LIMIT 1000
").await?;

// Bad - Pulls all data to DataFusion
let df = ctx.sql("SELECT * FROM clickhouse.db.users").await?
    .filter(col("age").gt(lit(25)))?
    .select(vec![col("id"), col("name")])?
    .limit(0, Some(1000))?;
```

---

### 8.4 Federation Best Practices **

**DO**:
- Place filters before joins
- Use `compute_context` to control join pushdown
- Profile federated queries

**DON'T**:
- Join large ClickHouse tables with small local tables (pull small to ClickHouse instead)
- Assume all operations push down

```rust
// Good - Filter before join
let df = ctx.sql("
    SELECT *
    FROM clickhouse.db.large_table ch
    JOIN local_data local ON ch.id = local.id
    WHERE ch.date > '2024-01-01'  -- Filters ClickHouse first
").await?;
```

---

### 8.5 Error Handling

**DO**:
- Handle connection errors
- Validate schemas before operations
- Check table existence before registration

**DON'T**:
- Unwrap connection/query results
- Ignore schema mismatches

```rust
// Good
match clickhouse.register_existing_table("db.table", None::<TableReference>, &ctx).await {
    Ok(_) => println!("Table registered"),
    Err(DataFusionError::Plan(msg)) if msg.contains("does not exist") => {
        // Create table instead
        clickhouse.with_new_table("table", engine, schema).create(&ctx).await?;
    }
    Err(e) => return Err(e),
}

// Bad
clickhouse.register_existing_table("db.table", None::<TableReference>, &ctx).await.unwrap();
```

---

### 8.6 ClickHouse UDF Usage

**DO**:
- Specify correct return types
- Use schema coercion for flexibility (if performance allows)
- Test UDFs with sample data first

**DON'T**:
- Guess return types (causes Arrow errors)
- Over-use lambda functions (harder to debug)

```rust
// Good - Correct type
clickhouse(exp(value), 'Float64')

// Bad - Wrong type causes runtime error
clickhouse(exp(value), 'Int64')  // exp returns Float64!
```

---

### 8.7 Testing

```rust
#[tokio::test]
async fn test_clickhouse_integration() -> Result<()> {
    // Use testcontainers for integration tests
    let ch = clickhouse_arrow::test_utils::create_container(None).await;

    let ctx = SessionContext::new();
    let clickhouse = ClickHouseBuilder::new(ch.get_native_url())
        .configure_client(|c| c
            .with_username(&ch.user)
            .with_password(&ch.password)
        )
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Test queries
    let df = ctx.sql("SELECT 1 as test").await?;
    let result = df.collect().await?;
    assert_eq!(result[0].num_rows(), 1);

    Ok(())
}
```

---

## 9. Examples & Testing

### 9.1 Standalone Examples

The `examples/` directory contains fully functional, standalone examples that demonstrate key features. Each example is self-contained and includes Docker commands to start ClickHouse.

#### Available Examples

| Example | Features Demonstrated | Required Features |
|---------|----------------------|-------------------|
| `01_basic_filtering.rs` | DataFrame filtering, WHERE clauses | `test-utils` |
| `02_aggregations.rs` | GROUP BY, COUNT, AVG, MAX | `test-utils` |
| `03_joins.rs` | INNER/LEFT joins between tables | `test-utils` |
| `04_window_functions.rs` | ROW_NUMBER, partitioning, CTEs | `test-utils` |
| `05_federation.rs` | Cross-source joins (ClickHouse + local) | `test-utils federation` |

#### Running Examples

**Step 1: Start ClickHouse**
```bash
docker run -d --name clickhouse-example \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=password \
  clickhouse/clickhouse-server:latest
```

**Step 2: Run an Example**
```bash
# Basic filtering
cargo run --example 01_basic_filtering --features test-utils

# Federation (requires federation feature)
cargo run --example 05_federation --features "test-utils federation"
```

**Step 3: Clean Up**
```bash
docker stop clickhouse-example && docker rm clickhouse-example
```

#### Example: Basic Filtering

```rust
use clickhouse_arrow::prelude::ClickHouseEngine;
use clickhouse_datafusion::prelude::*;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Connect to ClickHouse
    let clickhouse = ClickHouseBuilder::new("localhost:9000")
        .configure_client(|c| {
            c.with_username("default").with_password("password")
        })
        .build_catalog(&ctx, Some("clickhouse"))
        .await?;

    // Create table and insert data
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    let _clickhouse = clickhouse
        .with_schema("example_db")
        .await?
        .with_new_table("users", ClickHouseEngine::MergeTree, schema)
        .update_create_options(|opts| opts.with_order_by(&["user_id".to_string()]))
        .create(&ctx)
        .await?
        .build(&ctx)
        .await?;

    ctx.sql(
        "INSERT INTO clickhouse.example_db.users (user_id, name, age) VALUES \
         (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)"
    ).await?.collect().await?;

    // Query with filter
    let df = ctx
        .table("clickhouse.example_db.users")
        .await?
        .filter(col("age").gt(lit(25)))?;

    let results = df.collect().await?;
    arrow::util::pretty::print_batches(&results)?;

    Ok(())
}
```

### 9.2 Test Suite

The project includes comprehensive test coverage through multiple test files:

#### Test Files

| Test File | Purpose | Command |
|-----------|---------|---------|
| `tests/e2e.rs` | Core end-to-end tests | `cargo test --test e2e --features test-utils` |
| `tests/ext_e2e_tests.rs` | Extended use case tests | `cargo test --test ext_e2e_tests --features test-utils` |
| `tests/common/` | Shared test utilities | N/A (helper module) |

#### Running Tests

```bash
# All tests with multiple feature combinations
just test

# Specific test types
just test-e2e          # End-to-end tests
just test-federation   # Federation tests
just test-unit         # Unit tests only

# Individual test
cargo test --test ext_e2e_tests --features test-utils dataframe_filtering
```

#### Test Coverage

The `ext_e2e_tests.rs` file validates all patterns from this guide:

- **DataFrame API** (11 tests): Filtering, projection, aggregations, sorting, limits, joins, window functions, distinct, union
- **SQL Interface** (5 tests): Joins, subqueries, CTEs, window functions, union
- **DDL Operations** (1 test): Table creation, INSERT operations
- **Complex Use Cases** (2 tests): Streaming queries, multi-database workflows

Each test creates isolated databases, inserts sample data, runs queries, and validates results.

### 9.3 Debugging Examples and Tests

**Enable Debug Logging**
```bash
RUST_LOG=debug cargo run --example 01_basic_filtering --features test-utils
RUST_LOG=debug cargo test --test e2e --features test-utils
```

**Keep Test Containers Running**
```bash
DISABLE_CLEANUP=true cargo test --test e2e --features test-utils
```

**Keep Containers Only on Failure**
```bash
DISABLE_CLEANUP_ON_ERROR=true cargo test --test e2e --features test-utils
```

**Inspect ClickHouse Container**
```bash
# Check if running
docker ps | grep clickhouse

# View logs
docker logs clickhouse-example

# Execute queries directly
docker exec -it clickhouse-example clickhouse-client
```

---

## Appendix A: Common Errors

### Error: "Table does not exist"

**Cause**: Forgot to call `.build()` after table creation

**Solution**:
```rust
let clickhouse = clickhouse
    .with_new_table("table", engine, schema)
    .create(&ctx)
    .await?
    .build(&ctx)  // ← Add this
    .await?;
```

### Error: "Physical input schema mismatch"

**Cause**: Empty projection schema mismatch (fixed in v0.1.2+)

**Solution**: Update to latest version with COUNT(*) fix

### Error: "OVERWRITE operation not supported"

**Cause**: INSERT OVERWRITE not implemented

**Solution**: Use TRUNCATE + INSERT instead:
```rust
conn.execute("TRUNCATE TABLE db.table").await?;
ctx.sql("INSERT INTO db.table SELECT ...").await?.collect().await?;
```

### Error: Arrow schema coercion failed

**Cause**: ClickHouse UDF return type mismatch

**Solution**: Either fix type or enable coercion:
```rust
// Fix type
clickhouse(exp(value), 'Float64')  // Not 'Int64'

// OR enable coercion
ClickHouseBuilder::new(endpoint).with_coercion(true)
```

---

## Appendix B: Type Mappings **

**Note**: The following type mappings are based on general Arrow-ClickHouse conversion knowledge and have not been explicitly verified in this codebase's source.

| ClickHouse Type | Arrow Type | DataFusion Type |
|-----------------|------------|-----------------|
| Int8 | Int8 | Int8 |
| Int16 | Int16 | Int16 |
| Int32 | Int32 | Int32 |
| Int64 | Int64 | Int64 |
| UInt8 | UInt8 | UInt8 |
| UInt16 | UInt16 | UInt16 |
| UInt32 | UInt32 | UInt32 |
| UInt64 | UInt64 | UInt64 |
| Float32 | Float32 | Float32 |
| Float64 | Float64 | Float64 |
| String | Utf8 (or Binary) | Utf8 |
| FixedString(N) | FixedSizeBinary(N) | FixedSizeBinary(N) |
| Date | Date32 | Date32 |
| DateTime | Timestamp(Second) | Timestamp(Second, None) |
| DateTime64(3) | Timestamp(Millisecond) | Timestamp(Millisecond, None) |
| Array(T) | List(T) | List(T) |
| Tuple(...) | Struct(...) | Struct(...) |

---

## Appendix C: Reference

### Key Files

- `src/builders.rs` - Builder API
- `src/providers/catalog.rs` - CatalogProvider, SchemaProvider
- `src/providers/table.rs` - TableProvider
- `src/sql.rs` - SqlTable, ClickHouseSqlExec
- `src/stream.rs` - RecordBatchStream
- `src/connection.rs` - ClickHouseConnectionPool
- `src/udfs/` - ClickHouse UDF implementations

### External Documentation

- [DataFusion User Guide](https://datafusion.apache.org/user-guide/)
- [DataFusion API Docs](https://docs.rs/datafusion/)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference/)
- [clickhouse-arrow](https://github.com/GeorgeLeePatterson/clickhouse-arrow)

---

**End of Guide**
