use std::sync::Arc;

use clickhouse_arrow::test_utils::ClickHouseContainer;

pub mod common;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    // ("clickhouse_arrow", "debug"),
];

macro_rules! test_func {
    ($n:ident, $t:expr) => {
        async fn $n(ch: Arc<ClickHouseContainer>) { $t(ch).await.unwrap(); }
    };
}
test_func!(test_builder, tests::test_clickhouse_builder);
test_func!(test_insert, tests::test_insert_data);

#[cfg(feature = "test-utils")]
e2e_test!(builder, test_builder, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
e2e_test!(insert, test_insert, TRACING_DIRECTIVES, None);

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use clickhouse_arrow::test_utils::ClickHouseContainer;
    use clickhouse_arrow::{CompressionMethod, CreateOptions};
    use clickhouse_datafusion::{
        ClickHouseBuilder, ClickHouseCatalogBuilder, ClickHouseEngine, default_arrow_options,
    };
    use datafusion::arrow;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::kernels::cast;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use tracing::{debug, error, info};

    // Create catalog builder
    async fn create_builder(
        ctx: &SessionContext,
        ch: &ClickHouseContainer,
    ) -> Result<ClickHouseCatalogBuilder> {
        ClickHouseBuilder::new(ch.get_native_url())
            .configure_client(|c| {
                c.with_username(&ch.user)
                    .with_password(&ch.password)
                    .with_ipv4_only(true)
                    .with_compression(CompressionMethod::LZ4)
                    .with_arrow_options(default_arrow_options())
            })
            .build_catalog(ctx, None)
            .await
    }

    async fn setup_test_tables(
        ctx: &SessionContext,
        ch: &ClickHouseContainer,
    ) -> Result<ClickHouseCatalogBuilder> {
        let schema_people = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let schema_knicknames = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("other_name", DataType::Utf8, false),
        ]));

        // Create all tables, registering any additional, then build the final context
        let clickhouse = create_builder(ctx, ch)
            .await?
            // Set schema (database)
            .with_schema("test_db")
            .await?
            // Create People table
            .with_table("people", ClickHouseEngine::MergeTree, schema_people)
            .update_create_options(|opts: CreateOptions| {
                opts.with_order_by(&["id".into(), "name".into()])
                    .with_primary_keys(&["id".into()])
                    .with_defaults(vec![("name".into(), "'Unknown'".into())].into_iter())
            })
            .create(ctx)
            .await?
            // Create Knicknames table
            .with_table_and_options(
                "knicknames",
                schema_knicknames,
                CreateOptions::new(ClickHouseEngine::MergeTree.to_string())
                    .with_order_by(&["id".into(), "other_name".into()])
                    .with_primary_keys(&["id".into()]),
            )
            .create(ctx)
            .await?
            .build_schema(/* Stay with same schema */ None, ctx)
            .await
            .inspect_err(|e| error!("Failed building schema: {e:?}"))?;

        Ok(clickhouse)
    }

    pub(super) async fn test_clickhouse_builder(ch: Arc<ClickHouseContainer>) -> Result<()> {
        // Initialize session context
        let ctx = SessionContext::new();

        let clickhouse = setup_test_tables(&ctx, &ch).await?;

        // Test query
        let df = ctx.sql("SELECT name FROM clickhouse.test_db.people").await?;
        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        debug!("Query completed successfully");

        // -----------------------------
        //
        // Test various sub tasks
        // Test registering non-existent table
        let result =
            clickhouse.register_table("test_db.missing", None::<TableReference>, &ctx).await;
        assert!(result.is_err(), "Expected table not found error");
        info!(">>> Unexpected table passed");

        // Register existing table
        clickhouse.register_table("test_db.people", Some("people_alias"), &ctx).await?;
        info!(">>> Registered existing table into alias `people_alias`");

        eprintln!(">> Test builder completed");

        Ok(())
    }

    pub(super) async fn test_insert_data(ch: Arc<ClickHouseContainer>) -> Result<()> {
        // Initialize session context
        let ctx = SessionContext::new();

        let clickhouse = setup_test_tables(&ctx, &ch).await?;

        // -----------------------------
        // Insert data using SQL
        let insert_df = ctx
            .sql("INSERT INTO clickhouse.test_db.people (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .await?;
        let results = insert_df.collect().await?;
        datafusion::arrow::util::pretty::print_batches(&results)?;

        let rows = results
            .first()
            .map(|b| {
                cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
            })
            .expect("Expected a row count");
        assert_eq!(rows, 2, "Expected 2 rows inserted");
        info!(">>> Inserted data into table `test_db.people`");

        // -----------------------------
        // Add another table
        let schema_people2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new_list("names", Field::new_list_field(DataType::Utf8, false), false),
        ]));

        let clickhouse = clickhouse
            .with_table("people2", ClickHouseEngine::MergeTree, schema_people2.clone())
            .update_create_options(|opts| opts.with_order_by(&["id".into()]))
            .create(&ctx)
            .await?;

        let _provider = clickhouse.build(&ctx).await?;

        // -----------------------------
        // Insert more data using SQL
        let insert_sql = "
                INSERT INTO clickhouse.test_db.people2 (id, names)
                VALUES
                    (1, make_array('Jazz', 'Vienna')),
                    (2, make_array('Kaya', 'Susie', 'Georgie')),
                    (3, make_array('Susana', 'Adrienne', 'Blayke'))
                ";
        debug!(insert_sql, ">>> Running insert sql 2");

        let insert_df = ctx.sql(insert_sql).await?;
        let results = insert_df.collect().await?;
        datafusion::arrow::util::pretty::print_batches(&results)?;

        let rows = results
            .first()
            .map(|b| {
                cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
            })
            .expect("Expected a row count");
        assert_eq!(rows, 3, "Expected 3 rows inserted");
        info!(">>> Inserted data into table `test_db.people2`");

        debug!("Until datafusion patches SETTINGS in Query structs, we wait: 2s");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // -----------------------------
        // Test select query 2
        let select_query = "SELECT id, names FROM clickhouse.test_db.people2";
        let df = ctx.sql(select_query).await?;

        let results = df.collect().await?;

        assert!(results
                .first()
                .map(|r| r.column(0))
                .filter(|c| (0..3).all(|i| c.as_primitive::<Int32Type>().value(i) as usize == i + 1))
                .is_some());

        arrow::util::pretty::print_batches(&results)?;
        debug!(">>> Select query on people2 passed");

        // -----------------------------
        // Test select w/ join
        let join_query = "SELECT * FROM clickhouse.test_db.people p1 JOIN \
                          clickhouse.test_db.people2 p2 ON p1.id = p2.id";
        let df = ctx.sql(join_query).await?;
        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        debug!(">>> Join query on people passed");

        // -----------------------------
        // Test datafusion unnest works when federation is off
        let query = "SELECT id, unnest(names) FROM clickhouse.test_db.people2";

        let df = ctx.sql(query).await?;
        let results = df.collect().await?;
        arrow::util::pretty::print_batches(&results)?;
        debug!(">>> Unnest query passed");

        eprintln!(">> Test insert completed");

        Ok(())
    }

    // async fn test_clickhouse_builders_udfs(ch: Arc<ClickHouseContainer>) -> Result<()> {
    //     // Initialize session context
    //     let ctx = SessionContext::new();

    //     let clickhouse = setup_test_tables(&ctx, &ch).await?;

    //     debug!("Created clickhouse catalog builder");

    //     // -----------------------------
    //     // Insert data using SQL
    //     let insert_sql =
    //         "INSERT INTO clickhouse.test_db.people (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    //     debug!(insert_sql, ">>> Running insert sql");

    //     let insert_df = ctx.sql(insert_sql).await?;
    //     let results = insert_df.collect().await?;
    //     datafusion::arrow::util::pretty::print_batches(&results)?;

    //     let rows = results
    //         .first()
    //         .map(|b| {
    //             cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
    //         })
    //         .expect("Expected a row count");
    //     assert_eq!(rows, 2, "Expected 2 rows inserted");
    //     info!(">>> Inserted data into table `test_db.people`");

    //     // -----------------------------
    //     //
    //     // Build context
    //     //
    //     // For federated, it's imperative to overwrite the context. It's additive.
    //     header("Building context", false);
    //     let _catalog = clickhouse.build(&ctx).await?;

    //     debug!(">>> Context built");

    //     // -----------------------------
    //     //
    //     // Add another table
    //     let schema_people2 = Arc::new(Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new_list("names", Field::new_list_field(DataType::Utf8, false), false),
    //     ]));

    //     let _clickhouse = clickhouse
    //         .table_creator("test_db.people2", ClickHouseEngine::MergeTree,
    // schema_people2.clone())         .await
    //         .expect("table creation")
    //         .update_table_options(|opts| opts.with_order_by(&["id".into()]))
    //         .create(&ctx)
    //         .await
    //         .expect("table creation");

    //     header("Added second table (people2), building catalog", true);

    //     let _provider = clickhouse.build(&ctx).await?;

    //     // -----------------------------
    //     //
    //     // Insert data using SQL
    //     header("Inserting data into people2", true);
    //     let insert_sql = "
    //         INSERT INTO clickhouse.test_db.people2 (id, names)
    //         VALUES
    //             (1, make_array('Jazz', 'Vienna')),
    //             (2, make_array('Kaya', 'Susie', 'Georgie')),
    //             (3, make_array('Susana', 'Adrienne', 'Blayke'))
    //         ";
    //     debug!(insert_sql, ">>> Running insert sql 2");

    //     let insert_df = ctx.sql(insert_sql).await?;
    //     let results = insert_df.collect().await?;
    //     datafusion::arrow::util::pretty::print_batches(&results)?;

    //     let rows = results
    //         .first()
    //         .map(|b| {
    //             cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
    //         })
    //         .expect("Expected a row count");
    //     assert_eq!(rows, 3, "Expected 3 rows inserted");
    //     info!(">>> Inserted data into table `test_db.people2`");

    //     debug!("Until datafusion patches SETTINGS in Query structs, we wait: 2s");
    //     tokio::time::sleep(Duration::from_secs(2)).await;

    //     // -----------------------------
    //     //
    //     // Test select query 2
    //     header("Selecting data from people2", false);
    //     let select_query = "SELECT id, names FROM clickhouse.test_db.people2";
    //     let df = ctx.sql(select_query).await?;

    //     let results = df.collect().await?;

    //     assert!(results
    //         .first()
    //         .map(|r| r.column(0))
    //         .filter(|c| (0..3).all(|i| c.as_primitive::<Int32Type>().value(i) as usize == i + 1))
    //         .is_some());

    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Select query on people2 passed");

    //     // -----------------------------
    //     //
    //     // Test select query again
    //     header("Testing select still works on people", false);
    //     let select_query = "SELECT name FROM clickhouse.test_db.people";
    //     let df = ctx.sql(select_query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Select query on people passed");

    //     header("NON-FEDERATED PATH", true);
    //     // -----------------------------
    //     //
    //     // Test datafusion unnest works when federation is off
    //     let query = "SELECT id, unnest(names) FROM clickhouse.test_db.people2";

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Unnest query passed");

    //     Ok(())
    // }

    // async fn test_clickhouse_builders_errors() -> Result<()> {
    //     // Initialize session context
    //     let ctx = SessionContext::new();

    //     let clickhouse = create_builder(&ctx).await.expect("builder");
    //     debug!("Created clickhouse catalog builder");

    //     // Define schemas
    //     let schema_people = Arc::new(Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new("name", DataType::Utf8, false),
    //     ]));
    //     let schema_nicknames = Arc::new(Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new("other_name", DataType::Utf8, false),
    //     ]));

    //     // -----------------------------
    //     //
    //     // Create tables
    //     header("Creating tables", false);
    //     let clickhouse = clickhouse
    //         .table_creator("test_db.people", ClickHouseEngine::MergeTree, schema_people.clone())
    //         .await
    //         .expect("table creation")
    //         .update_table_options(|opts| {
    //             opts.with_order_by(&["id".into(), "name".into()])
    //                 .with_primary_keys(&["id".into()])
    //                 .with_defaults(vec![("name".into(), "'Unknown'".into())].into_iter())
    //         })
    //         .create(&ctx)
    //         .await
    //         .expect("table creation")
    //         .table_creator("test_db.nicknames", ClickHouseEngine::MergeTree, schema_nicknames)
    //         .await
    //         .expect("table creation")
    //         .update_table_options(|opts| {
    //             opts.with_order_by(&["id".into(), "other_name".into()])
    //                 .with_primary_keys(&["id".into()])
    //         })
    //         .create(&ctx)
    //         .await
    //         .expect("table creation")
    //         .build_schema(&ctx, None)
    //         .await?;

    //     info!(">>> Registered tables with catalog provider");

    //     // -----------------------------
    //     //
    //     // Insert data using SQL
    //     header("Inserting data: people", true);
    //     let insert_sql =
    //         "INSERT INTO clickhouse.test_db.people (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    //     debug!(insert_sql, ">>> Running insert sql");

    //     let insert_df = ctx.sql(insert_sql).await?;
    //     let results = insert_df.collect().await?;
    //     datafusion::arrow::util::pretty::print_batches(&results)?;

    //     let rows = results
    //         .first()
    //         .map(|b| {
    //             cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
    //         })
    //         .expect("Expected a row count");
    //     assert_eq!(rows, 2, "Expected 2 rows inserted");
    //     info!(">>> Inserted data into table `test_db.people`");

    //     // -----------------------------
    //     //
    //     // Build context
    //     //
    //     // For federated, it's imperative to overwrite the context. It's additive.
    //     header("Building context", false);
    //     let _ = clickhouse.build(&ctx).await?;

    //     header("Registering external table (mem_table)", false);

    //     // -----------------------------
    //     //
    //     // Add in-memory table
    //     let mem_schema =
    //         Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
    //     let mem_table =
    //         Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
    //             vec![datafusion::arrow::record_batch::RecordBatch::try_new(mem_schema, vec![
    //                 Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
    //             ])?],
    //         ])?);
    //     let mem_catalog = Arc::new(MemoryCatalogProvider::new());
    //     let mem_schema = Arc::new(MemorySchemaProvider::new());
    //     let _ = mem_schema.register_table("mem_events".into(), mem_table)?;
    //     let _ = mem_catalog.register_schema("internal", mem_schema)?;
    //     let _ = ctx.register_catalog("memory", mem_catalog);

    //     // -----------------------------
    //     //
    //     // Add another table
    //     let schema_people2 = Arc::new(Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new("name", DataType::Utf8, false),
    //         Field::new_list("names", Field::new_list_field(DataType::Utf8, false), false),
    //     ]));

    //     let clickhouse = clickhouse
    //         .table_creator("test_db.people2", ClickHouseEngine::MergeTree,
    // schema_people2.clone())         .await
    //         .expect("table creation")
    //         .update_table_options(|opts| opts.with_order_by(&["id".into()]))
    //         .create(&ctx)
    //         .await
    //         .expect("table creation");

    //     header("Added second table (people2), building catalog", true);

    //     // -----------------------------
    //     //
    //     // Build context
    //     //
    //     // For federated, it's imperative to overwrite the context. It's additive.
    //     header("Building context", false);
    //     let _ = clickhouse.build(&ctx).await?;

    //     // -----------------------------
    //     //
    //     // Insert data using SQL
    //     header("Inserting data into people2", true);
    //     let insert_sql = "
    //         INSERT INTO clickhouse.test_db.people2 (id, name, names)
    //         VALUES
    //             (1, 'Bob', make_array('Jazz', 'Vienna')),
    //             (2, 'Alice', make_array('Kaya', 'Susie', 'Georgie')),
    //             (3, 'Charlie', make_array('Susana', 'Adrienne', 'Blayke'))
    //         ";
    //     debug!(insert_sql, ">>> Running insert sql 2");

    //     let insert_df = ctx.sql(insert_sql).await?;
    //     let results = insert_df.collect().await?;
    //     datafusion::arrow::util::pretty::print_batches(&results)?;

    //     let rows = results
    //         .first()
    //         .map(|b| {
    //             cast(b.column(0), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0)
    //         })
    //         .expect("Expected a row count");
    //     assert_eq!(rows, 3, "Expected 3 rows inserted");
    //     info!(">>> Inserted data into table `test_db.people2`");

    //     debug!("Until datafusion patches SETTINGS in Query structs, we wait: 2s");
    //     tokio::time::sleep(Duration::from_secs(2)).await;

    //     // -----------------------------
    //     //
    //     // Registering Test UDF Optimizer and UDF Pushdown
    //     header("Building context", false);
    //     let ctx = ClickHouseSessionContext::new(ctx, None);

    //     // -----------------------------
    //     //
    //     // Test projection with custom Analyzer
    //     header("Testing projection with custom Analyzer 1", false);

    //     let query = "
    //         SELECT p.name
    //             , m.event_id
    //             , clickhouse(exp(p2.id), 'Float64')
    //             , p2.names
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN (
    //             SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
    //             FROM clickhouse.test_db.people2
    //         ) p2 ON p.id = p2.id
    //         ";
    //     let df =
    //         ctx.sql(query).await.inspect_err(|error| error!("Error exe 1 query: {}", error))?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Projection test custom Analyzer 1 passed");

    //     // -----------------------------
    //     //
    //     // Test projection with custom Analyzer
    //     header("Testing projection with custom Analyzer 2", false);

    //     let query = "
    //         SELECT p.name
    //             , m.event_id
    //             , clickhouse(exp(p2.id), 'Float64')
    //             , clickhouse(concat(p2.name, 'hello'), 'Utf8')
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN clickhouse.test_db.people2 p2 ON p.id = p2.id
    //         ";
    //     let df =
    //         ctx.sql(query).await.inspect_err(|error| error!("Error exe 2 query: {}", error))?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Projection test custom Analyzer 2 passed");

    //     // -----------------------------
    //     //
    //     // Test projection with custom Analyzer
    //     header("Testing projection with custom Analyzer 3", false);

    //     let query = "
    //         SELECT p.name
    //             , m.event_id
    //             , clickhouse(exp(p2.id), 'Float64')
    //             , clickhouse(concat(p2.names, 'hello'), 'Utf8')
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN (
    //             SELECT id
    //                 , clickhouse(`arrayJoin`(names), 'Utf8') as names
    //             FROM clickhouse.test_db.people2
    //         ) p2 ON p.id = p2.id
    //         ";
    //     let df =
    //         ctx.sql(query).await.inspect_err(|error| error!("Error exe 3 query: {}", error))?;

    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Projection test custom Analyzer 3 passed");

    //     // -----------------------------
    //     //
    //     // Test projection with custom Analyzer
    //     header("Testing projection with custom Analyzer 4", false);

    //     let query = "
    //         SELECT p.name
    //             , m.event_id
    //             , clickhouse(exp(p2.id), 'Float64')
    //             , concat(p2.names, 'hello')
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN (
    //             SELECT id
    //                 , clickhouse(`arrayJoin`(names), 'Utf8') as names
    //             FROM clickhouse.test_db.people2
    //         ) p2 ON p.id = p2.id
    //         ";
    //     let df =
    //         ctx.sql(query).await.inspect_err(|error| error!("Error exe 4 query: {}", error))?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Projection test custom Analyzer 4 passed");

    //     Ok(())
    // }
}
