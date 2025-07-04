#[cfg(all(feature = "test-utils", feature = "federation"))]
mod tests {

    // async fn test_clickhouse_builders_simple() -> Result<()> {
    //     // Initialize session context
    //     let ctx = SessionContext::new();

    //     let clickhouse = create_builder(&ctx).await.expect("builder");

    //     // Define schemas
    //     let schema_people = Arc::new(Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new("name", DataType::Utf8, false),
    //     ]));

    //     // Create tables
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
    //         .inspect_err(|error| error!(?error, "Error creating table"))
    //         .expect("table creation");

    //     // -----------------------------
    //     //
    //     // Test various sub tasks
    //     {
    //         // Test schema mismatch
    //         let result = clickhouse
    //             .table_creator(
    //                 "other_db.people",
    //                 ClickHouseEngine::MergeTree,
    //                 schema_people.clone(),
    //             )
    //             .await;
    //         assert!(result.is_err(), "Expected schema mismatch error");
    //         info!(">>> Schema mismatch passed");

    //         // Test registering non-existent table
    //         let result =
    //             clickhouse.register_table(&ctx, "test_db.missing", None::<TableReference>).await;
    //         assert!(result.is_err(), "Expected table not found error");
    //         info!(">>> Unexpected table passed");

    //         // Register existing table
    //         clickhouse.register_table(&ctx, "test_db.people", Some("people_alias")).await?;
    //         info!(">>> Registered existing table into alias `people_alias`");
    //     }

    //     // Build with federation
    //     let ctx = clickhouse.build_federated(&ctx).await?;

    //     // Test query
    //     let df = ctx.sql("SELECT name FROM clickhouse.test_db.people").await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!("Query completed successfully");

    //     Ok(())
    // }

    // async fn test_clickhouse_builders_federation() -> Result<()> {
    //     use clickhouse_datafusion::federation;
    //     use datafusion::arrow::array::Int32Array;
    //     use datafusion::arrow::record_batch::RecordBatch;

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
    //         .expect("table creation");
    //     info!(">>> Registered tables with catalog provider");

    //     // Insert data
    //     // let batch = RecordBatch::try_new(
    //     //     schema_people.clone(),
    //     //     vec![
    //     //         Arc::new(Int32Array::from(vec![1, 2])),
    //     //         Arc::new(StringArray::from(vec!["Alice", "Bob"])),
    //     //     ],
    //     // )?;

    //     // -----------------------------
    //     //
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
    //     info!("Inserted data into table `test_db.people`");

    //     // -----------------------------
    //     //
    //     // Create ch catalog provider and external catalog
    //     //
    //     // NOTE: Notice build_federated is NOT used. That is to show that federation can be done
    //     // manually which occurs after the creation of the in memory table
    //     let clickhouse_catalog = clickhouse.build(&ctx).await?;
    //     let external_catalog = federation::external::FederatedCatalogProvider::new();

    //     // NOTE: The clickhouse catalog can be added here as well. Either way works
    //     // external_catalog.add_catalog(CLICKHOUSE_CATALOG_NAME.into(),
    //     // Arc::new(clickhouse_catalog));

    //     // Add in-memory table
    //     let mem_schema =
    //         Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
    //     let mem_table =
    //         Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
    //             vec![RecordBatch::try_new(mem_schema,
    // vec![Arc::new(Int32Array::from(vec![1]))])?],         ])?);
    //     let _ = external_catalog.register_non_federated_table("mem_events".into(), mem_table)?;

    //     // -----------------------------
    //     //
    //     // Federate session context
    //     let ctx = federation::federate_session_context(Some(&ctx));
    //     debug!(">>> Session context federated");

    //     // -----------------------------
    //     //
    //     // Register federated catalogs
    //     let _ = ctx.register_catalog("memory", Arc::new(external_catalog));
    //     let _ = ctx.register_catalog(DEFAULT_CLICKHOUSE_CATALOG, clickhouse_catalog);
    //     debug!(">>> Federated catalog registered");

    //     // -----------------------------
    //     //
    //     // Test select query
    //     let select_query = "SELECT name FROM clickhouse.test_db.people";
    //     let df = ctx.sql(select_query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!("Select query passed");

    //     // -----------------------------
    //     //
    //     // Test select query on mem table
    //     let df = ctx.sql("SELECT m.event_id FROM memory.internal.mem_events m").await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!("Select query passed (memory)");

    //     // -----------------------------
    //     //
    //     // Test federated query
    //     let join_query = "
    //         SELECT p.name, m.event_id
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         ";

    //     let df = ctx.sql(join_query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!("Federated query passed");

    //     Ok(())
    // }

    // async fn test_clickhouse_builders_udfs() -> Result<()> {
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
    //     let ctx = clickhouse.build_federated(&ctx).await?;

    //     debug!(">>> Context built");
    //     header("FEDERATION PATH", true);
    //     header("Registering external table (mem_table)", false);

    //     // -----------------------------
    //     //
    //     // Create external catalog
    //     let external_catalog =
    //         clickhouse_datafusion::federation::external::FederatedCatalogProvider::new();

    //     // NOTE: The clickhouse catalog can be added here as well. Either way works
    //     // external_catalog.add_catalog(CLICKHOUSE_CATALOG_NAME.into(),
    //     // Arc::new(clickhouse_catalog));

    //     // Add in-memory table
    //     let mem_schema =
    //         Arc::new(Schema::new(vec![Field::new("event_id", DataType::Int32, false)]));
    //     let mem_table =
    //         Arc::new(datafusion::datasource::MemTable::try_new(Arc::clone(&mem_schema), vec![
    //             vec![datafusion::arrow::record_batch::RecordBatch::try_new(mem_schema, vec![
    //                 Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
    //             ])?],
    //         ])?);
    //     let _ = external_catalog.register_non_federated_table("mem_events".into(), mem_table)?;

    //     // -----------------------------
    //     //
    //     // Register federated catalog
    //     let _ = ctx.register_catalog("memory", Arc::new(external_catalog));
    //     debug!(">>> Mem catalog registered");

    //     // -----------------------------
    //     //
    //     // Test federated query
    //     header("Testing federated query", false);
    //     let join_query = "
    //         SELECT p.name, m.event_id
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         ";

    //     let df = ctx.sql(join_query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Federated query passed");

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

    //     let ctx = clickhouse.build_federated(&ctx).await?;

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

    //     header("FEDERATED PATH", true);

    //     // -----------------------------
    //     //
    //     // Test select query on mem table again
    //     let query = "SELECT m.event_id FROM memory.internal.mem_events m";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Select query passed (memory)");
    //     header("Tested mem_table again", true);

    //     // -----------------------------
    //     //
    //     // Test federated query again
    //     let query = "
    //         SELECT p.name, m.event_id, p2.names
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN clickhouse.test_db.people2 p2 ON p.id = p2.id
    //         ";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Federated query passed");
    //     header("Tested federated query again", true);

    //     // -----------------------------
    //     //
    //     // Test arrayJoin
    //     let query = "SELECT id, arrayJoin(names) FROM clickhouse.test_db.people2";
    //     debug!("Running query: {query}");

    //     let dialect = datafusion::sql::sqlparser::dialect::ClickHouseDialect {};
    //     let ast = datafusion::sql::sqlparser::parser::Parser::parse_sql(&dialect, query)?;
    //     tracing::debug!("Parsed AST: {:?}", ast);

    //     let df = ctx.sql(query).await?;
    //     let logical_plan = df.logical_plan();
    //     tracing::debug!("Logical plan: {}", logical_plan.display_indent());

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Array join query passed");
    //     header("Tested arrayJoin UDF works", true);

    //     //  ----------------------
    //     //
    //     // Test arrayJoin with federation in CTE
    //     let query = "
    //         WITH p2 AS (
    //             SELECT id, arrayJoin(p2.names) as names FROM clickhouse.test_db.people2 p2
    //         )
    //         SELECT p.name, m.event_id, p2.names
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN p2 ON p.id = p2.id
    //         ";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Array join with federated query in CTE passed");
    //     header("Tested arrayJoin UDF with joins in CTE works", true);

    //     //  ----------------------
    //     //
    //     // Test arrayJoin with federation in subquery
    //     let query = "
    //         SELECT p.name, m.event_id, p2.names
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN (
    //             SELECT id, arrayJoin(p2.names) as names FROM clickhouse.test_db.people2 p2
    //         ) AS p2 ON p.id = p2.id
    //         ";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Array join with federated query with Subquery passed");
    //     header("Tested arrayJoin UDF with joins with Subquery works", true);

    //     //  ----------------------
    //     //
    //     // Test arrayJoin with federation
    //     header("Error query", true);
    //     let query = "
    //         SELECT p.name, m.event_id, arrayJoin(p2.names)
    //         FROM memory.internal.mem_events m
    //         JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //         JOIN clickhouse.test_db.people2 p2 ON p.id = p2.id
    //         ";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let is_err = df.collect().await.is_err();
    //     assert!(is_err);
    //     debug!(">>> Array join with federated fails as expected");
    //     header("Tested arrayJoin UDF without manual push down fails", true);

    //     // -----------------------------
    //     //
    //     // Test clickhouse function 1
    //     let query = "
    //         SELECT id, clickhouse('arrayStringConcat(names)', 'Utf8')
    //         FROM clickhouse.test_db.people2
    //         ";
    //     debug!("Running query: {query}");

    //     let dialect = datafusion::sql::sqlparser::dialect::ClickHouseDialect {};
    //     let ast = datafusion::sql::sqlparser::parser::Parser::parse_sql(&dialect, query)?;
    //     tracing::debug!("Parsed AST: {:?}", ast);

    //     let df = ctx.sql(query).await?;
    //     let logical_plan = df.logical_plan();
    //     tracing::debug!("Logical plan: {}", logical_plan.display_indent());

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Clickhouse UDF Passed");

    //     // -----------------------------
    //     //
    //     // Test clickhouse function 2
    //     let query = "
    //         SELECT id, clickhouse('arrayStringConcat(names, '','')', 'Utf8')
    //         FROM clickhouse.test_db.people2
    //         ";
    //     debug!("Running query: {query}");

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Clickhouse UDF with Single Quotes Passed");

    //     // -----------------------------
    //     //
    //     // Test clickhouse function 3
    //     let query = "
    //         SELECT id, clickhouse('arraySort(names)', 'List(Utf8)')
    //         FROM clickhouse.test_db.people2
    //         ";

    //     let df = ctx.sql(query).await?;
    //     let results = df.collect().await?;
    //     arrow::util::pretty::print_batches(&results)?;
    //     debug!(">>> Clickhouse List Back to DataFusion Passed");

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
    //     let ctx = clickhouse.build_federated(&ctx).await?;

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
    //     let ctx = clickhouse.build_federated(&ctx).await?;

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

    //     // TODO: Figure out how to use HOC functions in ClickHouse
    //     //
    //     // There are a couple ways to do this, but I believe the best way will involve:
    //     // 1. Adding AST parsing abilities for `->`
    //     // 2. Figuring out how to pass in columns if #1 doesn't work.
    //     // 3. Or use a custom recognized symbol for `->` and replacing in PlaceholderFunction
    //     //
    //     //
    //     // // -----------------------------
    //     // //
    //     // // Test projection with custom Analyzer
    //     // header("Testing projection with custom Analyzer 5", false);

    //     // let query = "
    //     //     SELECT p.name
    //     //         , m.event_id
    //     //         , clickhouse('arrayMap(x -> concat(x, ''hello''), names)', 'List(Utf8)')
    //     //     FROM memory.internal.mem_events m
    //     //     JOIN clickhouse.test_db.people p ON p.id = m.event_id
    //     //     JOIN clickhouse.test_db.people2 p2 ON p.id = p2.id
    //     //     ";
    //     // let df = ctx
    //     //     .sql(query)
    //     //     .await
    //     //     .inspect_err(|error| error!("Error exe 5 query: {}", error))?;
    //     // let results = df.collect().await?;
    //     // arrow::util::pretty::print_batches(&results)?;
    //     // debug!(">>> Projection test custom Analyzer 5 passed");

    //     Ok(())
    // }
}
