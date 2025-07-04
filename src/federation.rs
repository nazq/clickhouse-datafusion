//! Implementations for federating `ClickHouse` schemas into a `DataFusion` [`SessionContext`].
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
#[cfg(feature = "federation")]
pub use datafusion_federation; // Re-export
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{
    FederatedQueryPlanner, FederatedTableProviderAdaptor, FederatedTableSource,
    default_optimizer_rules,
};
use futures_util::TryStreamExt;
use tracing::debug;

use crate::dialect::ClickHouseDialect;
use crate::table_provider::ClickHouseTableProvider;

// TODO: Docs - Need a lot more explaining here. Also, how does this interplay with the structures
// in `context`? Need to consolidate and define this, ensure the order is hard to mess up.
//
/// Use to modify an existing [`SessionContext`] to be used in a federated context, pushing queries
/// and statements down to the sql to be run on remote schemas.
pub fn federate_session_context(ctx: Option<&SessionContext>) -> SessionContext {
    let state = if let Some(ctx) = ctx.filter(|ctx| {
        ctx.state()
            .optimizer()
            .rules
            .iter()
            .any(|rule| rule.name() == "federation_optimizer_rule")
    }) {
        debug!("SessionContext is already federated, skipping");
        ctx.state()
    } else {
        debug!("SessionContext is not federated, adding optimizer rules");
        let rules = default_optimizer_rules();
        SessionStateBuilder::new()
            .with_optimizer_rules(rules)
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .build()
    };
    SessionContext::new_with_state(state)
}

impl ClickHouseTableProvider {
    /// Create a federated table source for this table provider.
    pub fn create_federated_table_source(self: Arc<Self>) -> Result<Arc<dyn FederatedTableSource>> {
        let table_name: RemoteTableRef = self.table.clone().into();
        let schema = self.schema();
        debug!(table = %table_name.table_ref(), "Creating federated table source");
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(fed_provider, table_name, schema)))
    }

    /// Create a federated table provider wrapping this table provider.
    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> Result<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(table_source, self))
    }
}

#[async_trait]
impl SQLExecutor for ClickHouseTableProvider {
    fn name(&self) -> &str { "clickhouse" }

    fn compute_context(&self) -> Option<String> { self.reader.compute_context() }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        Arc::new(ClickHouseDialect)
    }

    fn ast_analyzer(&self) -> Option<datafusion_federation::sql::AstAnalyzer> {
        // No custom AST rewriting needed for now; arrayJoin and other functions handled by dialect
        None
    }

    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let pool = self.writer.clone();
        let query = query.to_string();
        let exec_schema = Arc::clone(&schema);
        let stream = futures_util::stream::once(async move {
            pool.connect().await?.query_arrow(&query, &[], Some(exec_schema)).await
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.writer
            .connect()
            .await?
            .tables(self.table.schema().expect("Schema must be present"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.writer
            .connect()
            .await?
            .get_schema(&TableReference::from(table_name))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

pub mod external {
    use std::any::Any;
    use std::sync::Arc;

    use dashmap::DashMap;
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
    };
    use datafusion::common::exec_err;
    use datafusion::error::Result;
    // Re-export
    pub use datafusion_federation::sql::MultiSchemaProvider;
    use tracing::debug;

    // TODO: Docs - speak about WHY this would be done. For example, to allow federating catalogs
    // across PostgresSQL and ClickHouse.
    //
    /// A "federated" [`CatalogProvider`] that allows providing different catalog providers that can
    /// be used to serve up [`SchemaProvider`]s across multiple catalogs.
    ///
    /// See [`MultiSchemaProvider`] for the equivalent functionality across schemas.
    #[derive(Debug, Clone)]
    pub struct FederatedCatalogProvider {
        catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
    }

    impl Default for FederatedCatalogProvider {
        fn default() -> Self { Self::new() }
    }

    impl FederatedCatalogProvider {
        pub fn new() -> Self {
            let mem_catalog = MemoryCatalogProvider::new();
            mem_catalog.register_schema("internal", Arc::new(MemorySchemaProvider::new())).unwrap();
            let catalogs = DashMap::new();
            catalogs.insert(
                "datafusion".to_string(),
                Arc::new(mem_catalog) as Arc<dyn CatalogProvider>,
            );
            Self { catalogs }
        }

        /// Add a new catalog to the federated catalog provider.
        pub fn add_catalog(&self, name: impl Into<String>, catalog: Arc<dyn CatalogProvider>) {
            let name = name.into();
            debug!("Adding catalog: {name}");
            self.catalogs.insert(name, catalog);
        }

        // TODO: Docs - explain why should the catalog be registered this way, ie that it enables
        // the federation with this catalog providers schemas
        //
        // TODO: Remove - remove the panics, just return an error, or at least use debug assertions
        //
        /// Register a non-federated table in the "internal" schema of the "datafusion" catalog.
        ///
        /// # Panics
        /// Should not panic, the catalog and schema are initialized in the constructor.
        pub fn register_non_federated_table(
            &self,
            name: String,
            table: Arc<dyn TableProvider>,
        ) -> Result<Option<Arc<dyn TableProvider>>> {
            debug!("Registering non-federated table: {name}");
            self.catalogs
                .get("datafusion")
                .unwrap()
                .schema("internal")
                .expect("schema registered in constructor")
                .register_table(name, table)
        }
    }

    impl CatalogProvider for FederatedCatalogProvider {
        fn as_any(&self) -> &dyn Any { self }

        fn schema_names(&self) -> Vec<String> {
            self.catalogs.iter().flat_map(|c| c.value().schema_names()).collect()
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            for catalogs in self.catalogs.iter() {
                let catalog = catalogs.key();
                if let Some(schema) = catalogs.value().schema(name) {
                    let is_fed = if catalog == "datafusion" && name == "internal" {
                        "NonFederated"
                    } else {
                        "Federated"
                    };
                    debug!("FederatedCatalogProvider found schema ({is_fed}): {name}");
                    return Some(schema);
                }
            }
            None
        }

        // TODO: Docs - note how this registers a NON federated table
        fn register_schema(
            &self,
            name: &str,
            schema: Arc<dyn SchemaProvider>,
        ) -> Result<Option<Arc<dyn SchemaProvider>>> {
            self.catalogs.get("datafusion").unwrap().register_schema(name, schema)
        }

        fn deregister_schema(
            &self,
            name: &str,
            cascade: bool,
        ) -> Result<Option<Arc<dyn SchemaProvider>>> {
            let mut found = None;
            for schemas in self.catalogs.iter() {
                if let Some(schema) = schemas.value().schema(name) {
                    found = Some((schemas.key().to_string(), schema));
                }
            }

            if let Some((catalog_name, schema)) = found {
                let table_names = schema.table_names();
                match (table_names.is_empty(), cascade) {
                    (true, _) | (false, true) => {
                        let catalog = self.catalogs.get(&catalog_name).unwrap();
                        catalog.deregister_schema(name, cascade)
                    }
                    (false, false) => exec_err!(
                        "Cannot drop schema {} because other tables depend on it: {:?}",
                        name,
                        table_names
                    ),
                }
            } else {
                Ok(None)
            }
        }
    }
}
