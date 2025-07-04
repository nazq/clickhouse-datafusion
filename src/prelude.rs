//!
//! To simplify compatibility, crates for [`clickhouse_arrow`], [`datafusion`], and
//! [`datafusion::arrow`] are re-exported.

/// Re-exports
mod reexports {
    pub use datafusion::arrow;
    pub use {clickhouse_arrow, datafusion};
}

pub use reexports::*;

pub use super::builders::{ClickHouseBuilder, ClickHouseEngine};
pub use super::connection::{ArrowPoolConnection, ClickHouseConnection, ClickHouseConnectionPool};
pub use super::sink::ClickHouseDataSink;
pub use super::sql::SqlTable;
pub use super::table_factory::{ClickHouseTableFactory, ClickHouseTableProviderFactory};
pub use super::table_provider::ClickHouseTableProvider;
pub use super::udfs::function::clickhouse_func_udf;
pub use super::udfs::register_clickhouse_functions;
