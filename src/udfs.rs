//! Various UDFs providing `DataFusion`'s sql parsing with some `ClickHouse` specific functionality.
//!
//! [`self::function::ClickHouseFunc`] is a sort of 'escape-hatch' to allow passing
//! syntax directly to ClickHouse as SQL.

pub mod analyzer;
pub mod function;
pub mod placeholder;
pub mod plan_node;
pub mod planner;
pub mod pushdown;

use datafusion::prelude::SessionContext;

// TODO: Docs - explain how this registers the best-effort UDF that can be used when the full
// `ClickHouseQueryPlanner` is not available.
//
/// Registers `ClickHouse`-specific UDFs with the provided [`SessionContext`].
pub fn register_clickhouse_functions(ctx: &SessionContext) {
    ctx.register_udf(function::clickhouse_func_udf());
}
