use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use datafusion::sql::sqlparser::{ast, dialect};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::Dialect as UnparserDialect;

use crate::udfs::function::CLICKHOUSE_FUNC_ALIASES;

// TODO: Docs - where is this used?
//
/// A custom [`UnparserDialect`] for `ClickHouse`.
pub struct ClickHouseDialect;

impl UnparserDialect for ClickHouseDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> { Some('"') }

    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if CLICKHOUSE_FUNC_ALIASES.contains(&func_name) {
            if args.len() != 2 {
                return plan_err!("clickhouse_func expects two string arguments: syntax and type");
            }
            match &args[0] {
                Expr::Literal(
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::Utf8View(Some(s))
                    | ScalarValue::LargeUtf8(Some(s)),
                    None,
                ) => {
                    if s.is_empty() {
                        return plan_err!("clickhouse_func syntax argument cannot be empty");
                    }

                    // Tokenize the string with ClickHouseDialect
                    let mut tokenizer = Tokenizer::new(&dialect::ClickHouseDialect {}, s);
                    let tokens = tokenizer.tokenize().map_err(|e| {
                        plan_datafusion_err!("Failed to tokenize ClickHouse expression '{s}': {e}")
                    })?;
                    // Create a Parser instance
                    let mut parser =
                        Parser::new(&dialect::ClickHouseDialect {}).with_tokens(tokens);
                    Ok(Some(parser.parse_expr().map_err(|e| {
                        plan_datafusion_err!("Invalid ClickHouse expression '{s}': {e}")
                    })?))
                }
                _ => plan_err!(
                    "clickhouse_func expects a string literal syntax argument, found: {:?}",
                    args[0]
                ),
            }
        } else {
            Ok(None)
        }
    }
}
