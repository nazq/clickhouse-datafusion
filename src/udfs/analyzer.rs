use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use clickhouse_arrow::rustc_hash::FxHashMap;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Column, plan_datafusion_err};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{Extension, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use tracing::error;

use super::plan_node::ClickHouseFunctionNode;
use super::pushdown::{
    CLICKHOUSE_FUNCTION_NODE_NAME, CLICKHOUSE_UDF_ALIASES, ClickHousePushdownUDF,
};

// TODO: Docs - What does it do?
//
// Analyzer Rule
#[derive(Default, Debug)]
pub struct ClickHouseUDFPushdownAnalyzerRule {}
impl AnalyzerRule for ClickHouseUDFPushdownAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut rewriter = ClickHouseRewriter::try_new(&plan)?;
        let transformed_plan = plan.rewrite(&mut rewriter)?;
        Ok(transformed_plan.data)
    }

    fn name(&self) -> &str { "ClickHouseUDFPushdownAnalyzerRule" }
}

// TODO: Docs - What does it do? What is it used for?
#[derive(Clone, Debug)]
pub struct ClickHouseFunction {
    pub(super) alias:       String,
    pub(super) func:        ScalarFunction,
    pub(super) field:       FieldRef,
    pub(super) table:       Option<TableReference>,
    pub(super) projections: Vec<Column>,
}

impl ClickHouseFunction {
    pub fn try_new(
        mut inner_func: ScalarFunction,
        type_str: &str,
        alias: Option<&str>,
        table: Option<TableReference>,
        nullable: bool,
    ) -> Result<Self> {
        // Parse data type
        let data_type = DataType::from_str(type_str)
            .map_err(|e| plan_datafusion_err!("Invalid data type: {e}"))?;

        // Gather inner column projections
        let mut projections = Some(HashSet::<Column>::new());

        // Update function args, removing any table aliases
        let (name, args) =
            Self::rewrite_function_name_and_args(&inner_func, &mut projections.as_mut());
        inner_func.args = args;

        // Gather projections
        let projections = projections.unwrap_or_default().into_iter().collect::<Vec<_>>();

        // Create alias
        let alias = alias.map(ToString::to_string).unwrap_or(name.clone());

        // Create arrow field
        let field = Arc::new(Field::new(&alias, data_type.clone(), nullable));

        // Attempt to store a table reference of some type
        let table = table.or(projections.first().and_then(|p| p.relation.clone()));

        Ok(Self { alias, func: inner_func, field, table, projections })
    }

    /// Update the table reference for this function.
    fn with_table(&mut self, table: &TableReference) { self.table = Some(table.clone()); }

    /// Get the table alias for the first column parsed out
    /// NOTE: I don't see a need to handle multiple columns, as it would be required that all
    /// columns come from the same table.
    fn table_alias(&self) -> Option<&str> {
        self.projections.first().and_then(|p| p.relation.as_ref().map(|r| r.table()))
    }

    fn table(&self) -> Option<&str> { self.table.as_ref().map(|t| t.table()) }

    fn alias(&self) -> &str { &self.alias }

    pub fn field(&self) -> &FieldRef { &self.field }

    pub fn func(&self) -> &ScalarFunction { &self.func }

    /// Return the inner function (first argument) if it is a ClickHouse UDF
    fn inner_function(func: &ScalarFunction) -> Option<&ScalarFunction> {
        if CLICKHOUSE_UDF_ALIASES.iter().any(|f| func.name() == *f)
            && func.args.len() == ClickHousePushdownUDF::ARG_LEN
        {
            return match &func.args[0] {
                Expr::ScalarFunction(inner_func) => Some(inner_func),
                _ => None,
            };
        }

        None
    }

    /// Generate a consistent name without table aliases and strip aliases out of column exprs
    fn rewrite_function_name_and_args(
        func: &ScalarFunction,
        collect_projections: &mut Option<&mut HashSet<Column>>,
    ) -> (String, Vec<Expr>) {
        let func =
            if let Some(inner_func) = Self::inner_function(func) { inner_func } else { func };

        let args = Self::rewrite_column_args(&func.args, collect_projections);
        let inner_display = func.func.display_name(&args).unwrap_or(func.name().to_string());

        (format!("clickhouse({inner_display})"), args)
    }

    fn rewrite_column_args(
        args: &[Expr],
        collect_projections: &mut Option<&mut HashSet<Column>>,
    ) -> Vec<Expr> {
        args.iter()
            .map(|arg| {
                arg.clone()
                    .transform_up(|e| match e {
                        Expr::Column(c) => {
                            collect_projections.as_mut().map(|p| p.insert(c.clone()));
                            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name))))
                        }
                        _ => Ok(Transformed::no(e)),
                    })
                    // Safe to unwrap, no errors thrown
                    .unwrap()
                    .data
            })
            .collect::<Vec<_>>()
    }
}

// TODO: Docs
// Rewriter for transforming the plan
struct ClickHouseRewriter {
    alias_map: HashMap<String, TableReference>, // alias -> table_name
    pushdowns: FxHashMap<String, ClickHouseFunction>, // key -> clickhouse function
    // Keep track of table refs during f_up
    stack:     Vec<TableReference>,
}

impl ClickHouseRewriter {
    fn try_new(plan: &LogicalPlan) -> Result<Self> {
        let mut alias_map = HashMap::new();
        let mut pushdowns = FxHashMap::default();
        Self::collect_aliases_and_pushdowns(plan, &mut alias_map, &mut pushdowns)?;
        Ok(Self { alias_map, pushdowns, stack: Vec::new() })
    }

    fn collect_aliases_and_pushdowns(
        node: &LogicalPlan,
        alias_map: &mut HashMap<String, TableReference>,
        pushdowns: &mut FxHashMap<String, ClickHouseFunction>,
    ) -> Result<()> {
        node.apply(|plan| {
            if let LogicalPlan::SubqueryAlias(alias) = plan {
                return alias.input.apply(|inner_plan| {
                    let mut table = None;
                    let mut finished = false;
                    if let LogicalPlan::TableScan(scan) = inner_plan {
                        table = Some(&scan.table_name);
                        finished = true;
                        alias_map.insert(alias.alias.table().to_string(), scan.table_name.clone());
                    }
                    // Collect any pushdowns
                    Self::collect_pushdowns(inner_plan, pushdowns, table)?;
                    Ok(if finished { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue })
                });
            };
            // Collect any pushdowns
            Self::collect_pushdowns(plan, pushdowns, None)?;
            Ok(TreeNodeRecursion::Continue)
        })?;

        // Run through functions, updating table where possible.
        for func in pushdowns.values_mut() {
            if let Some(aliased_table) = func.table_alias().and_then(|a| alias_map.get(a)) {
                func.with_table(aliased_table);
            }
        }

        Ok(())
    }

    fn collect_pushdowns(
        node: &LogicalPlan,
        pushdowns: &mut FxHashMap<String, ClickHouseFunction>,
        table: Option<&TableReference>,
    ) -> Result<()> {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Some(clickhouse_func) = Self::unwrap_clickhouse_expr(e, None, table)? {
                    let _ = pushdowns
                        .entry(clickhouse_func.alias().to_string())
                        .or_insert(clickhouse_func);
                    return Ok(TreeNodeRecursion::Jump);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(())
    }

    /// Given an expression try and create a [`ClickHouseFunction`]
    fn unwrap_clickhouse_expr(
        expr: &Expr,
        name: Option<&str>,
        table: Option<&TableReference>,
    ) -> Result<Option<ClickHouseFunction>> {
        match expr {
            Expr::Alias(Alias { expr, name, .. }) => {
                Self::unwrap_clickhouse_expr(expr, Some(name), table)
            }
            Expr::ScalarFunction(scalar)
                if ClickHouseFunction::inner_function(scalar).is_some() =>
            {
                Self::unwrap_udf_args(&scalar.args, name, table)
            }
            _ => Ok(None),
        }
    }

    /// Given the args of a [`ScalarFunction`] try and create a [`ClickHouseFunction`]
    fn unwrap_udf_args(
        args: &[Expr],
        name: Option<&str>,
        table: Option<&TableReference>,
    ) -> Result<Option<ClickHouseFunction>> {
        if let (
            Some(Expr::ScalarFunction(func)),
            Some(Expr::Literal(ScalarValue::Utf8(Some(type_str)), _)),
        ) = (args.first(), args.get(1))
        {
            return Ok(Some(ClickHouseFunction::try_new(
                func.clone(),
                type_str,
                name,
                table.cloned(),
                // TODO: Implement nullable
                true,
            )?));
        }
        Ok(None)
    }
}

impl ClickHouseRewriter {
    fn replace_clickhouse_function(
        expr: Expr,
        table_ref: Option<&TableReference>,
        alias: Option<&str>,
        pushdowns: &FxHashMap<String, ClickHouseFunction>,
        current_projs: &mut Option<&mut HashSet<String>>,
        modified: &mut bool,
    ) -> Result<Transformed<Expr>> {
        expr.transform_up(|e| {
            if let Expr::ScalarFunction(f) = &e {
                if ClickHouseFunction::inner_function(f).is_some() {
                    let name = if let Some(a) = alias {
                        a
                    } else {
                        &ClickHouseFunction::rewrite_function_name_and_args(f, &mut None).0
                    };
                    if let Some(func) = pushdowns.get(name) {
                        *modified = true;
                        current_projs.as_mut().map(|c| c.insert(func.alias().to_string()));
                        return Ok(Transformed::new(
                            Expr::Column(Column::new(table_ref.cloned(), func.alias())),
                            true,
                            TreeNodeRecursion::Jump,
                        ));
                    }
                }
            };
            Ok(Transformed::no(e))
        })
    }

    fn replace_plan_expressions(
        plan: LogicalPlan,
        table_ref: Option<&TableReference>,
        pushdowns: &FxHashMap<String, ClickHouseFunction>,
        current_projs: &mut HashSet<String>,
        modified: &mut bool,
    ) -> Result<LogicalPlan> {
        Ok(plan
            .map_expressions(|expr| match expr {
                Expr::Alias(Alias { name, expr, .. }) => {
                    current_projs.insert(name.to_string());
                    Self::replace_clickhouse_function(
                        *expr,
                        table_ref,
                        Some(&name),
                        pushdowns,
                        &mut None,
                        modified,
                    )
                }
                Expr::Column(column) => {
                    current_projs.insert(column.name.to_string());
                    Ok(Transformed::new(Expr::Column(column), false, TreeNodeRecursion::Jump))
                }
                Expr::ScalarFunction(f) => Self::replace_clickhouse_function(
                    Expr::ScalarFunction(f),
                    table_ref,
                    None,
                    pushdowns,
                    &mut Some(current_projs),
                    modified,
                ),
                expr => Self::replace_clickhouse_function(
                    expr,
                    table_ref,
                    None,
                    pushdowns,
                    &mut Some(current_projs),
                    modified,
                ),
            })?
            .data)
    }

    fn replace_expressions_with_pushdowns(
        plan: LogicalPlan,
        pushdowns: &FxHashMap<String, ClickHouseFunction>,
        table_ref: Option<&TableReference>,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut current_projs = HashSet::<String>::default();
        let mut modified = false;

        let new_plan = Self::replace_plan_expressions(
            plan,
            table_ref,
            pushdowns,
            &mut current_projs,
            &mut modified,
        )?;

        Ok(match &new_plan {
            LogicalPlan::Projection(Projection { input, .. }) => {
                let relevant_pushdowns = pushdowns
                    .iter()
                    .filter_map(|(_, func)| {
                        let table = table_ref.map(|t| t.table());
                        ((func.table() == table || func.table_alias() == table)
                            && !current_projs.contains(func.alias()))
                        .then_some(func.alias())
                    })
                    .map(|name| Expr::Column(Column::new(table_ref.cloned(), name.to_string())))
                    .collect::<Vec<_>>();

                // Return early if no relevant pushdowns and no modifications
                if relevant_pushdowns.is_empty() && !modified {
                    return Ok(Transformed::no(new_plan));
                }

                let mut exprs = new_plan.expressions();
                exprs.extend(relevant_pushdowns);

                Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(exprs, input.clone())
                        .inspect_err(|error| error!("Error creating projection: {error:?}"))?,
                ))
            }
            _ => Transformed::yes(
                new_plan
                    .recompute_schema()
                    .inspect_err(|error| error!("Error recomputing schema: {error:?}"))?,
            ),
        })
    }
}

impl TreeNodeRewriter for ClickHouseRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // Handle TableScan first
        let node = if let LogicalPlan::TableScan(scan) = node {
            let table_name = scan.table_name.table();

            // Reset projections
            self.stack.clear();

            // Check if any pushdowns target this table. Can't pushdown without table_ref
            let exprs = self
                .pushdowns
                .iter()
                .filter(|(_, func)| func.table().is_some())
                .filter(|(_, func)| {
                    table_name == func.table().unwrap()
                        || func
                            .table_alias()
                            .map(|a| self.alias_map.get(a).is_some_and(|t| t.table() == table_name))
                            .unwrap_or_default()
                })
                .map(|(_, func)| func.clone())
                .collect::<Vec<_>>();

            if !exprs.is_empty() {
                // Add pushdown table scan to stack
                self.stack.push(scan.table_name.clone());

                // Create pushdown node wrapper
                let ext_node = ClickHouseFunctionNode::try_new(exprs, scan)?;

                #[cfg(feature = "federation")]
                let plan = ext_node.into_projection_plan()?;

                #[cfg(not(feature = "federation"))]
                let plan = LogicalPlan::Extension(Extension { node: Arc::new(ext_node) });

                return Ok(Transformed::yes(plan));
            }

            return Ok(Transformed::new(
                LogicalPlan::TableScan(scan),
                false,
                TreeNodeRecursion::Jump,
            ));
        } else {
            node
        };

        // Update subquery aliases
        if let LogicalPlan::SubqueryAlias(subquery) = &node {
            self.stack.push(subquery.alias.clone());
        }

        // Remove any added projections in the federated case
        #[cfg(feature = "federation")]
        let node = match node {
            LogicalPlan::Projection(proj) => {
                if let LogicalPlan::SubqueryAlias(subquery) = proj.input.as_ref() {
                    if let TableReference::Bare { table } = &subquery.alias {
                        if Some(table.as_ref()) == self.stack.last().map(|t| t.table()) {
                            return Ok(Transformed::yes(proj.input.as_ref().clone()));
                        }
                    }
                }
                LogicalPlan::Projection(proj)
            }
            n => n,
        };

        match &node {
            // TODO: The following plans will need special attention
            //  - `Values`: both schema and exprs will need to be updated
            //
            // Ignored
            _ if !needs_schema_rewrite(&node) || self.pushdowns.is_empty() => {
                Ok(Transformed::no(node))
            }
            // Need expr rewrite
            _ => Self::replace_expressions_with_pushdowns(node, &self.pushdowns, self.stack.last()),
        }
    }
}

fn needs_schema_rewrite(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Extension(Extension { node: inner_node, .. })
            if inner_node.name() == CLICKHOUSE_FUNCTION_NODE_NAME =>
        {
            false
        }
        _ => !matches!(
            node,
            LogicalPlan::Subquery(_)
                | LogicalPlan::Dml(_)
                | LogicalPlan::Copy(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Ddl(_)
                | LogicalPlan::RecursiveQuery(_)
                | LogicalPlan::Analyze(_)
                | LogicalPlan::Explain(_)
                | LogicalPlan::TableScan(_)
                | LogicalPlan::EmptyRelation(_)
                | LogicalPlan::Statement(_)
                | LogicalPlan::DescribeTable(_)
        ),
    }
}
