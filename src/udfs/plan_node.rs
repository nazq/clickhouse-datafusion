use std::sync::Arc;

use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{Column, DFSchema, DFSchemaRef, Spans};
use datafusion::error::Result;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    InvariantLevel, LogicalPlan, Projection, SubqueryAlias, TableScan, UserDefinedLogicalNodeCore,
};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;

use super::analyzer::ClickHouseFunction;
use super::pushdown::CLICKHOUSE_FUNCTION_NODE_NAME;

// TODO: Docs - this is the "wrapper" logical plan that is created to recognize ClickHouse functions
// that will be pushed down to the ClickHouse server.
#[derive(Clone, Debug)]
pub struct ClickHouseFunctionNode {
    table_name: String,
    exprs:      Vec<Expr>,
    input:      LogicalPlan,
    schema:     DFSchemaRef,
}

impl ClickHouseFunctionNode {
    pub fn try_new(funcs: Vec<ClickHouseFunction>, input: TableScan) -> Result<Self> {
        let (exprs, schema) = merge_function_exprs_in_schema(funcs, &input)?;
        Ok(Self {
            table_name: input.table_name.table().to_string(),
            exprs,
            input: LogicalPlan::TableScan(input),
            schema: Arc::new(schema),
        })
    }

    pub fn into_projection_plan(self) -> Result<LogicalPlan> {
        let alias = TableReference::bare(self.table_name.as_str());
        let projection =
            Projection::try_new_with_schema(self.exprs, Arc::new(self.input), self.schema)?;
        let input = Arc::new(LogicalPlan::Projection(projection));
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, alias)?))
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseFunctionNode {
    fn name(&self) -> &str { CLICKHOUSE_FUNCTION_NODE_NAME }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }

    fn schema(&self) -> &DFSchemaRef { &self.schema }

    fn expressions(&self) -> Vec<Expr> { self.exprs.clone() }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{CLICKHOUSE_FUNCTION_NODE_NAME}: {} ", self.input)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        if exprs == self.exprs && inputs[0] == self.input {
            return Ok(self.clone());
        }

        Ok(Self {
            table_name: self.table_name.clone(),
            exprs,
            schema: self.schema.clone(),
            input: inputs.swap_remove(0),
        })
    }

    fn check_invariants(&self, _check: InvariantLevel, _plan: &LogicalPlan) -> Result<()> { Ok(()) }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn supports_limit_pushdown(&self) -> bool { false }
}

impl PartialEq for ClickHouseFunctionNode {
    fn eq(&self, other: &Self) -> bool {
        self.exprs == other.exprs && self.schema == other.schema && self.input == other.input
    }
}

impl Eq for ClickHouseFunctionNode {}

impl std::hash::Hash for ClickHouseFunctionNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.exprs.hash(state);
        self.schema.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for ClickHouseFunctionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for ClickHouseFunctionNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare args element-wise
        for (self_arg, other_arg) in self.exprs.iter().zip(other.exprs.iter()) {
            let cmp = format!("{self_arg:?}").cmp(&format!("{other_arg:?}"));
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        // Compare lengths if unequal
        self.input.partial_cmp(&other.input).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Helper function to add a Vec of [`ClickHouseFunction`]s to the input schema
fn merge_function_exprs_in_schema(
    funcs: Vec<ClickHouseFunction>,
    input: &TableScan,
) -> Result<(Vec<Expr>, DFSchema)> {
    let (exprs, fields): (Vec<_>, Vec<_>) = input
        .projected_schema
        .iter()
        .filter(|(table_ref, field)| {
            !funcs
                .iter()
                .any(|func| &func.alias == field.name() && func.table.as_ref() == *table_ref)
        })
        .map(|(table_ref, field)| {
            (
                Expr::Column(Column {
                    relation: table_ref.cloned(),
                    name:     field.name().to_string(),
                    spans:    Spans::new(),
                }),
                (table_ref.cloned(), Arc::clone(field)),
            )
        })
        .collect();
    // Create filtered projection schema
    let filtered_schema =
        DFSchema::new_with_metadata(fields, input.projected_schema.metadata().clone())?;

    // Now handle functions
    let (mut function_exprs, function_fields): (Vec<_>, Vec<_>) = funcs
        .into_iter()
        .map(|func| {
            let field = func.field;
            (
                Expr::Alias(Alias {
                    relation: Some(input.table_name.clone()),
                    name:     field.name().to_string(),
                    expr:     Box::new(Expr::ScalarFunction(func.func)),
                    metadata: None,
                }),
                (Some(input.table_name.clone()), field),
            )
        })
        .unzip();

    // Merge schemas - NOTE: It's important the function schema is first, otherwise it won't work
    let mut schema =
        DFSchema::new_with_metadata(function_fields, input.projected_schema.metadata().clone())?;
    schema.merge(&filtered_schema);

    // Add existing exprs
    function_exprs.extend(exprs);

    Ok((function_exprs, schema))
}

// TODO: Decide whether to remove the following functions

type FunctionExprs = (Vec<Expr>, Vec<(Option<TableReference>, FieldRef)>);

/// Helper function to add a Vec of [`ClickHouseFunction`]s to the input schema
#[expect(unused)]
fn add_function_exprs_in_schema(
    funcs: Vec<ClickHouseFunction>,
    input: &TableScan,
) -> FunctionExprs {
    input
        .projected_schema
        .iter()
        .map(|(table_ref, field)| {
            (
                Expr::Column(Column {
                    relation: table_ref.cloned(),
                    name:     field.name().to_string(),
                    spans:    Spans::new(),
                }),
                (table_ref.cloned(), field.clone()),
            )
        })
        .chain(funcs.into_iter().map(|func| {
            let field = func.field;
            (
                Expr::Alias(Alias {
                    relation: Some(input.table_name.clone()),
                    name:     field.name().to_string(),
                    expr:     Box::new(Expr::ScalarFunction(func.func)),
                    metadata: None,
                }),
                (Some(input.table_name.clone()), field),
            )
        }))
        .unzip()
}

/// Helper function to convert a Vec of [`ClickHouseFunction`]s to exprs and [`DFSchema`]
#[expect(unused)]
fn replace_function_exprs_in_schema(
    funcs: Vec<ClickHouseFunction>,
    input: &TableScan,
) -> FunctionExprs {
    input
        .projected_schema
        .iter()
        .map(|(table_ref, field)| {
            if let Some(func) = funcs
                .iter()
                .find(|f| f.projections.first().map(|c| c.name.as_str()) == Some(field.name()))
            {
                (table_ref.cloned(), Arc::clone(func.field()), Some(func.func().clone()))
            } else {
                (table_ref.cloned(), field.clone(), None)
            }
        })
        .map(|(table_ref, field, func)| {
            if let Some(func) = func {
                (
                    Expr::Alias(Alias {
                        expr:     Box::new(Expr::ScalarFunction(func)),
                        relation: table_ref.clone(),
                        name:     field.name().to_string(),
                        metadata: None,
                    }),
                    (table_ref, field),
                )
            } else {
                (
                    Expr::Column(Column {
                        relation: table_ref.clone(),
                        name:     field.name().to_string(),
                        spans:    Spans::new(),
                    }),
                    (table_ref, field),
                )
            }
        })
        .unzip()
}
