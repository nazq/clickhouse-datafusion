use std::str::FromStr;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::internal_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

pub const CLICKHOUSE_UDF_ALIASES: [&str; 4] =
    ["clickhouse", "clickhouse_udf", "clickhouse_pushdown", "clickhouse_pushdown_udf"];
pub const CLICKHOUSE_FUNCTION_NODE_NAME: &str = "ClickHouseFunctionNode";

pub fn clickhouse_udf_pushdown_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ClickHousePushdownUDF::new())
}

// TODO: Implement nullability arg
#[derive(Debug)]
pub struct ClickHousePushdownUDF {
    signature: Signature,
    aliases:   Vec<String>,
}

impl Default for ClickHousePushdownUDF {
    fn default() -> Self {
        Self {
            signature: Signature::any(Self::ARG_LEN, Volatility::Immutable),
            aliases:   CLICKHOUSE_UDF_ALIASES.iter().map(ToString::to_string).collect(),
        }
    }
}

impl ClickHousePushdownUDF {
    pub const ARG_LEN: usize = 2;

    pub fn new() -> Self { Self::default() }
}

impl ScalarUDFImpl for ClickHousePushdownUDF {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str { CLICKHOUSE_UDF_ALIASES[0] }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args used")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Some(Some(ScalarValue::Utf8(Some(return_type_str)))) = &args.scalar_arguments.last()
        {
            let dt = DataType::from_str(return_type_str.as_str())
                .map_err(|e| DataFusionError::Plan(format!("Invalid return type: {e}")))?;
            Ok(Field::new(self.name(), dt, false).into())
        } else {
            Err(DataFusionError::Plan("Expected Utf8 literal for return type".into()))
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented("clickhouse UDF is for planning only".into()))
    }
}
