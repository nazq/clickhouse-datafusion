use clickhouse_arrow::Error as ClickhouseNativeError;
use datafusion::error::DataFusionError;

/// Helper to map [`ClickhouseNativeError`] to [`DataFusionError`]
pub fn map_clickhouse_err(error: ClickhouseNativeError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Helper to map any [`std::error::Error`] to [`DataFusionError`]
pub fn map_external_err<E>(error: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(error))
}
