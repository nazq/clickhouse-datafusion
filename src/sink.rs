use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::sql::TableReference;
use futures_util::{StreamExt, TryStreamExt};

use crate::connection::ClickHouseConnectionPool;

// TODO: Docs
/// [`datafusion::datasource::sink::DataSink`] for `ClickHouse`
#[derive(Debug)]
pub struct ClickHouseDataSink {
    #[cfg_attr(feature = "mocks", expect(unused))]
    writer:            Arc<ClickHouseConnectionPool>,
    table:             TableReference,
    schema:            SchemaRef,
    metrics:           ExecutionPlanMetricsSet,
    write_concurrency: usize,
}

impl ClickHouseDataSink {
    pub fn new(
        writer: Arc<ClickHouseConnectionPool>,
        table: TableReference,
        schema: SchemaRef,
    ) -> Self {
        let write_concurrency = writer.write_concurrency();
        Self { writer, table, schema, metrics: ExecutionPlanMetricsSet::new(), write_concurrency }
    }

    /// Verify that a passed in schema aligns with the sink schema
    ///
    /// Ordering and metadata don't matter
    ///
    /// # Errors
    /// - Returns an error if the field lengths don't match
    /// - Returns an error if data types don't match
    /// - Returns an error if names don't match
    /// - Returns an error if nullability doesn't match
    pub fn verify_input_schema(&self, input: &SchemaRef) -> Result<()> {
        let sink_fields = self.schema.fields();
        let input_fields = input.fields();
        if sink_fields.len() != input_fields.len() {
            let (input_len, sink_len) = (input_fields.len(), sink_fields.len());
            return exec_err!(
                "Schema fields must match, input has {input_len} fields, sink {sink_len}"
            );
        }

        for field in sink_fields {
            let name = field.name();
            let data_type = field.data_type();
            let is_nullable = field.is_nullable();

            let Some((_, input_field)) = input_fields.find(name) else {
                return exec_err!("Sink field {name} missing from input");
            };

            if data_type != input_field.data_type() {
                return exec_err!(
                    "Sink field {name} expected data type {data_type:?} but found {:?}",
                    input_field.data_type()
                );
            }

            if is_nullable != input_field.is_nullable() {
                return exec_err!(
                    "Sink field {name} expected nullability {is_nullable} but found {}",
                    input_field.is_nullable()
                );
            }
        }

        Ok(())
    }
}

impl DisplayAs for ClickHouseDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "ClickHouseDataSink: table={}", self.table)
    }
}

#[async_trait::async_trait]
impl datafusion::datasource::sink::DataSink for ClickHouseDataSink {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn schema(&self) -> &SchemaRef { &self.schema }

    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> Result<u64> {
        #[cfg(not(feature = "mocks"))]
        use datafusion::error::DataFusionError;

        // Create baseline metrics for this partition
        // DataSink always runs on partition 0 (by DataFusion design)
        let partition = 0;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let _timer = baseline.elapsed_compute().timer();

        let db = self.table.schema();
        let table = self.table.table();

        let query = if let Some(db) = db {
            format!("INSERT INTO {db}.{table} FORMAT Native")
        } else {
            format!("INSERT INTO {table} FORMAT Native")
        };

        #[cfg(not(feature = "mocks"))]
        let writer = Arc::clone(&self.writer);
        let schema = Arc::clone(&self.schema);
        let concurrency = self.write_concurrency;
        let baseline_clone = baseline.clone();

        // Process batches concurrently using buffer_unordered
        let row_count = data
            .map(move |batch_result| {
                #[cfg(not(feature = "mocks"))]
                let writer_clone = Arc::clone(&writer);
                let query = query.clone();
                let schema = Arc::clone(&schema);
                let baseline = baseline_clone.clone();

                async move {
                    let batch = batch_result?;

                    // Runtime schema validation
                    let sink_fields = schema.fields();
                    let input_fields = batch.schema_ref().fields();
                    if sink_fields.len() != input_fields.len() {
                        let (input_len, sink_len) = (input_fields.len(), sink_fields.len());
                        return exec_err!(
                            "Schema fields must match, input has {input_len} fields, sink \
                             {sink_len}"
                        );
                    }

                    for field in sink_fields {
                        let name = field.name();
                        let data_type = field.data_type();
                        let is_nullable = field.is_nullable();

                        let Some((_, input_field)) = input_fields.find(name) else {
                            return exec_err!("Sink field {name} missing from input");
                        };

                        if data_type != input_field.data_type() {
                            return exec_err!(
                                "Sink field {name} expected data type {data_type:?} but found {:?}",
                                input_field.data_type()
                            );
                        }

                        if is_nullable != input_field.is_nullable() {
                            return exec_err!(
                                "Sink field {name} expected nullability {is_nullable} but found {}",
                                input_field.is_nullable()
                            );
                        }
                    }

                    let num_rows = batch.num_rows();

                    #[cfg(not(feature = "mocks"))]
                    {
                        let pool_conn = writer_clone
                            .pool()
                            .get()
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let mut results = pool_conn
                            .insert(&query, batch, None)
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        // Drain the result stream to ensure the insert completes
                        while let Some(result) = results.next().await {
                            result.map_err(|e| DataFusionError::External(Box::new(e)))?;
                        }
                    }

                    #[cfg(feature = "mocks")]
                    eprintln!("Mocking query: {query}");

                    baseline.record_output(num_rows);
                    Ok(num_rows as u64)
                }
            })
            .buffer_unordered(concurrency)
            .try_fold(0u64, |acc, rows| async move { Ok(acc + rows) })
            .await?;

        Ok(row_count)
    }
}
