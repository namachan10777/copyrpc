use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::epoch::EpochData;

pub struct BenchRow {
    pub system: String,
    pub mode: String,
    pub epoch_index: u32,
    pub rps: f64,
    pub message_size: u64,
    pub endpoints: u32,
    pub inflight_per_ep: u32,
    pub clients: u32,
    pub threads: u32,
    pub run_index: u32,
}

#[allow(clippy::too_many_arguments)]
pub fn rows_from_epochs(
    system: &str,
    mode: &str,
    epochs: &[EpochData],
    message_size: u64,
    endpoints: u32,
    inflight_per_ep: u32,
    clients: u32,
    threads: u32,
    run_index: u32,
) -> Vec<BenchRow> {
    epochs
        .iter()
        .map(|e| {
            let duration_secs = e.duration_ns as f64 / 1_000_000_000.0;
            BenchRow {
                system: system.to_string(),
                mode: mode.to_string(),
                epoch_index: e.index,
                rps: e.completed as f64 / duration_secs,
                message_size,
                endpoints,
                inflight_per_ep,
                clients,
                threads,
                run_index,
            }
        })
        .collect()
}

pub fn write_parquet(path: &str, rows: &[BenchRow]) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("system", DataType::Utf8, false),
        Field::new("mode", DataType::Utf8, false),
        Field::new("epoch_index", DataType::UInt32, false),
        Field::new("rps", DataType::Float64, false),
        Field::new("message_size", DataType::UInt64, false),
        Field::new("endpoints", DataType::UInt32, false),
        Field::new("inflight_per_ep", DataType::UInt32, false),
        Field::new("clients", DataType::UInt32, false),
        Field::new("threads", DataType::UInt32, false),
        Field::new("run_index", DataType::UInt32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.system.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.mode.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.epoch_index).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.rps).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.message_size).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.endpoints).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.inflight_per_ep).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.clients).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.threads).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.run_index).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
