use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::epoch::EpochData;

pub struct BenchRow {
    pub mode: String,
    pub epoch_index: u32,
    pub rps: f64,
    pub rank: u32,
    pub server_threads: u32,
    pub client_threads: u32,
    pub queue_depth: u32,
    pub key_range: u64,
    pub run_index: u32,
}

#[allow(clippy::too_many_arguments)]
pub fn rows_from_epochs(
    mode: &str,
    epochs: &[EpochData],
    rank: u32,
    server_threads: u32,
    client_threads: u32,
    queue_depth: u32,
    key_range: u64,
    run_index: u32,
) -> Vec<BenchRow> {
    epochs
        .iter()
        .map(|e| {
            let duration_secs = e.duration_ns as f64 / 1_000_000_000.0;
            BenchRow {
                mode: mode.to_string(),
                epoch_index: e.index,
                rps: e.completed as f64 / duration_secs,
                rank,
                server_threads,
                client_threads,
                queue_depth,
                key_range,
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
        Field::new("mode", DataType::Utf8, false),
        Field::new("epoch_index", DataType::UInt32, false),
        Field::new("rps", DataType::Float64, false),
        Field::new("rank", DataType::UInt32, false),
        Field::new("server_threads", DataType::UInt32, false),
        Field::new("client_threads", DataType::UInt32, false),
        Field::new("queue_depth", DataType::UInt32, false),
        Field::new("key_range", DataType::UInt64, false),
        Field::new("run_index", DataType::UInt32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.mode.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.epoch_index).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.rps).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.rank).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.server_threads).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.client_threads).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.queue_depth).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.key_range).collect::<Vec<_>>(),
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
