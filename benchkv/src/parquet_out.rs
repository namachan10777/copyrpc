use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub struct BatchRecord {
    pub elapsed_ns: u64,
    pub batch_size: u32,
}

pub struct BenchRow {
    pub mode: String,
    pub rank: u32,
    pub client_id: u32,
    pub run_index: u32,
    pub batch_index: u32,
    pub elapsed_ns: u64,
    pub batch_size: u32,
    pub server_threads: u32,
    pub client_threads: u32,
    pub queue_depth: u32,
    pub key_range: u64,
    pub peak_process_rss_kb: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn rows_from_batches(
    mode: &str,
    rank: u32,
    client_batches: &[Vec<BatchRecord>],
    run_boundaries: &[(u32, u64, u64)],
    server_threads: u32,
    client_threads: u32,
    queue_depth: u32,
    key_range: u64,
    peak_process_rss_kb: u64,
) -> Vec<BenchRow> {
    let mut rows = Vec::new();
    for (client_id, batches) in client_batches.iter().enumerate() {
        for &(run_index, start_ns, end_ns) in run_boundaries {
            let mut batch_index = 0u32;
            for b in batches {
                if b.elapsed_ns >= start_ns && b.elapsed_ns < end_ns {
                    rows.push(BenchRow {
                        mode: mode.to_string(),
                        rank,
                        client_id: client_id as u32,
                        run_index,
                        batch_index,
                        elapsed_ns: b.elapsed_ns,
                        batch_size: b.batch_size,
                        server_threads,
                        client_threads,
                        queue_depth,
                        key_range,
                        peak_process_rss_kb,
                    });
                    batch_index += 1;
                }
            }
        }
    }
    rows
}

pub fn compute_run_rps(client_batches: &[Vec<BatchRecord>], start_ns: u64, end_ns: u64) -> f64 {
    let mut total_completed = 0u64;
    for batches in client_batches {
        for b in batches {
            if b.elapsed_ns >= start_ns && b.elapsed_ns < end_ns {
                total_completed += b.batch_size as u64;
            }
        }
    }
    let duration_s = (end_ns - start_ns) as f64 / 1e9;
    if duration_s > 0.0 {
        total_completed as f64 / duration_s
    } else {
        0.0
    }
}

pub fn write_parquet(path: &str, rows: &[BenchRow]) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("mode", DataType::Utf8, false),
        Field::new("rank", DataType::UInt32, false),
        Field::new("client_id", DataType::UInt32, false),
        Field::new("run_index", DataType::UInt32, false),
        Field::new("batch_index", DataType::UInt32, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("batch_size", DataType::UInt32, false),
        Field::new("server_threads", DataType::UInt32, false),
        Field::new("client_threads", DataType::UInt32, false),
        Field::new("queue_depth", DataType::UInt32, false),
        Field::new("key_range", DataType::UInt64, false),
        Field::new("peak_process_rss_kb", DataType::UInt64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.mode.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.rank).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.client_id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.run_index).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.batch_index).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.elapsed_ns).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|r| r.batch_size).collect::<Vec<_>>(),
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
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.peak_process_rss_kb)
                    .collect::<Vec<_>>(),
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
