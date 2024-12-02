pub mod avro;

use std::error::Error;

use arrow::record_batch::RecordBatch;

pub trait RecordBatchWriter {
    fn len(&self) -> usize;
    fn add(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>>;
    fn flush(&mut self) -> Result<RecordBatch, Box<dyn Error>>;
}
