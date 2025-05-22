use std::io::Cursor;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
// Add module declarations

pub struct ArrowTable {
    record_batch: RecordBatch,
    table_name: String,
}

impl ArrowTable {
    /// Constructs ArrowTable from a byte slice containing Arrow IPC stream data.
    pub fn new(bytes: &[u8], table_name: &str) -> Self {
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).expect("Failed to create StreamReader");
        let record_batch = reader.next().expect("No RecordBatch found").expect("Failed to read RecordBatch");
        ArrowTable { record_batch, table_name: table_name.to_string() }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }
}