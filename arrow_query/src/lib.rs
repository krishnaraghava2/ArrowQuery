use datafusion::datasource::MemTable;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use std::io::Cursor;

pub struct ArrowTable {
    record_batch: RecordBatch,
}

// FFI INTEROP SECTION

#[no_mangle]
pub extern "C" fn arrow_table_new(ptr: *const u8, len: usize) -> *mut ArrowTable {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let table = ArrowTable::new(bytes);
    Box::into_raw(Box::new(table))
}

#[no_mangle]
pub extern "C" fn arrow_table_free(table: *mut ArrowTable) {
    if !table.is_null() {
        unsafe { Box::from_raw(table); }
    }
}

// Async FFI is tricky; provide a sync wrapper for simple queries
#[no_mangle]
pub extern "C" fn arrow_table_query(
    table: *mut ArrowTable,
    sql_ptr: *const u8,
    sql_len: usize,
    out_buf_ptr: *mut *mut u8,
    out_buf_len: *mut usize,
) -> i32 {
    let table = unsafe {
        assert!(!table.is_null());
        &*table
    };
    let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql_ptr, sql_len)) };

    // Use a tokio runtime for async
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return -2,
    };

    let result = rt.block_on(table.query(sql));
    let json = match result {
        Ok(s) => s,
        Err(_) => return -3,
    };
    let buffer = json.into_bytes();

    // Allocate buffer for C#
    let out = buffer.into_boxed_slice();
    let out_len = out.len();
    let out_ptr = Box::into_raw(out) as *mut u8;

    unsafe {
        *out_buf_ptr = out_ptr;
        *out_buf_len = out_len;
    }
    0 // success
}

#[no_mangle]
pub extern "C" fn arrow_free_buffer(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len) as *mut [u8]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, ArrayRef};
    use arrow::datatypes::{Field, DataType, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_arrow_query() {
        // Create a RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();

        // Serialize to bytes
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Construct ArrowTable from bytes
        let aq = ArrowTable::new(&buffer);

        // Query
        let sql = "SELECT id, name FROM batch WHERE id > 1";
        let result_json = aq.query(sql).await.unwrap();

        // Parse and check JSON results
        let json_value: serde_json::Value = serde_json::from_str(&result_json).unwrap();
        assert!(json_value.is_array());
        let rows = json_value.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"].as_i64().unwrap(), 2);
        assert_eq!(rows[1]["id"].as_i64().unwrap(), 3);
        assert_eq!(rows[0]["name"].as_str().unwrap(), "Bob");
        assert_eq!(rows[1]["name"].as_str().unwrap(), "Carol");
    }
}

impl ArrowTable {
    /// Constructs ArrowTable from a byte slice containing Arrow IPC stream data.
    pub fn new(bytes: &[u8]) -> Self {
        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).expect("Failed to create StreamReader");
        let record_batch = reader.next().expect("No RecordBatch found").expect("Failed to read RecordBatch");
        ArrowTable { record_batch }
    }

    /// Executes a SQL query against the RecordBatch using DataFusion.
    pub async fn query(&self, sql: &str) -> datafusion::error::Result<String> {
        let schema = Arc::new(self.record_batch.schema().as_ref().clone());
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![self.record_batch.clone()]])?;
        ctx.register_table("batch", Arc::new(mem_table))?;
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        // Convert results to JSON array string
        let schema = results.get(0).map(|b| b.schema()).unwrap_or_else(|| self.record_batch.schema());
        let mut buf = Vec::new();
        let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut buf);
        for batch in &results {
            writer.write(batch).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        }
        writer.finish().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let json_lines = String::from_utf8(buf).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let json_array: Vec<serde_json::Value> = json_lines
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        Ok(serde_json::to_string_pretty(&json_array).unwrap_or_else(|_| "[]".to_string()))
    }
}
