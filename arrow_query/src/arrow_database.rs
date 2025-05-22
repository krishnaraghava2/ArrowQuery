use std::sync::Arc;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
// Add module declarations
use crate::{arrow_table};
pub struct ArrowDatabase {
    arrow_tables: Vec<arrow_table::ArrowTable>
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, ArrayRef};
    use arrow::datatypes::{Field, DataType, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow_array::RecordBatch;

    #[tokio::test]
    async fn test_arrow_database() {
        // Create a RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        
        let id_array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
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
        let table_name = "people";
        let mut aq = ArrowDatabase::new();
        aq.add_table(&buffer, table_name);

        // Query
        let sql = "SELECT id, name FROM people WHERE id > 1";
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

impl ArrowDatabase {
    /// Constructs ArrowDatabase from a byte slice containing Arrow IPC stream data.
    pub fn new() -> Self {
        ArrowDatabase { 
            arrow_tables: Vec::new(),
        }
    }

    /// Adds a new ArrowTable to the database.
    pub fn add_table(&mut self, bytes: &[u8], table_name: &str) {
        let table = arrow_table::ArrowTable::new(bytes, table_name);
        self.arrow_tables.push(table);
    }

    /// Executes a SQL query against the RecordBatch using DataFusion.
    pub async fn query(&self, sql: &str) -> datafusion::error::Result<String> {
        let ctx = SessionContext::new();
        for table in &self.arrow_tables {
            let record_batch = table.get_record_batch();
            let table_name = table.get_table_name();
            let schema = record_batch.schema();
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![record_batch.clone()]])?;
            ctx.register_table(table_name, Arc::new(mem_table))?;
        }

        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        // Convert results to JSON array string
        if results.is_empty() {
            return Ok("[]".to_string());
        }

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