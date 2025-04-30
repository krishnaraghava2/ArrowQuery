using System;
using System.IO;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using ArrowQuery.Interop;

class Program
{
    static void Main(string[] args)
    {
        // Create sample Arrow data in memory
        var idArray = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var nameArray = new StringArray.Builder().Append("Alice").Append("Bob").Append("Carol").Build();

        var fields = new List<Field>
        {
            new Field("id", ArrowType.Int32, false),
            new Field("name", ArrowType.Utf8, false)
        };
        var schema = new Schema.Builder().Fields(fields).Build();

        var recordBatch = new RecordBatch.Builder()
            .Append("id", idArray)
            .Append("name", nameArray)
            .Build();

        // Serialize to Arrow IPC bytes
        byte[] arrowBytes;
        using (var ms = new MemoryStream())
        using (var writer = new ArrowStreamWriter(ms, schema))
        {
            writer.WriteRecordBatchAsync(recordBatch).GetAwaiter().GetResult();
            writer.WriteEndAsync().GetAwaiter().GetResult();
            arrowBytes = ms.ToArray();
        }

        // Create ArrowTable
        var tablePtr = ArrowTableInterop.CreateArrowTable(arrowBytes);

        // Example SQL query
        string sql = "SELECT * FROM batch WHERE id > 1";

        // Run query and get JSON result
        string json = ArrowTableInterop.Query(tablePtr, sql);

        Console.WriteLine("Query Result (JSON):");
        Console.WriteLine(json);

        // Free ArrowTable
        ArrowTableInterop.arrow_table_free(tablePtr);
    }
}
