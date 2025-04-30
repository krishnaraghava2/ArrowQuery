using System;
using System.IO;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using ArrowQuery.Interop;
using Apache.Arrow.Types;

class Program
{
    static void Main(string[] args)
    {
        // Create sample Arrow data in memory
        var idArray = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var nameArray = new StringArray.Builder().Append("Alice").Append("Bob").Append("Carol").Build();

        var fields = new List<Field>
        {
            new Field("id", Int32Type.Default, false),
            new Field("name", StringType.Default, false)
        };
        var schema = new Schema(fields, new Dictionary<string, string>());

        var recordBatch = new RecordBatch.Builder()
            .Append("id", true, idArray)
            .Append("name", true, nameArray)
            .Build();

        Apache.Arrow.Compression.CompressionCodecFactory factory = new Apache.Arrow.Compression.CompressionCodecFactory();

        var options = new IpcOptions
        {
            CompressionCodec = CompressionCodecType.Zstd,
            CompressionLevel = 3,
            CompressionCodecFactory = factory,
        };

        // Serialize to Arrow IPC bytes
        byte[] arrowBytes;
        using (var ms = new MemoryStream())
        using (var writer = new ArrowStreamWriter(ms, schema, false, options))
        {
            writer.WriteRecordBatchAsync(recordBatch).GetAwaiter().GetResult();
            writer.WriteEndAsync().GetAwaiter().GetResult();
            arrowBytes = ms.ToArray();
        }

        using (ArrowQuery.Interop.ArrowTable arrowTable = new ArrowTable(arrowBytes))
        {
            string sql = "SELECT * FROM batch WHERE id > 1";
            var json = arrowTable.Query(sql);
            Console.WriteLine("Query Result (JSON):");
            Console.WriteLine(json);
        }
    }
}
