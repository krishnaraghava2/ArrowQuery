# ArrowQuery

ArrowQuery is a cross-language solution for high-performance data processing, combining Rust and .NET via FFI. It provides a Rust library for Arrow-based data operations and a .NET interop layer for seamless integration.

## Project Structure

- **arrow_query/**: Rust library using Apache Arrow and DataFusion for data processing.
- **ArrowQuery.Interop/**: .NET interop layer (C#) for calling into the Rust library.
- **ArrowQuery.Sample/**: Example .NET application demonstrating usage.

## Prerequisites

- Rust (with Cargo)
- .NET SDK (6.0+ recommended)
- Visual Studio Build Tools (for sn.exe if signing is required)

## Building

### Rust Library

```sh
cd arrow_query
cargo build --release
```

### .NET Projects

```sh
dotnet build ArrowQuery.Interop/ArrowQuery.Interop.csproj
dotnet build ArrowQuery.Sample/ArrowQuery.Sample.csproj
```

## Testing

Run Rust and .NET tests:

```sh
cd arrow_query
cargo test

cd ..
dotnet test ArrowQuery.Sample/ArrowQuery.Sample.csproj
```

## Assembly Signing

- The .NET interop assembly uses `ArrowQuery.snk` for strong-name signing.
- To sign NuGet packages, generate a code signing certificate (`.pfx`) and use `nuget sign`.

## Troubleshooting

- If you see errors about missing zstd decompression, ensure the `zstd` feature is enabled in `arrow_query/Cargo.toml`.
- For FFI issues, verify that the Rust library is built as `cdylib` and accessible to .NET.

## Example Usage

Below is a minimal example using the interop layer from C#:

```csharp
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
        var cityArray = new StringArray.Builder().Append("New York").Append("Los Angeles").Append("Chicago").Build();

        var fields1 = new List<Field>
        {
            new Field("id", Int32Type.Default, false),
            new Field("name", StringType.Default, false)
        };


        var fields2 = new List<Field>
        {
            new Field("id", Int32Type.Default, false),
            new Field("city", StringType.Default, false)
        };

        var schema1 = new Schema(fields1, new Dictionary<string, string>());

        var schema2 = new Schema(fields2, new Dictionary<string, string>());

        var recordBatch1 = new RecordBatch.Builder()
            .Append("id", true, idArray)
            .Append("name", true, nameArray)
            .Build();

        var recordBatch2 = new RecordBatch.Builder()
            .Append("id", true, idArray)
            .Append("city", true, cityArray)
            .Build();

        Apache.Arrow.Compression.CompressionCodecFactory factory = new Apache.Arrow.Compression.CompressionCodecFactory();

        var options = new IpcOptions
        {
            CompressionCodec = CompressionCodecType.Zstd,
            CompressionLevel = 3,
            CompressionCodecFactory = factory,
        };

        // Serialize to Arrow IPC bytes
        byte[] arrowBytes1;
        using (var ms = new MemoryStream())
        using (var writer = new ArrowStreamWriter(ms, schema1, false, options))
        {
            writer.WriteRecordBatchAsync(recordBatch1).GetAwaiter().GetResult();
            writer.WriteEndAsync().GetAwaiter().GetResult();
            arrowBytes1 = ms.ToArray();
        }

        byte[] arrowBytes2;
        using (var ms = new MemoryStream())
        using (var writer = new ArrowStreamWriter(ms, schema2, false, options))
        {
            writer.WriteRecordBatchAsync(recordBatch2).GetAwaiter().GetResult();
            writer.WriteEndAsync().GetAwaiter().GetResult();
            arrowBytes2 = ms.ToArray();
        }

        using (ArrowQuery.Interop.ArrowDatabase arrowDb = new ArrowDatabase())
        {
            arrowDb.AddTable(arrowBytes1, "batch1");
            arrowDb.AddTable(arrowBytes2, "batch2");
            string sql = "SELECT b1.*, b2.* FROM batch1 b1 join batch2 b2 ON b1.id=b2.id";
            var json = arrowDb.Query(sql);
            Console.WriteLine("Query Result (JSON):");
            Console.WriteLine(json);
        }
    }
}

```
## Note

Note: The Table name will always be 'batch' and column names are always converted to lower case by DataFusion. If you have Uppercase then use column names quoted like :

SELECT "ExampleColumn" FROM batch

## License

MIT License.
