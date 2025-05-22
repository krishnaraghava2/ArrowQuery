using System;
using System.Runtime.InteropServices;

namespace ArrowQuery.Interop
{
    internal static class ArrowQueryInterop
    {
        private const string DllName = "arrow_query"; // arrow_query.dll must be in output dir

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr arrow_database_new();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void arrow_database_free(IntPtr database);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int arrow_database_add_table(
            IntPtr database,
            IntPtr dataPtr,
            UIntPtr dataLen,
            IntPtr tableNamePtr,
            UIntPtr tableNameLen);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int arrow_database_query(
            IntPtr database,
            IntPtr sqlPtr,
            UIntPtr sqlLen,
            out IntPtr outBufPtr,
            out UIntPtr outBufLen,
            out IntPtr errorBufPtr,
            out UIntPtr errorBufLen);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void arrow_free_buffer(IntPtr ptr, UIntPtr len);

        // .NET-friendly wrapper for creating ArrowDatabase
        public static IntPtr CreateArrowDatabase()
        {
            return arrow_database_new();
        }

        // .NET-friendly wrapper for adding a table to the database
        public static void AddTableToDatabase(IntPtr database, byte[] arrowIpcBytes, string tableName)
        {
            var dataHandle = GCHandle.Alloc(arrowIpcBytes, GCHandleType.Pinned);
            var tableNameBytes = System.Text.Encoding.UTF8.GetBytes(tableName);
            var tableNameHandle = GCHandle.Alloc(tableNameBytes, GCHandleType.Pinned);

            try
            {
                int result = arrow_database_add_table(
                    database,
                    dataHandle.AddrOfPinnedObject(),
                    (UIntPtr)arrowIpcBytes.Length,
                    tableNameHandle.AddrOfPinnedObject(),
                    (UIntPtr)tableNameBytes.Length);

                if (result != 0)
                    throw new InvalidOperationException($"Failed to add table with error code {result}");
            }
            finally
            {
                dataHandle.Free();
                tableNameHandle.Free();
            }
        }

        // .NET-friendly wrapper for querying database (returns JSON string)
        public static string QueryDatabase(IntPtr database, string sql)
        {
            var sqlBytes = System.Text.Encoding.UTF8.GetBytes(sql);
            var handle = GCHandle.Alloc(sqlBytes, GCHandleType.Pinned);
            try
            {
                IntPtr outBufPtr;
                UIntPtr outBufLen;
                IntPtr errorBufPtr;
                UIntPtr errorBufLen;

                int result = arrow_database_query(
                    database,
                    handle.AddrOfPinnedObject(),
                    (UIntPtr)sqlBytes.Length,
                    out outBufPtr,
                    out outBufLen,
                    out errorBufPtr,
                    out errorBufLen);

                string errorMessage = string.Empty;
                if (errorBufLen != UIntPtr.Zero)
                {
                    var errorMessageBytes = new byte[(int)errorBufLen];
                    Marshal.Copy(errorBufPtr, errorMessageBytes, 0, (int)errorBufLen);
                    errorMessage = System.Text.Encoding.UTF8.GetString(errorMessageBytes);
                    arrow_free_buffer(errorBufPtr, errorBufLen);
                }
                
                if (result != 0)
                    throw new InvalidOperationException(errorMessage);

                var managed = new byte[(int)outBufLen];
                Marshal.Copy(outBufPtr, managed, 0, (int)outBufLen);
                arrow_free_buffer(outBufPtr, outBufLen);
                return System.Text.Encoding.UTF8.GetString(managed);
            }
            finally
            {
                handle.Free();
            }
        }
    }
}