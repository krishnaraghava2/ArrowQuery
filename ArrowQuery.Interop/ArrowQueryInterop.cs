using System;
using System.Runtime.InteropServices;

namespace ArrowQuery.Interop
{
    public static class ArrowQueryInterop
    {
        private const string DllName = "arrow_query"; // arrow_query.dll must be in output dir

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr arrow_table_new(IntPtr bytes, UIntPtr len);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void arrow_table_free(IntPtr table);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int arrow_table_query(
            IntPtr table,
            IntPtr sqlPtr,
            UIntPtr sqlLen,
            out IntPtr outBufPtr,
            out UIntPtr outBufLen);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void arrow_free_buffer(IntPtr ptr, UIntPtr len);

        // .NET-friendly wrapper for creating ArrowTable
        public static IntPtr CreateArrowTable(byte[] arrowIpcBytes)
        {
            var handle = GCHandle.Alloc(arrowIpcBytes, GCHandleType.Pinned);
            try
            {
                return arrow_table_new(handle.AddrOfPinnedObject(), (UIntPtr)arrowIpcBytes.Length);
            }
            finally
            {
                handle.Free();
            }
        }

        // .NET-friendly wrapper for querying (returns JSON string)
        public static string Query(IntPtr table, string sql)
        {
            var sqlBytes = System.Text.Encoding.UTF8.GetBytes(sql);
            var handle = GCHandle.Alloc(sqlBytes, GCHandleType.Pinned);
            try
            {
                IntPtr outBufPtr;
                UIntPtr outBufLen;
                int result = arrow_table_query(
                    table,
                    handle.AddrOfPinnedObject(),
                    (UIntPtr)sqlBytes.Length,
                    out outBufPtr,
                    out outBufLen);
                if (result != 0)
                    throw new InvalidOperationException($"Query failed with code {result}");

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
