using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArrowQuery.Interop
{
    /// <summary>
    /// Represents an Arrow table that can be queried using SQL.
    /// </summary>
    public class ArrowTable : IDisposable
    {
        private IntPtr _tablePtr;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrowTable"/> class with the specified Arrow IPC bytes.
        /// </summary>
        /// <param name="arrowIpcBytes"></param>
        public ArrowTable(byte[] arrowIpcBytes)
        {
            _tablePtr = ArrowQueryInterop.CreateArrowTable(arrowIpcBytes);
        }

        /// <summary>
        /// Queries the Arrow table using the specified SQL query.
        /// </summary>
        /// <param name="sql">SQL Query</param>
        /// <returns></returns>
        public string Query(string sql)
        {
            return ArrowQueryInterop.Query(_tablePtr, sql);
        }

        /// <summary>
        /// Releases the resources used by the <see cref="ArrowTable"/> instance.
        /// </summary>
        public void Dispose()
        {
            if (_tablePtr != IntPtr.Zero)
            {
                ArrowQueryInterop.arrow_table_free(_tablePtr);
                _tablePtr = IntPtr.Zero;
            }
        }
    }
}
