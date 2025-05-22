using System;
using System.Collections.Generic;

namespace ArrowQuery.Interop
{
    /// <summary>
    /// Represents an Arrow database that can contain multiple tables and be queried using SQL.
    /// </summary>
    public class ArrowDatabase : IDisposable
    {
        private IntPtr _databasePtr;
        private readonly Dictionary<string, byte[]> _tables = new Dictionary<string, byte[]>();
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrowDatabase"/> class.
        /// </summary>
        public ArrowDatabase()
        {
            _databasePtr = ArrowQueryInterop.CreateArrowDatabase();
        }

        /// <summary>
        /// Adds an Arrow table to the database with the specified name.
        /// </summary>
        /// <param name="arrowIpcBytes">The Arrow IPC format bytes representing the table data</param>
        /// <param name="tableName">The name to assign to the table in the database</param>
        public void AddTable(byte[] arrowIpcBytes, string tableName)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ArrowDatabase));

            ArrowQueryInterop.AddTableToDatabase(_databasePtr, arrowIpcBytes, tableName);
            _tables[tableName] = arrowIpcBytes;
        }

        /// <summary>
        /// Queries the Arrow database using the specified SQL query.
        /// </summary>
        /// <param name="sql">SQL Query</param>
        /// <returns>Query results in JSON format</returns>
        public string Query(string sql)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ArrowDatabase));

            return ArrowQueryInterop.QueryDatabase(_databasePtr, sql);
        }

        /// <summary>
        /// Releases the resources used by the <see cref="ArrowDatabase"/> instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="ArrowDatabase"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (_databasePtr != IntPtr.Zero)
                {
                    ArrowQueryInterop.arrow_database_free(_databasePtr);
                    _databasePtr = IntPtr.Zero;
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Finalizer to ensure resources are freed if Dispose is not called.
        /// </summary>
        ~ArrowDatabase()
        {
            Dispose(false);
        }
    }
}