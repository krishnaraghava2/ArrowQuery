// Add module declarations
mod arrow_table;
mod arrow_database;

// Import the necessary types from those modules
use arrow_database::ArrowDatabase;

// FFI INTEROP SECTION

#[unsafe(no_mangle)]
pub extern "C" fn arrow_database_new() -> *mut ArrowDatabase {
    let database = ArrowDatabase::new();
    Box::into_raw(Box::new(database))
}

#[unsafe(no_mangle)]
pub extern "C" fn arrow_database_free(database: *mut ArrowDatabase) {
    if !database.is_null() {
        unsafe {
            let _ = Box::from_raw(database);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn arrow_database_add_table(
    database: *mut ArrowDatabase,
    data_ptr: *const u8,
    data_len: usize,
    table_name_ptr: *const u8,
    table_name_len: usize
) -> i32 {
    if database.is_null() {
        return -1;
    }

    let database = unsafe { &mut *database };
    let bytes = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
    let table_name = match unsafe { std::str::from_utf8(std::slice::from_raw_parts(table_name_ptr, table_name_len)) } {
        Ok(name) => name,
        Err(_) => return -2,
    };

    database.add_table(bytes, table_name);
    0 // success
}

#[unsafe(no_mangle)]
pub extern "C" fn arrow_database_query(
    database: *mut ArrowDatabase,
    sql_ptr: *const u8,
    sql_len: usize,
    out_buf_ptr: *mut *mut u8,
    out_buf_len: *mut usize,
    error_buf_ptr: *mut *mut u8,
    error_buf_len: *mut usize,
) -> i32 {
    if database.is_null() {
        return -1;
    }

    let database = unsafe { &mut *database };
    let sql = match unsafe { std::str::from_utf8(std::slice::from_raw_parts(sql_ptr, sql_len)) } {
        Ok(sql) => sql,
        Err(_) => return -2,
    };

    // Use a tokio runtime for async
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return -3,
    };

    let result = rt.block_on(database.query(sql));
    let json = match result {
        Ok(s) => s,
        Err(e) => {
            let error = e.to_string();
            if !error.is_empty() {
                let error_buffer = error.into_bytes();
                let error_out = error_buffer.into_boxed_slice();
                let error_out_len = error_out.len();
                let error_out_ptr = Box::into_raw(error_out) as *mut u8;

                unsafe {
                    *error_buf_ptr = error_out_ptr;
                    *error_buf_len = error_out_len;
                }
            }
            return -4
        }
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

#[unsafe(no_mangle)]
pub extern "C" fn arrow_free_buffer(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len) as *mut [u8]);
        }
    }
}