use {
    gluesql_core::result::Result,
    idb::{KeyRange, Query},
    wasm_bindgen::JsValue,
};

use crate::storage_error::StorageError;

/// Creates an indexeddb query for passing to a cursor that matches all objects inside the given table
pub(crate) fn table_data_query(table_name: &str) -> Result<Query> {
    let lower_bound = format!("{}/", table_name);
    let upper_bound = format!("{}0", table_name); // 0 comes after / in ascii

    Ok(Query::KeyRange(
        KeyRange::bound(
            &JsValue::from_str(&lower_bound),
            &JsValue::from_str(&upper_bound),
            None,
            None,
        )
        .map_err(StorageError::Idb)?,
    ))
}
