use gluesql_core::result::Result;
use idb::{KeyRange, Query};
use wasm_bindgen::JsValue;

use crate::storage_error::StorageError;

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
