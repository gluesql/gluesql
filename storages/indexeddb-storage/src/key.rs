use gluesql_core::prelude::Key;
use gluesql_core::result::Result;

use crate::storage_error::StorageError;

pub(crate) fn generate_key(table_name: &str, id: u32) -> String {
    format!("{}/{}", table_name, id)
}

// Key format: table_name/0,1,2,3,4

pub(crate) fn convert_key(table_name: &str, key: &Key) -> String {
    let key: Vec<_> = key.to_cmp_be_bytes().iter().map(u8::to_string).collect();
    format!("{}/{}", table_name, key.join(","))
}

pub(crate) fn retrieve_key(table_name: &str, key: &str) -> Result<Key> {
    let key: Vec<u8> = key
        .strip_prefix(&format!("{}/", table_name))
        .ok_or_else(|| StorageError::KeyParseError(key.to_owned()))?
        .split(',')
        .map(|s| s.parse::<u8>())
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| StorageError::KeyParseError(key.to_owned()))?;

    Ok(Key::Bytea(key))
}
