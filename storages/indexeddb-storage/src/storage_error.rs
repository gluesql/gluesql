use {gluesql_core::result::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error(transparent)]
    Idb(#[from] idb::Error),
    #[error(transparent)]
    SerdeWasmBindgen(#[from] serde_wasm_bindgen::Error),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        use StorageError::*;

        match e {
            Idb(e) => Self::StorageMsg(e.to_string()), // Cannot take whole error as JsValue is not thread-safe
            SerdeWasmBindgen(e) => Self::StorageMsg(e.to_string()),
            // Sled(e) => Error::Storage(Box::new(e)),
            // Bincode(e) => Error::Storage(e),
            // Str(e) => Error::Storage(Box::new(e)),
            // SystemTime(e) => Error::Storage(Box::new(e)),
            // TryFromSlice(e) => Error::Storage(Box::new(e)),
            // AlterTable(e) => e.into(),
            // Index(e) => e.into(),
        }
    }
}
