use {gluesql_core::result::Error, thiserror::Error as ThisError};

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error(transparent)]
    Idb(#[from] idb::Error),
    #[error(transparent)]
    SerdeWasmBindgen(#[from] serde_wasm_bindgen::Error),
    #[error("Couldn't parse key `{0}`")]
    KeyParseError(String),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        use StorageError::*;

        match e {
            Idb(e) => Self::StorageMsg(format!("{:?}", e)), // Cannot take whole error as JsValue is not Send
            SerdeWasmBindgen(e) => Self::StorageMsg(format!("{:?}", e)),
            KeyParseError(s) => Self::StorageMsg(format!("{:?}", KeyParseError(s))),
        }
    }
}
