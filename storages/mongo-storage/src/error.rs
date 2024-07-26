use {
    bson::document::ValueAccessError,
    gluesql_core::error::Error,
    std::{array::TryFromSliceError, num::TryFromIntError},
    thiserror::Error,
};

pub trait ResultExt<T, E> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T> ResultExt<T, mongodb::error::Error> for std::result::Result<T, mongodb::error::Error> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, serde_json::Error> for std::result::Result<T, serde_json::Error> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, bson::ser::Error> for std::result::Result<T, bson::ser::Error> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, chrono::ParseError> for std::result::Result<T, chrono::ParseError> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, ValueAccessError> for std::result::Result<T, ValueAccessError> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, std::net::AddrParseError>
    for std::result::Result<T, std::net::AddrParseError>
{
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, TryFromIntError> for std::result::Result<T, TryFromIntError> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, TryFromSliceError> for std::result::Result<T, TryFromSliceError> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

impl<T> ResultExt<T, MongoStorageError> for std::result::Result<T, MongoStorageError> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

pub trait OptionExt<T, E: ToString> {
    fn map_storage_err(self, error: E) -> Result<T, Error>;
}

impl<T, E: ToString> OptionExt<T, E> for std::option::Option<T> {
    fn map_storage_err(self, error: E) -> Result<T, Error> {
        self.ok_or_else(|| error.to_string())
            .map_err(Error::StorageMsg)
    }
}

#[derive(Error, Debug)]
pub enum MongoStorageError {
    #[error("invalid document")]
    InvalidDocument,

    #[error("unreachable")]
    Unreachable,

    #[error("unsupported bson type")]
    UnsupportedBsonType,

    #[error(r#"Invalid bsonType - it should be Array eg) ["string"] or ["string", "null"]"#)]
    InvalidBsonType,

    #[error("Invalid glueType - it should be type of GlueSQL Value")]
    InvalidGlueType,
}
