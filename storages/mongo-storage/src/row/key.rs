use {
    crate::error::MongoStorageError,
    bson::{Binary, Bson},
    gluesql_core::prelude::Key,
};

type Result<T> = std::result::Result<T, MongoStorageError>;

pub trait KeyIntoBson {
    fn into_bson(self, has_primary: bool) -> Result<Bson>;
}

impl KeyIntoBson for Key {
    fn into_bson(self, has_primary: bool) -> Result<Bson> {
        if has_primary {
            Ok(Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: self
                    .to_cmp_be_bytes()
                    .map_err(|_| MongoStorageError::UnsupportedBsonType)?,
            }))
        } else {
            into_object_id(self)
        }
    }
}

pub fn into_object_id(key: Key) -> Result<Bson> {
    match key {
        Key::Bytea(bytes) => {
            let mut byte_array: [u8; 12] = [0; 12];
            byte_array[..].copy_from_slice(&bytes[..]);

            Ok(Bson::ObjectId(bson::oid::ObjectId::from_bytes(byte_array)))
        }
        _ => Err(MongoStorageError::UnsupportedBsonType),
    }
}
