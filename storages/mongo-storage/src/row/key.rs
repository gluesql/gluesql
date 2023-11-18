use {
    crate::error::MongoStorageError,
    bson::{Binary, Bson},
    gluesql_core::prelude::{Error, Key, Result},
};

pub trait KeyIntoBson {
    fn into_bson(self, has_primary: bool) -> Result<Bson>;
}

impl KeyIntoBson for Key {
    fn into_bson(self, has_primary: bool) -> Result<Bson> {
        match has_primary {
            true => Ok(Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: self.to_cmp_be_bytes()?,
            })),
            false => into_object_id(self),
        }
    }
}

pub fn into_object_id(key: Key) -> Result<Bson> {
    Ok(match key {
        Key::Bytea(bytes) => {
            let mut byte_array: [u8; 12] = [0; 12];
            byte_array[..].copy_from_slice(&bytes[..]);

            Bson::ObjectId(bson::oid::ObjectId::from_bytes(byte_array))
        }
        _ => {
            return Err(Error::StorageMsg(
                MongoStorageError::UnsupportedBsonType.to_string(),
            ))
        }
    })
}
