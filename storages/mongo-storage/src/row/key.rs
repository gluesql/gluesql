use {
    crate::error::{MongoStorageError, ResultExt},
    bson::{Binary, Bson},
    gluesql_core::prelude::{Error, Key, Result},
    std::str::FromStr,
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
        Key::Str(str) => Bson::ObjectId(bson::oid::ObjectId::from_str(&str).map_storage_err()?),
        Key::Bytea(bytes) => {
            if bytes.len() != 12 {
                return Err(Error::StorageMsg(
                    MongoStorageError::UnsupportedBsonType.to_string(),
                ));
            }

            let mut byte_array: [u8; 12] = [0; 12];
            byte_array[..].copy_from_slice(&bytes[..]);

            Bson::ObjectId(bson::oid::ObjectId::from_bytes(byte_array))
        }
        Key::U8(val) => Bson::ObjectId(bson::oid::ObjectId::from([val; 12])),
        _ => {
            return Err(Error::StorageMsg(
                MongoStorageError::UnsupportedBsonType.to_string(),
            ))
        }
    })
}
