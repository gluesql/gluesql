pub mod data_type;
pub mod key;
pub mod value;

use {
    self::value::IntoValue,
    crate::error::ResultExt,
    gluesql_core::{
        prelude::{DataType, Key, Result},
        store::DataRow,
    },
    mongodb::bson::Document,
};

pub trait IntoRow {
    fn into_row<'a>(
        self,
        data_types: impl Iterator<Item = &'a DataType>,
        is_primary: bool,
    ) -> Result<(Key, DataRow)>;
}

impl IntoRow for Document {
    fn into_row<'a>(
        self,
        data_types: impl Iterator<Item = &'a DataType>,
        has_primary: bool,
    ) -> Result<(Key, DataRow)> {
        let key = match has_primary {
            true => self.get_binary_generic("_id").map_storage_err()?.to_owned(),
            false => self
                .get_object_id("_id")
                .map_storage_err()?
                .bytes()
                .to_vec(),
        };
        let key = Key::Bytea(key);

        let row = self
            .into_iter()
            .skip(1)
            .zip(data_types)
            .map(|((_, bson), data_type)| bson.into_value(data_type))
            .collect::<Result<Vec<_>>>()?;

        Ok((key, DataRow::Vec(row)))
    }
}
