pub mod data_type;
pub mod key;
pub mod value;

use gluesql_core::{
    prelude::{DataType, Key},
    store::DataRow,
};
use mongodb::bson::Document;

use gluesql_core::prelude::Result;

use self::value::IntoValue;

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
            true => self.get_binary_generic("_id").unwrap().to_owned(),
            false => self.get_object_id("_id").unwrap().bytes().to_vec(),
        };
        let key = Key::Bytea(key);

        let row = self
            .into_iter()
            .skip(1)
            .zip(data_types)
            .map(|((_, bson), data_type)| bson.into_value(data_type))
            .collect::<Vec<_>>();

        Ok((key, DataRow::Vec(row)))
    }
}
