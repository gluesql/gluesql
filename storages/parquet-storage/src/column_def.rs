use gluesql_core::{
    ast::ColumnDef,
    prelude::{DataType, Error},
};
use parquet::schema::types::Type as SchemaType;

use crate::data_type::ParquetBasicPhysicalType;

#[derive(Debug)]
pub struct ParquetSchemaType<'a>(pub &'a SchemaType);

impl<'a> TryFrom<ParquetSchemaType<'a>> for ColumnDef {
    type Error = Error; // Use the appropriate error type

    fn try_from(parquet_field: ParquetSchemaType<'a>) -> Result<Self, Self::Error> {
        let name = parquet_field.0.name().to_owned();
        let data_type = DataType::try_from(ParquetBasicPhysicalType(
            parquet_field.0.get_physical_type(),
        ))?;

        Ok(ColumnDef {
            name,
            data_type,
            nullable: false, // Parquet doesn't have nullable value
            default: None,   // Parquet doesn't support defaults
            unique: None,    // Parquet doesn't support uniqueness constraints
        })
    }
}

impl<'a> ParquetSchemaType<'a> {
    pub fn into_inner(self) -> &'a SchemaType {
        self.0
    }
}
