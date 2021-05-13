use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
        result::Result,
    },
};

pub fn validate(column_def: &ColumnDef) -> Result<()> {
    let ColumnDef {
        data_type,
        options,
        name,
        ..
    } = column_def;

    // unique + data type
    if matches!(data_type, DataType::Float)
        && options
            .iter()
            .any(|ColumnOptionDef { option, .. }| matches!(option, ColumnOption::Unique { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_string(),
            format!("{:?}", data_type),
        )
        .into());
    }

    Ok(())
}
