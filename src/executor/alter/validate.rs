use {
    super::AlterError,
    crate::result::Result,
    sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
};

pub fn validate(column_def: &ColumnDef) -> Result<()> {
    let ColumnDef {
        data_type,
        options,
        name,
        ..
    } = column_def;

    // data type
    if !matches!(
        data_type,
        DataType::Boolean
            | DataType::Int
            | DataType::Float(_)
            | DataType::Text
            | DataType::Date
            | DataType::Timestamp
    ) {
        return Err(AlterError::UnsupportedDataType(data_type.to_string()).into());
    }

    // column option
    if let Some(option) = options.iter().find(|ColumnOptionDef { option, .. }| {
        !matches!(
            option,
            ColumnOption::Null
                | ColumnOption::NotNull
                | ColumnOption::Default(_)
                | ColumnOption::Unique { .. }
        )
    }) {
        return Err(AlterError::UnsupportedColumnOption(option.to_string()).into());
    }

    // unique + data type
    if matches!(data_type, DataType::Float(_))
        && options
            .iter()
            .any(|ColumnOptionDef { option, .. }| matches!(option, ColumnOption::Unique { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_string(),
            data_type.to_string(),
        )
        .into());
    }

    Ok(())
}
