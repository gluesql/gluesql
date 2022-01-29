use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
        executor::evaluate_stateless,
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
    if matches!(data_type, DataType::Float | DataType::Map)
        && options
            .iter()
            .any(|ColumnOptionDef { option, .. }| matches!(option, ColumnOption::Unique { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_string(),
            data_type.clone(),
        )
        .into());
    }

    match data_type {
        DataType::Decimal(None, None) => (),
        DataType::Decimal(_p, None) => (),
        DataType::Decimal(p, s) => {
            if *p < *s {
                return Err(AlterError::UnsupportedDecimalScale(
                    (*s).unwrap().to_string(),
                    (*p).unwrap().to_string(),
                )
                .into());
            }
        }
        _ => (),
    }

    let default = options
        .iter()
        .find_map(|ColumnOptionDef { option, .. }| match option {
            ColumnOption::Default(expr) => Some(expr),
            _ => None,
        });

    if let Some(expr) = default {
        evaluate_stateless(None, expr)?;
    }

    Ok(())
}
