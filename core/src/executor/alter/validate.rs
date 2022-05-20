use {
    super::AlterError,
    crate::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
        data::value::ValueError,
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

    if let DataType::Decimal(p, s) = data_type {
        let s: u64 = (*s).unwrap_or(0);

        match *p {
            Some(x) => match x <= s {
                true => {
                    return Err(
                        AlterError::UnsupportedDecimalScale(x.to_string(), s.to_string()).into(),
                    )
                }
                false => (),
            },
            None => return Err(ValueError::NoPrecisionDecimalNotSupported.into()),
        }
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
