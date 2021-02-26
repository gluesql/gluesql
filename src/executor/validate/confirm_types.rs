use {
    sqlparser::ast::DataType,
    crate::data::Value,
    std::{
        fmt::Debug,
    },
    crate::{
        data::Row,
        result::Result,
        store::Store,
    },
    super::{
        ColumnValidation,
        ValidateError,
    },
};

pub async fn confirm_types<T: 'static + Debug>(
    _storage: &impl Store<T>,
    _table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(columns) => columns,
        ColumnValidation::SpecifiedColumns(columns, ..) => columns,
    };
    for row in row_iter.clone() {
        for (index, column) in columns.into_iter().enumerate() {
            let _temp = match row.get_value(index) {
                Some(row_data) => Some(match (column.data_type.clone(), row_data) {
                    (DataType::Boolean, Value::Bool(value)) | (DataType::Boolean, Value::OptBool(Some(value))) => Value::Bool(*value),
                    (DataType::Text, Value::Str(value)) | (DataType::Text, Value::OptStr(Some(value))) => Value::Str(value.to_owned()),
                    (DataType::Int, Value::I64(value)) | (DataType::Int,  Value::OptI64(Some(value))) => Value::I64(*value),
                    (DataType::Float(_), Value::F64(value)) | (DataType::Float(_), Value::OptF64(Some(value))) => Value::F64(*value),
                    (_, Value::OptBool(None)) | (_, Value::OptStr(None)) | (_, Value::OptI64(None)) | (_, Value::OptF64(None)) => row_data.to_owned(),
                    (_, row_data) => {return Err(ValidateError::IncompatibleTypeOnTypedField(format!("{:?}", row_data), column.name.value.to_string(), column.data_type.to_string()).into());},
                }),
                None => None,
            };
        }
    }
    Ok(())
}