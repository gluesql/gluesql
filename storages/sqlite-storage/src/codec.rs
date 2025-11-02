use {
    crate::{map_sql_err, schema::primary_column},
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::{Key, Schema, Value, value::BTreeMapJsonExt},
        error::{Error as GlueError, Result},
        store::DataRow,
    },
    rusqlite::{Row, types::Value as SqlValue},
    serde_json::Value as JsonValue,
    std::{collections::BTreeMap, convert::TryFrom, convert::TryInto},
    uuid::Uuid,
};

pub(crate) fn decode_row_with_key(schema: &Schema, row: &Row<'_>) -> Result<(Key, DataRow)> {
    if let Some(columns) = schema.column_defs.as_ref() {
        let (values, key) = if let Some(name) = primary_column(schema) {
            let values = decode_structured_values(row, columns)?;
            let index = columns.iter().position(|col| col.name == name).unwrap_or(0);
            let key = Key::try_from(values[index].clone())?;
            (values, key)
        } else {
            let rowid: i64 = row.get(0).map_err(map_sql_err)?;
            let values = decode_structured_values_with_offset(row, columns, 1)?;
            (values, Key::I64(rowid))
        };
        Ok((key, DataRow::Vec(values)))
    } else {
        let rowid: i64 = row.get(0).map_err(map_sql_err)?;
        let payload: String = row.get(1).map_err(map_sql_err)?;
        let map = BTreeMap::parse_json_object(&payload)?;
        Ok((Key::I64(rowid), DataRow::Map(map)))
    }
}

pub(crate) fn build_key_params(key: &Key, schema: &Schema) -> Result<Vec<SqlValue>> {
    match primary_column(schema) {
        Some(_) => Ok(vec![key_to_sql(key)?]),
        None => match key {
            Key::I64(id) => Ok(vec![SqlValue::Integer(*id)]),
            _ => Err(GlueError::StorageMsg("rowid must be an i64 key".to_owned())),
        },
    }
}

pub(crate) fn key_to_sql(key: &Key) -> Result<SqlValue> {
    let value = match key {
        Key::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Key::I8(v) => SqlValue::Integer(i64::from(*v)),
        Key::I16(v) => SqlValue::Integer(i64::from(*v)),
        Key::I32(v) => SqlValue::Integer(i64::from(*v)),
        Key::I64(v) => SqlValue::Integer(*v),
        Key::I128(v) => SqlValue::Text(v.to_string()),
        Key::U8(v) => SqlValue::Integer(i64::from(*v)),
        Key::U16(v) => SqlValue::Integer(i64::from(*v)),
        Key::U32(v) => SqlValue::Integer(i64::from(*v)),
        Key::U64(v) => SqlValue::Text(v.to_string()),
        Key::U128(v) => SqlValue::Text(v.to_string()),
        Key::F32(v) => SqlValue::Real(f64::from(**v)),
        Key::F64(v) => SqlValue::Real(**v),
        Key::Decimal(v) => SqlValue::Text(v.to_string()),
        Key::Str(v) => SqlValue::Text(v.clone()),
        Key::Bytea(v) => SqlValue::Blob(v.clone()),
        Key::Date(v) => SqlValue::Text(v.to_string()),
        Key::Timestamp(v) => SqlValue::Text(v.to_string()),
        Key::Time(v) => SqlValue::Text(v.to_string()),
        Key::Interval(v) => SqlValue::Text(v.to_sql_str()),
        Key::Uuid(v) => SqlValue::Text(Uuid::from_u128(*v).hyphenated().to_string()),
        Key::Inet(v) => SqlValue::Text(v.to_string()),
        Key::None => {
            return Err(GlueError::StorageMsg(
                "Key::None cannot be encoded as parameter".to_owned(),
            ));
        }
    };

    Ok(value)
}

fn decode_structured_values(row: &Row<'_>, columns: &[ColumnDef]) -> Result<Vec<Value>> {
    decode_structured_values_with_offset(row, columns, 0)
}

fn decode_structured_values_with_offset(
    row: &Row<'_>,
    columns: &[ColumnDef],
    offset: usize,
) -> Result<Vec<Value>> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let sql_value: SqlValue = row.get(idx + offset).map_err(map_sql_err)?;
            decode_sql_value(sql_value, &column.data_type)
        })
        .collect()
}

fn decode_sql_value(value: SqlValue, data_type: &DataType) -> Result<Value> {
    match value {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Integer(v) => match data_type {
            DataType::Boolean => Ok(Value::Bool(v != 0)),
            DataType::Int8 => Ok(Value::I8(v as i8)),
            DataType::Int16 => Ok(Value::I16(v as i16)),
            DataType::Int32 => Ok(Value::I32(v as i32)),
            DataType::Int => Ok(Value::I64(v)),
            DataType::Time => Ok(Value::Time(
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(v as u32, 0)
                    .unwrap_or_default(),
            )),
            DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32
            | DataType::Uint64
            | DataType::Uint128
            | DataType::Int128 => Value::I64(v).cast(data_type),
            other => Err(GlueError::StorageMsg(format!(
                "cannot decode INTEGER value for {other:?}"
            ))),
        },
        SqlValue::Real(v) => match data_type {
            DataType::Float32 => Ok(Value::F32(v as f32)),
            DataType::Float => Ok(Value::F64(v)),
            other => Err(GlueError::StorageMsg(format!(
                "cannot decode REAL value for {other:?}"
            ))),
        },
        SqlValue::Text(text) => match data_type {
            DataType::Int128
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32
            | DataType::Uint64
            | DataType::Uint128
            | DataType::Uuid
            | DataType::Inet
            | DataType::Decimal
            | DataType::Interval
            | DataType::Date
            | DataType::Timestamp
            | DataType::Time
            | DataType::Map
            | DataType::List
            | DataType::Point => Value::Str(text).cast(data_type),
            _ => Ok(Value::Str(text)),
        },
        SqlValue::Blob(blob) => match data_type {
            DataType::Bytea => Ok(Value::Bytea(blob)),
            other => Err(GlueError::StorageMsg(format!(
                "cannot decode BLOB value for {other:?}"
            ))),
        },
    }
}

pub(crate) fn encode_value(value: Value) -> Result<SqlValue> {
    let result = match value {
        Value::Null => SqlValue::Null,
        Value::Bool(v) => SqlValue::Integer(i64::from(v)),
        Value::I8(v) => SqlValue::Integer(i64::from(v)),
        Value::I16(v) => SqlValue::Integer(i64::from(v)),
        Value::I32(v) => SqlValue::Integer(i64::from(v)),
        Value::I64(v) => SqlValue::Integer(v),
        Value::I128(v) => SqlValue::Text(v.to_string()),
        Value::U8(v) => SqlValue::Integer(i64::from(v)),
        Value::U16(v) => SqlValue::Integer(i64::from(v)),
        Value::U32(v) => SqlValue::Integer(i64::from(v)),
        Value::U64(v) => SqlValue::Text(v.to_string()),
        Value::U128(v) => SqlValue::Text(v.to_string()),
        Value::F32(v) => SqlValue::Real(f64::from(v)),
        Value::F64(v) => SqlValue::Real(v),
        Value::Decimal(v) => SqlValue::Text(v.to_string()),
        Value::Str(v) => SqlValue::Text(v),
        Value::Bytea(v) => SqlValue::Blob(v),
        Value::Inet(v) => SqlValue::Text(v.to_string()),
        Value::Date(v) => SqlValue::Text(v.to_string()),
        Value::Timestamp(v) => SqlValue::Text(v.to_string()),
        Value::Time(v) => SqlValue::Text(v.to_string()),
        Value::Interval(v) => SqlValue::Text(v.to_sql_str()),
        Value::Uuid(v) => SqlValue::Text(Uuid::from_u128(v).hyphenated().to_string()),
        Value::Map(map) => {
            let json: JsonValue = Value::Map(map).try_into()?;
            SqlValue::Text(json.to_string())
        }
        Value::List(list) => {
            let json: JsonValue = Value::List(list).try_into()?;
            SqlValue::Text(json.to_string())
        }
        Value::Point(point) => SqlValue::Text(point.to_string()),
    };

    Ok(result)
}
