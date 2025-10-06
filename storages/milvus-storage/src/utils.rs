use gluesql_core::{
    ast::ColumnDef,
    data::{Key, Schema},
    error::{Error, Result}
};

pub fn get_primary_key(column_defs: &[ColumnDef]) -> Option<&ColumnDef> {
    column_defs
        .iter()
        .find(|column_def| column_def.unique.as_ref().map(|x| x.is_primary).unwrap_or(false))
}

pub fn key_to_milvus_expression(key: &Key, pk_field_name: &str) -> Result<String> {
    match key {
        Key::None => Err(Error::StorageMsg("Cannot query with None key".to_string())),
        Key::I8(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::I16(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::I32(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::I64(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::I128(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::U8(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::U16(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::U32(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::U64(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::U128(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::F32(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::F64(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::Bool(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::Str(v) => Ok(format!("{} == \"{}\"", pk_field_name, v.replace("\"", "\\\""))),
        Key::Decimal(v) => Ok(format!("{} == {}", pk_field_name, v)),
        Key::Uuid(v) => Ok(format!("{} == \"{}\"", pk_field_name, v)),
        Key::Bytea(v) => Err(Error::StorageMsg(format!(
            "Bytea key type not supported for Milvus query: {:?}", v
        ))),
        Key::Date(v) => Err(Error::StorageMsg(format!(
            "Date key type not supported for Milvus query: {:?}", v
        ))),
        Key::Timestamp(v) => Err(Error::StorageMsg(format!(
            "Timestamp key type not supported for Milvus query: {:?}", v
        ))),
        Key::Time(v) => Err(Error::StorageMsg(format!(
            "Time key type not supported for Milvus query: {:?}", v
        ))),
        Key::Interval(v) => Err(Error::StorageMsg(format!(
            "Interval key type not supported for Milvus query: {:?}", v
        ))),
        Key::Inet(v) => Err(Error::StorageMsg(format!(
            "Inet key type not supported for Milvus query: {:?}", v
        ))),
    }
}
