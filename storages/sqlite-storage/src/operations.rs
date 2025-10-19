use {
    crate::{SqliteStorage, codec::encode_value, schema::primary_column},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error as GlueError, Result},
    },
    rusqlite::{params_from_iter, types::Value as SqlValue},
    serde_json::Value as JsonValue,
    std::{collections::BTreeMap, convert::TryFrom, convert::TryInto},
};

pub(crate) async fn insert_structured(
    storage: &SqliteStorage,
    table_name: &str,
    schema: &Schema,
    values: Vec<Value>,
) -> Result<()> {
    insert_structured_internal(storage, table_name, schema, values).await
}

pub(crate) async fn insert_structured_with_key(
    storage: &SqliteStorage,
    table_name: &str,
    schema: &Schema,
    key: Key,
    values: Vec<Value>,
) -> Result<()> {
    if let Some(primary) = primary_column(schema) {
        let columns = schema.column_defs.as_ref().unwrap();
        if let Some(index) = columns.iter().position(|col| col.name == primary) {
            let candidate = Key::try_from(values[index].clone())?;
            if candidate != key {
                return Err(GlueError::StorageMsg(
                    "primary key value does not match provided key".to_owned(),
                ));
            }
        }
    }

    insert_structured_internal(storage, table_name, schema, values).await
}

async fn insert_structured_internal(
    storage: &SqliteStorage,
    table_name: &str,
    schema: &Schema,
    values: Vec<Value>,
) -> Result<()> {
    let columns = schema.column_defs.as_ref().ok_or_else(|| {
        GlueError::StorageMsg("schemaless row supplied to structured table".into())
    })?;

    if columns.len() != values.len() {
        return Err(GlueError::StorageMsg(
            "column count does not match values".to_owned(),
        ));
    }

    let column_list = columns
        .iter()
        .map(|col| format!(r#""{}""#, SqliteStorage::escape_identifier(&col.name)))
        .collect::<Vec<_>>()
        .join(", ");

    let params = values
        .into_iter()
        .map(encode_value)
        .collect::<Result<Vec<_>>>()?;

    let placeholders = std::iter::repeat_n("?", columns.len())
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        r#"INSERT OR REPLACE INTO "{}" ({}) VALUES ({})"#,
        SqliteStorage::escape_identifier(table_name),
        column_list,
        placeholders
    );

    storage
        .with_conn(move |conn| {
            conn.execute(sql.as_str(), params_from_iter(params.iter()))
                .map(|_| ())
        })
        .await
}

pub(crate) async fn insert_schemaless(
    storage: &SqliteStorage,
    table_name: &str,
    map: BTreeMap<String, Value>,
) -> Result<()> {
    let json: JsonValue = Value::Map(map).try_into()?;
    let payload = SqlValue::Text(json.to_string());

    let sql = format!(
        r#"INSERT INTO "{}" ("_gluesql_payload") VALUES (?)"#,
        SqliteStorage::escape_identifier(table_name)
    );

    storage
        .with_conn(move |conn| {
            conn.execute(sql.as_str(), params_from_iter([payload].iter()))
                .map(|_| ())
        })
        .await
}

pub(crate) async fn update_structured_row(
    storage: &SqliteStorage,
    table_name: &str,
    schema: &Schema,
    key: Key,
    values: Vec<Value>,
) -> Result<()> {
    let rowid = match key {
        Key::I64(v) => v,
        _ => {
            return Err(GlueError::StorageMsg(
                "rowid key must be an i64 value".to_owned(),
            ));
        }
    };

    let columns = schema.column_defs.as_ref().ok_or_else(|| {
        GlueError::StorageMsg("schemaless row supplied to structured table".into())
    })?;

    if columns.len() != values.len() {
        return Err(GlueError::StorageMsg(
            "column count does not match values".to_owned(),
        ));
    }

    let assignments = columns
        .iter()
        .map(|column| {
            format!(
                r#""{}" = ?"#,
                SqliteStorage::escape_identifier(&column.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        r#"UPDATE "{}" SET {} WHERE rowid = ?"#,
        SqliteStorage::escape_identifier(table_name),
        assignments
    );

    let mut params = values
        .into_iter()
        .map(encode_value)
        .collect::<Result<Vec<_>>>()?;
    params.push(SqlValue::Integer(rowid));

    storage
        .with_conn(move |conn| {
            conn.execute(sql.as_str(), params_from_iter(params.iter()))
                .map(|_| ())
        })
        .await
}

pub(crate) async fn update_schemaless_row(
    storage: &SqliteStorage,
    table_name: &str,
    key: Key,
    map: BTreeMap<String, Value>,
) -> Result<()> {
    let rowid = match key {
        Key::I64(v) => v,
        _ => {
            return Err(GlueError::StorageMsg(
                "rowid key must be an i64 value".to_owned(),
            ));
        }
    };

    let json: JsonValue = Value::Map(map).try_into()?;
    let payload = SqlValue::Text(json.to_string());

    let sql = format!(
        r#"UPDATE "{}" SET "_gluesql_payload" = ? WHERE rowid = ?"#,
        SqliteStorage::escape_identifier(table_name)
    );

    storage
        .with_conn(move |conn| {
            conn.execute(
                sql.as_str(),
                params_from_iter([payload, SqlValue::Integer(rowid)].iter()),
            )
            .map(|_| ())
        })
        .await
}
