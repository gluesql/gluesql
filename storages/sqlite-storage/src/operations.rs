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

#[cfg(test)]
mod tests {
    use {
        super::*,
        gluesql_core::{
            ast::{ColumnDef, ColumnUniqueOption, DataType},
            data::Schema,
            error::Error as GlueError,
            store::{DataRow, Store, StoreMut},
        },
    };

    fn structured_schema(table_name: &str, with_primary: bool) -> Schema {
        Schema {
            table_name: table_name.to_owned(),
            column_defs: Some(vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: with_primary.then_some(ColumnUniqueOption { is_primary: true }),
                    comment: None,
                },
                ColumnDef {
                    name: "value".to_owned(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: None,
                    unique: None,
                    comment: None,
                },
            ]),
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        }
    }

    #[tokio::test]
    async fn structured_insert_column_count_mismatch() {
        let mut storage = SqliteStorage::memory().await.expect("memory storage");
        let schema = structured_schema("mismatch_case", true);
        storage
            .insert_schema(&schema)
            .await
            .expect("insert schema for mismatch");

        let err = insert_structured(
            &storage,
            "mismatch_case",
            &schema,
            vec![Value::I64(1)], // missing second column
        )
        .await
        .expect_err("column count mismatch should error");

        assert!(matches!(
            err,
            GlueError::StorageMsg(msg) if msg == "column count does not match values"
        ));
    }

    #[tokio::test]
    async fn insert_structured_with_key_without_primary_branch() {
        let mut storage = SqliteStorage::memory().await.expect("memory storage");
        let schema = structured_schema("rowid_table", false);
        storage
            .insert_schema(&schema)
            .await
            .expect("insert schema without primary");

        insert_structured_with_key(
            &storage,
            "rowid_table",
            &schema,
            Key::I64(999),
            vec![Value::I64(10), Value::Str("hello".to_owned())],
        )
        .await
        .expect("rowid insert succeeds without primary key");

        let fetched = storage
            .fetch_data("rowid_table", &Key::I64(1))
            .await
            .expect("fetch row")
            .expect("row present");

        assert_eq!(
            fetched,
            DataRow::Vec(vec![Value::I64(10), Value::Str("hello".to_owned())])
        );
    }
}
