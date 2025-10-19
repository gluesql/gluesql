use {
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption, DataType},
        data::{Key, Schema},
        error::{Error as GlueError, Result},
        prelude::Value,
        store::{DataRow, StoreMut, Transaction},
    },
    gluesql_sqlite_storage::SqliteStorage,
    std::collections::BTreeMap,
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

fn schemaless_schema(table_name: &str) -> Schema {
    Schema {
        table_name: table_name.to_owned(),
        column_defs: None,
        indexes: vec![],
        engine: None,
        foreign_keys: vec![],
        comment: None,
    }
}

#[tokio::test]
async fn column_count_mismatch() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = structured_schema("mismatch", true);
    storage.insert_schema(&schema).await?;

    let err = storage
        .append_data(
            "mismatch",
            vec![DataRow::Vec(vec![Value::I64(1)])], // missing second column
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "column count does not match values"
    ));

    Ok(())
}

#[tokio::test]
async fn primary_key_value_mismatch() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = structured_schema("pk_table", true);
    storage.insert_schema(&schema).await?;

    let err = storage
        .insert_data(
            "pk_table",
            vec![(
                Key::I64(1),
                DataRow::Vec(vec![Value::I64(2), Value::Str("alpha".to_owned())]),
            )],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "primary key value does not match provided key"
    ));

    Ok(())
}

#[tokio::test]
async fn rowid_key_type_mismatch() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = structured_schema("no_pk", false);
    storage.insert_schema(&schema).await?;

    storage
        .append_data(
            "no_pk",
            vec![DataRow::Vec(vec![
                Value::I64(1),
                Value::Str("row".to_owned()),
            ])],
        )
        .await?;

    let err = storage
        .delete_data("no_pk", vec![Key::Str("wrong".to_owned())])
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "rowid must be an i64 key"
    ));

    Ok(())
}

#[tokio::test]
async fn key_none_not_allowed() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = structured_schema("key_none", true);
    storage.insert_schema(&schema).await?;

    let err = storage
        .delete_data("key_none", vec![Key::None])
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "Key::None cannot be encoded as parameter"
    ));

    Ok(())
}

#[tokio::test]
async fn structured_ops_on_schemaless_table() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = schemaless_schema("logs");
    storage.insert_schema(&schema).await?;

    let err = storage
        .append_data(
            "logs",
            vec![DataRow::Vec(vec![
                Value::I64(1),
                Value::Str("invalid".to_owned()),
            ])],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "schemaless row supplied to structured table"
    ));

    Ok(())
}

#[tokio::test]
async fn transaction_double_begin() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;

    let first = storage.begin(false).await?;
    assert!(!first, "non-autocommit begin returns false");

    let err = storage.begin(false).await.unwrap_err();
    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "transaction already started"
    ));

    storage.rollback().await?;

    Ok(())
}
#[tokio::test]
async fn table_not_found_errors() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;

    let err = storage
        .append_data("missing_table", vec![DataRow::Vec(vec![Value::I64(1)])])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "table 'missing_table' not found"
    ));

    let err = storage
        .insert_data(
            "missing_table",
            vec![(Key::I64(1), DataRow::Vec(vec![Value::I64(1)]))],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "table 'missing_table' not found"
    ));

    let err = storage
        .delete_data("missing_table", vec![Key::I64(1)])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "table 'missing_table' not found"
    ));

    Ok(())
}

#[tokio::test]
async fn rowid_update_with_non_i64() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = structured_schema("no_pk_structured", false);
    storage.insert_schema(&schema).await?;

    storage
        .append_data(
            "no_pk_structured",
            vec![DataRow::Vec(vec![
                Value::I64(1),
                Value::Str("hello".to_owned()),
            ])],
        )
        .await?;

    let err = storage
        .insert_data(
            "no_pk_structured",
            vec![(
                Key::Str("A".to_owned()),
                DataRow::Vec(vec![Value::I64(2), Value::Str("world".to_owned())]),
            )],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "rowid key must be an i64 value"
    ));

    Ok(())
}

#[tokio::test]
async fn schemaless_update_with_non_i64_rowid() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    let schema = schemaless_schema("schemaless_updates");
    storage.insert_schema(&schema).await?;

    storage
        .append_data(
            "schemaless_updates",
            vec![DataRow::Map(BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Str("alpha".to_owned())),
            ]))],
        )
        .await?;

    let err = storage
        .insert_data(
            "schemaless_updates",
            vec![(
                Key::Str("invalid".to_owned()),
                DataRow::Map(BTreeMap::from([
                    ("id".to_owned(), Value::I64(1)),
                    ("name".to_owned(), Value::Str("beta".to_owned())),
                ])),
            )],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        GlueError::StorageMsg(msg) if msg == "rowid key must be an i64 value"
    ));

    Ok(())
}
