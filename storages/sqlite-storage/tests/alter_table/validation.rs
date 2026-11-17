use {
    super::column,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{AlterTableError, Error},
        store::{AlterTable, Store, StoreMut},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    std::collections::BTreeMap,
};

#[test]
fn direct_alter_calls_preserve_missing_table_errors() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let expected = Err(Error::AlterTable(AlterTableError::TableNotFound(
        "absent".to_owned(),
    )));

    assert_eq!(storage.rename_schema("absent", "renamed"), expected);
    assert_eq!(storage.rename_column("absent", "id", "renamed"), expected);
    assert_eq!(
        storage.add_column("absent", &column("id INT"), Value::Null),
        expected
    );
    assert_eq!(storage.drop_column("absent", "id", true), expected);
}

#[test]
fn schemaless_column_changes_leave_documents_unchanged() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let schema = Schema::from_ddl("CREATE TABLE documents").unwrap();
    storage.insert_schema(&schema).unwrap();
    let document = Value::Map(BTreeMap::from([("id".to_owned(), Value::I64(10))]));
    storage
        .append_data("documents", vec![vec![document.clone()]])
        .unwrap();
    let expected = Err(Error::AlterTable(AlterTableError::SchemalessTableFound(
        "documents".to_owned(),
    )));

    let actual = storage.add_column("documents", &column("label TEXT"), Value::Null);
    assert_eq!(actual, expected);

    let actual = storage.rename_column("documents", "id", "label");
    assert_eq!(actual, expected);

    let actual = storage.drop_column("documents", "label", true);
    assert_eq!(actual, expected);

    assert_eq!(storage.fetch_schema("documents").unwrap(), Some(schema));
    let actual = storage.fetch_data("documents", &Key::I64(1)).unwrap();
    let expected = Some(vec![document]);
    assert_eq!(actual, expected);
}

#[test]
fn unsupported_layout_changes_leave_the_table_unchanged() {
    let mut storage = SqliteStorage::in_memory().unwrap();
    let schema = Schema::from_ddl("CREATE TABLE item (_rowid_ INT, rowid INT, value INT)").unwrap();
    storage.insert_schema(&schema).unwrap();
    let row = vec![Value::I64(10), Value::I64(20), Value::I64(30)];
    storage.append_data("item", vec![row.clone()]).unwrap();

    // Neither adding nor renaming a column may hide the final rowid alias.
    let actual = storage.add_column("item", &column("oid INT"), Value::Null);
    let expected = Err(SqliteStorageError::RowidUnavailable("item".to_owned()).into());
    assert_eq!(actual, expected);

    let actual = storage.rename_column("item", "value", "oid");
    assert_eq!(actual, expected);

    let actual = storage.rename_schema("item", "__GLUESQL_TABLE_META");
    let expected =
        Err(SqliteStorageError::ReservedTableName("__GLUESQL_TABLE_META".to_owned()).into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("item").unwrap(), Some(schema));
    assert_eq!(storage.fetch_data("item", &Key::I64(1)).unwrap(), Some(row));

    // Removing the last column would require a different physical table layout.
    let schema = Schema::from_ddl("CREATE TABLE last_column (id INT)").unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .append_data("last_column", vec![vec![Value::I64(10)]])
        .unwrap();

    let actual = storage.drop_column("last_column", "id", false);
    let expected = Err(SqliteStorageError::EmptySchema("last_column".to_owned()).into());
    assert_eq!(actual, expected);
    assert_eq!(storage.fetch_schema("last_column").unwrap(), Some(schema));
    assert_eq!(
        storage.fetch_data("last_column", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(10)])
    );
}
