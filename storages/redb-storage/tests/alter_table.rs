#[path = "migration/database.rs"]
pub mod database;

use {
    database::TestFile,
    gluesql_core::{
        data::{Key, Value},
        parse_sql::parse_expr,
        prelude::{Error, Glue},
        store::{AlterTable, Store, StoreMut, Transaction},
        translate::translate_expr,
    },
    gluesql_redb_storage::RedbStorage,
};

#[test]
fn failed_drop_column_keeps_schema_and_previous_rows() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("CREATE TABLE Items (n INT)").unwrap();
    let schema = glue.storage.fetch_schema("Items").unwrap();
    glue.storage.begin(false).unwrap();
    glue.storage
        .insert_data(
            "Items",
            vec![(Key::I64(1), vec![Value::I64(10)]), (Key::I64(2), vec![])],
        )
        .unwrap();

    assert_eq!(
        glue.storage.drop_column("Items", "n", false),
        Err(Error::StorageMsg(
            "conflict - row has no column at position 0".into()
        ))
    );
    assert_eq!(glue.storage.fetch_schema("Items").unwrap(), schema);
    assert_eq!(
        glue.storage.fetch_data("Items", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(10)])
    );
    assert_eq!(
        glue.storage.fetch_data("Items", &Key::I64(2)).unwrap(),
        Some(vec![])
    );
    glue.storage.rollback().unwrap();
}

#[test]
fn failed_rename_column_keeps_the_stored_index_definition() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("CREATE TABLE Items (n INT); CREATE INDEX by_n ON Items (n)")
        .unwrap();
    let mut schema = glue.storage.fetch_schema("Items").unwrap().unwrap();
    // Store an unsupported expression after creating a valid index.
    schema.indexes[0].expr = translate_expr(&parse_expr("n + ABS(n)").unwrap(), &[]).unwrap();
    glue.storage.begin(false).unwrap();
    glue.storage.insert_schema(&schema).unwrap();

    assert_eq!(
        glue.storage.rename_column("Items", "n", "renamed"),
        Err(Error::StorageMsg(
            "conflict - unsupported stored index expression during column rename".into()
        ))
    );
    assert_eq!(glue.storage.fetch_schema("Items").unwrap(), Some(schema));
    glue.storage.rollback().unwrap();
}
