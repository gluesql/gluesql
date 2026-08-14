use {
    gluesql_core::{
        data::{Schema, Value},
        prelude::Error,
        store::StoreMut,
    },
    gluesql_json_storage::{JsonStorage, error::JsonStorageError},
    std::fs::{create_dir_all, remove_dir_all},
};

// #1878: a schemaless row must be a single `Value::Map`. A non-Map row is an
// invalid shape and must return an error rather than silently persisting `{}`.
#[test]
fn schemaless_write_rejects_non_map_row() {
    let path = "./tests/schemaless_invalid_shape/";
    let _ = remove_dir_all(path);
    create_dir_all(path).unwrap();

    let mut storage = JsonStorage::new(path).unwrap();

    let schema = Schema {
        table_name: "Schemaless".to_owned(),
        column_defs: None,
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        comment: None,
    };
    storage.insert_schema(&schema).unwrap();

    let result = storage.append_data("Schemaless", vec![vec![Value::I64(1)]]);
    assert_eq!(
        result,
        Err(Error::StorageMsg(
            JsonStorageError::JsonObjectTypeRequired.to_string()
        ))
    );

    remove_dir_all(path).unwrap();
}
