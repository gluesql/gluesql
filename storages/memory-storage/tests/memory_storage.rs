use {
    gluesql_core::{
        prelude::{Glue, Key, Value},
        store::{Store, StoreMut},
    },
    gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

impl Tester<MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, MemoryTester);

generate_alter_table_tests!(test, MemoryTester);

generate_metadata_table_tests!(test, MemoryTester);

generate_custom_function_tests!(test, MemoryTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).unwrap();
    };
}

macro_rules! test {
    ($glue: ident $sql: expr, $result: expr) => {
        assert_eq!($glue.execute($sql), $result);
    };
}

#[test]
fn memory_storage_index() {
    use gluesql_core::{
        prelude::{Error, Glue},
        store::{Index, ScanColumns, Store},
    };

    let storage = MemoryStorage::default();

    assert_eq!(
        Store::scan_data(&storage, "Idx", ScanColumns::All)
            .unwrap()
            .collect::<gluesql_core::prelude::Result<Vec<_>>>()
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        storage
            .scan_indexed_data("Idx", "hello", None, None)
            .map(|_| ()),
        Err(Error::StorageMsg(
            "[MemoryStorage] index is not supported".to_owned()
        ))
    );

    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Idx (id INTEGER);");
    test!(
        glue "CREATE INDEX idx_id ON Idx (id);",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
    test!(
        glue "CREATE INDEX idx_constant ON Idx (100);",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
    test!(
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
}

#[test]
fn memory_storage_transaction() {
    use gluesql_core::prelude::{Error, Glue, Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Ok(vec![Payload::Commit]));
    test!(glue "ROLLBACK", Ok(vec![Payload::Rollback]));
}

#[test]
fn memory_storage_native_query() {
    use gluesql_core::{
        prelude::Error,
        query_builder::{Build, native_query},
    };

    let mut glue = Glue::new(MemoryStorage::default());
    let statement = native_query("SELECT 1")
        .columns([("n", "INT")])
        .alias_as("native")
        .select()
        .build()
        .unwrap();
    let actual = glue.execute_stmt(statement);
    let expected = Err(Error::StorageMsg(
        "[Storage] NativeQuery::query_native is not supported".to_owned(),
    ));
    assert_eq!(actual, expected);
}

#[test]
fn schemaless_update_conflict_on_non_map_row() {
    use gluesql_core::{
        data::Value,
        error::UpdateError,
        prelude::{Error, Glue},
        store::StoreMut,
    };

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Logs;");
    glue.storage
        .append_data("Logs", vec![vec![Value::I64(1)]])
        .unwrap();

    test!(
        glue "UPDATE Logs SET id = 2;",
        Err(Error::Update(UpdateError::ConflictOnNonMapSchemalessRow))
    );
}

#[test]
fn memory_storage_dynamic_metadata() {
    use gluesql_core::{data::Value, prelude::Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Restaurant (id INTEGER);");

    let metadata = glue.storage.metadata.get_mut("Restaurant").unwrap();
    metadata.insert("FOOD".to_owned(), Value::Str("Pizza".to_owned()));
    let created = metadata.get("CREATED").unwrap().clone();

    test!(
        glue "SELECT * FROM GLUE_OBJECTS;",
        Ok(vec![Payload::Select {
            labels: vec![
                "OBJECT_NAME".to_owned(),
                "OBJECT_TYPE".to_owned(),
                "CREATED".to_owned(),
                "FOOD".to_owned(),
            ],
            rows: vec![vec![
                Value::Str("Restaurant".to_owned()),
                Value::Str("TABLE".to_owned()),
                created,
                Value::Str("Pizza".to_owned()),
            ]],
        }])
    );
}

#[test]
fn memory_storage_dynamic_metadata_union() {
    use gluesql_core::{data::Value, prelude::Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Cafe (id INTEGER);");
    exec!(glue "CREATE TABLE Restaurant (id INTEGER);");

    glue.storage
        .metadata
        .get_mut("Cafe")
        .unwrap()
        .insert("DRINK".to_owned(), Value::Str("Coffee".to_owned()));

    glue.storage
        .metadata
        .get_mut("Restaurant")
        .unwrap()
        .insert("FOOD".to_owned(), Value::Str("Pizza".to_owned()));

    let cafe_created = glue.storage.metadata["Cafe"]["CREATED"].clone();
    let restaurant_created = glue.storage.metadata["Restaurant"]["CREATED"].clone();

    test!(
        glue "SELECT * FROM GLUE_OBJECTS;",
        Ok(vec![Payload::Select {
            labels: vec![
                "OBJECT_NAME".to_owned(),
                "OBJECT_TYPE".to_owned(),
                "CREATED".to_owned(),
                "DRINK".to_owned(),
                "FOOD".to_owned(),
            ],
            rows: vec![
                vec![
                    Value::Str("Cafe".to_owned()),
                    Value::Str("TABLE".to_owned()),
                    cafe_created,
                    Value::Str("Coffee".to_owned()),
                    Value::Null,
                ],
                vec![
                    Value::Str("Restaurant".to_owned()),
                    Value::Str("TABLE".to_owned()),
                    restaurant_created,
                    Value::Null,
                    Value::Str("Pizza".to_owned()),
                ],
            ],
        }])
    );
}

fn float_key_bits(key: &Key) -> u64 {
    match key {
        Key::F32(value) => u64::from(value.0.to_bits()),
        Key::F64(value) => value.0.to_bits(),
        other => panic!("expected a float key, got {other:?}"),
    }
}

fn storage_with_float_key(data_type: &str, key: Key, row: Vec<Value>) -> Glue<MemoryStorage> {
    let mut glue = Glue::new(MemoryStorage::default());
    glue.execute(format!("CREATE TABLE Zero (id {data_type}, tag TEXT)"))
        .unwrap();
    glue.storage.insert_data("Zero", vec![(key, row)]).unwrap();
    glue
}

#[test]
fn float_key_fetch_data_by_equal_key() {
    for (data_type, negative_key, negative_value, positive_key, negative_bits) in [
        (
            "FLOAT32",
            Key::F32((-0.0_f32).into()),
            Value::F32(-0.0_f32),
            Key::F32(0.0_f32.into()),
            0x8000_0000_u64,
        ),
        (
            "FLOAT",
            Key::F64((-0.0_f64).into()),
            Value::F64(-0.0_f64),
            Key::F64(0.0_f64.into()),
            0x8000_0000_0000_0000_u64,
        ),
    ] {
        let original = vec![negative_value, Value::Str("neg".to_owned())];
        let glue = storage_with_float_key(data_type, negative_key, original.clone());
        let rows = &glue.storage.items["Zero"].rows;
        assert_eq!(rows.len(), 1, "{data_type}");
        let (stored_key, stored_row) = rows.first_key_value().unwrap();
        assert_eq!(float_key_bits(stored_key), negative_bits, "{data_type}");
        let stored_value_bits = match &stored_row[0] {
            Value::F32(value) => u64::from(value.to_bits()),
            Value::F64(value) => value.to_bits(),
            other => panic!("expected a float value, got {other:?}"),
        };
        assert_eq!(stored_value_bits, negative_bits, "{data_type}");
        assert_eq!(
            glue.storage.fetch_data("Zero", &positive_key).unwrap(),
            Some(original),
            "{data_type}"
        );
    }
}

#[test]
fn float_key_insert_data_replaces_equal_key() {
    for (data_type, negative_key, negative_value, positive_key, positive_value) in [
        (
            "FLOAT32",
            Key::F32((-0.0_f32).into()),
            Value::F32(-0.0_f32),
            Key::F32(0.0_f32.into()),
            Value::F32(0.0_f32),
        ),
        (
            "FLOAT",
            Key::F64((-0.0_f64).into()),
            Value::F64(-0.0_f64),
            Key::F64(0.0_f64.into()),
            Value::F64(0.0_f64),
        ),
    ] {
        let original = vec![negative_value, Value::Str("neg".to_owned())];
        let mut glue = storage_with_float_key(data_type, negative_key.clone(), original);
        let replacement = vec![positive_value, Value::Str("pos".to_owned())];
        glue.storage
            .insert_data("Zero", vec![(positive_key.clone(), replacement.clone())])
            .unwrap();

        assert_eq!(glue.storage.items["Zero"].rows.len(), 1, "{data_type}");
        assert_eq!(
            glue.storage.fetch_data("Zero", &negative_key).unwrap(),
            Some(replacement.clone()),
            "{data_type}"
        );
        assert_eq!(
            glue.storage.fetch_data("Zero", &positive_key).unwrap(),
            Some(replacement),
            "{data_type}"
        );
    }
}
