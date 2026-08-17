use {gluesql_core::prelude::Glue, gluesql_memory_storage::MemoryStorage, test_suite::*};

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
        store::{Index, Store},
    };

    let storage = MemoryStorage::default();

    assert_eq!(
        Store::scan_data(&storage, "Idx")
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
        glue "DROP INDEX Idx.idx_id;",
        Err(Error::StorageMsg("[MemoryStorage] index is not supported".to_owned()))
    );
}

#[test]
fn memory_storage_transaction() {
    use gluesql_core::prelude::{Error, Glue};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "ROLLBACK", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
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
