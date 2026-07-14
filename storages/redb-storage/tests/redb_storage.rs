use {
    gluesql_core::{
        ast::{IndexOperator::Eq, OrderByExpr},
        data::Key,
        error::IndexError,
        prelude::{Glue, Value::I64},
        store::{IndexMut, StoreMut, Transaction},
    },
    gluesql_redb_storage::RedbStorage,
    std::fs::{create_dir, remove_file},
    test_suite::*,
};

struct RedbTester {
    glue: Glue<RedbStorage>,
}

impl Tester<RedbStorage> for RedbTester {
    fn new(namespace: &str) -> Self {
        let _ = create_dir("tmp");
        let path = format!("tmp/{namespace}");
        let _ = remove_file(&path);

        let storage = RedbStorage::new(path).expect("[RedbTester] failed to create storage");
        let glue = Glue::new(storage);

        Self { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<RedbStorage> {
        &mut self.glue
    }
}

impl RedbTester {
    fn from_path(path: &str) -> Self {
        let storage = RedbStorage::new(path).expect("[RedbTester] failed to open storage");
        let glue = Glue::new(storage);

        Self { glue }
    }
}

#[test]
fn index_persists_after_reopen() {
    let _ = create_dir("tmp");
    let path = "tmp/index_persists_after_reopen";
    let _ = remove_file(path);

    {
        let mut tester = RedbTester::from_path(path);
        tester.run("CREATE TABLE PersistedIndex (id INTEGER, value INTEGER);");
        tester.run("INSERT INTO PersistedIndex VALUES (1, 10), (2, 20), (3, 30);");
        tester.run("CREATE INDEX idx_value ON PersistedIndex (value);");
    }

    let mut tester = RedbTester::from_path(path);
    tester.test_idx(
        "SELECT id, value FROM PersistedIndex WHERE value = 20",
        Ok(select!(id | value I64 | I64; 2 20)),
        idx!(idx_value, Eq, "20"),
    );
}

#[test]
fn mutations_require_an_existing_schema() {
    let _ = create_dir("tmp");
    let path = "tmp/mutations_require_an_existing_schema";
    let _ = remove_file(path);
    let mut storage = RedbStorage::new(path).expect("create storage");
    storage.begin(false).expect("begin transaction");

    let index_column = OrderByExpr {
        expr: test_suite::expr("id"),
        asc: None,
    };
    let create_index_error = storage
        .create_index("Missing", "idx_missing", &index_column)
        .expect_err("create index should require a schema");
    assert_eq!(
        create_index_error,
        IndexError::TableNotFound("Missing".to_owned()).into()
    );

    let append_error = storage
        .append_data("Missing", Vec::new())
        .expect_err("append should require a schema");
    let insert_error = storage
        .insert_data("Missing", Vec::new())
        .expect_err("insert should require a schema");
    let delete_error = storage
        .delete_data("Missing", Vec::new())
        .expect_err("delete should require a schema");

    for error in [append_error, insert_error, delete_error] {
        assert!(
            error
                .to_string()
                .contains("conflict - table not found: Missing")
        );
    }

    storage
        .delete_schema("Missing")
        .expect("deleting a missing schema should be idempotent");
    storage.rollback().expect("rollback transaction");
}

#[test]
fn repeated_row_key_keeps_only_the_latest_index_entry() {
    let _ = create_dir("tmp");
    let path = "tmp/repeated_row_key_keeps_only_the_latest_index_entry";
    let _ = remove_file(path);
    let mut tester = RedbTester::from_path(path);
    tester.run("CREATE TABLE RepeatedKey (id INTEGER, value INTEGER);");
    tester.run("CREATE INDEX idx_value ON RepeatedKey (value);");

    tester.glue.storage.begin(false).expect("begin transaction");
    tester
        .glue
        .storage
        .insert_data(
            "RepeatedKey",
            vec![
                (Key::I64(1), vec![I64(1), I64(10)]),
                (Key::I64(1), vec![I64(1), I64(20)]),
            ],
        )
        .expect("insert repeated row key");
    tester.glue.storage.commit().expect("commit transaction");

    tester.test_idx(
        "SELECT id, value FROM RepeatedKey WHERE value = 10",
        Ok(select!(id | value)),
        idx!(idx_value, Eq, "10"),
    );
    tester.test_idx(
        "SELECT id, value FROM RepeatedKey WHERE value = 20",
        Ok(select!(id | value I64 | I64; 1 20)),
        idx!(idx_value, Eq, "20"),
    );
}

generate_store_tests!(test, RedbTester);
generate_index_tests!(test, RedbTester);
generate_transaction_tests!(test, RedbTester);
generate_alter_table_index_tests!(test, RedbTester);
generate_transaction_index_tests!(test, RedbTester);
generate_metadata_index_tests!(test, RedbTester);
