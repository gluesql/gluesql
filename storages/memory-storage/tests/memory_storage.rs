use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_core::{
        data::ValueError,
        executor::EvaluateError,
        prelude::{PayloadVariable, Value::*},
        translate::TranslateError,
    },
    gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

#[async_trait(?Send)]
impl Tester<MemoryStorage> for MemoryTester {
    async fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, MemoryTester);

generate_alter_table_tests!(tokio::test, MemoryTester);

generate_metadata_table_tests!(tokio::test, MemoryTester);

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
    use {
        futures::executor::block_on,
        gluesql_core::{
            prelude::Glue,
            result::{Error, Result},
            store::{Index, Store},
        },
    };

    let storage = MemoryStorage::default();

    assert_eq!(
        block_on(storage.scan_data("Idx"))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .as_ref()
            .map(Vec::len),
        Ok(0),
    );

    assert_eq!(
        block_on(storage.scan_indexed_data("Idx", "hello", None, None)).map(|_| ()),
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
    use gluesql_core::{
        prelude::{Glue, Payload},
        result::Error,
    };

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Ok(vec![Payload::Commit]));
    test!(glue "ROLLBACK", Ok(vec![Payload::Rollback]));
}

#[test]
fn memory_storage_function() {
    use gluesql_core::prelude::{Glue, Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    let test_cases = [
        ("CREATE FUNCTION add_none ()", Ok(vec![Payload::Create])),
        (
            "CREATE FUNCTION add_one (n INT, x INT DEFAULT 1) RETURN n + x",
            Ok(vec![Payload::Create]),
        ),
        (
            "CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT) RETURN n + x + y",
            Ok(vec![Payload::Create]),
        ),
        // (
        //     "SELECT add_none() AS r",
        //     Ok(vec![select_with_null!(r; Null)]),
        // ),
        (
            "SELECT add_one(1) AS r",
            Ok(vec![select!(
                r
                I64;
                2
            )]),
        ),
        (
            "SELECT add_one(1, 8) AS r",
            Ok(vec![select!(
                r
                I64;
                9
            )]),
        ),
        (
            "SELECT add_one(1, 2, 4)",
            Err(EvaluateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "SELECT add_one()",
            Err(EvaluateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT add_two(1, null, 2) as r",
            Ok(vec![select!(
                r
                I64;
                4
            )]),
        ),
        (
            "SELECT add_two(1, 2)",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
        (
            "DROP FUNCTION add_one, add_two",
            Ok(vec![Payload::DropFunction]),
        ),
        (
            "SHOW FUNCTIONS",
            Ok(vec![Payload::ShowVariable(PayloadVariable::Functions(
                vec!["add_none()".to_owned()],
            ))]),
        ),
        (
            "DROP FUNCTION IF EXISTS add_one, add_two, add_none",
            Ok(vec![Payload::DropFunction]),
        ),
        (
            "CREATE FUNCTION test(INT)",
            Err(TranslateError::UnNamedFunctionArgNotSupported.into()),
        ),
        (
            "CREATE TABLE test(a INT DEFAULT test())",
            Err(EvaluateError::UnsupportedCustomFunction.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(glue sql, expected);
    }
}
