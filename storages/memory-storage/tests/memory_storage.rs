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
    use gluesql_core::prelude::{Error, Glue, Payload};

    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE TxTest (id INTEGER);");
    test!(glue "BEGIN", Err(Error::StorageMsg("[MemoryStorage] transaction is not supported".to_owned())));
    test!(glue "COMMIT", Ok(vec![Payload::Commit]));
    test!(glue "ROLLBACK", Ok(vec![Payload::Rollback]));
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
fn unplanned_right_outer_join_is_rejected_at_execution() {
    use gluesql_core::{
        executor::QueryError,
        parse_sql::parse,
        plan::StatementPlan,
        prelude::{Error, Glue},
        translate::translate,
    };

    let mut glue = Glue::new(MemoryStorage::default());

    exec!(glue "CREATE TABLE Base (id INTEGER);");
    exec!(glue "CREATE TABLE Side (base_id INTEGER);");

    // Stands in for a custom `Planner` whose pipeline leaves out `plan_right_outer_join`.
    let sql = "SELECT * FROM Base RIGHT JOIN Side ON Side.base_id = Base.id";
    let parsed = parse(sql).unwrap().into_iter().next().unwrap();
    let unplanned = StatementPlan::from(translate(&parsed).unwrap());

    assert_eq!(
        glue.execute_stmt(&unplanned),
        Err(Error::Query(QueryError::UnreachableUnplannedRightOuterJoin)),
    );

    // The same statement runs once the planner has lowered it.
    assert!(glue.execute(sql).is_ok());
}

// `RightOuterJoinInputPlan::Hash` is only reachable by building the plan directly: the hash join
// planner leaves a RIGHT JOIN's own `ON` as a nested loop. A right row must stay available for the
// unmatched pass even when it never enters the hash index — NULL key, or excluded by `right_filter`.
#[test]
fn right_outer_hash_lookup_keeps_every_right_row_for_the_unmatched_pass() {
    use gluesql_core::{
        parse_sql::parse_expr,
        plan::{
            ExprPlan, HashJoinInputPlan, HashJoinPlan, NullExtendPlan, ProjectInputPlan,
            ProjectPlan, ProjectionPlan, QueryPlan, RightOuterJoinInputPlan, RightOuterJoinPlan,
            SelectItemPlan, SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
        },
        prelude::{Glue, Payload, Value},
        translate::translate_expr,
    };

    fn expr(sql: &str) -> ExprPlan {
        ExprPlan::from(translate_expr(&parse_expr(sql).unwrap(), &[]).unwrap())
    }

    fn table(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScanRequired,
        })
    }

    fn select_item(sql: &str, label: &str) -> SelectItemPlan {
        SelectItemPlan::Expr {
            expr: expr(sql),
            label: label.to_owned(),
        }
    }

    let mut glue = Glue::new(MemoryStorage::default());
    exec!(glue "CREATE TABLE Base (id INTEGER);");
    exec!(glue "INSERT INTO Base VALUES (1), (2);");
    exec!(glue "CREATE TABLE Side (base_id INTEGER NULL, label TEXT);");
    exec!(glue "INSERT INTO Side VALUES (1, 'matched'), (NULL, 'null key'), (2, 'filtered out'), (9, 'no match');");

    let statement = StatementPlan::Query(QueryPlan::Project(ProjectPlan {
        input: ProjectInputPlan::RightOuterJoin(Box::new(RightOuterJoinPlan {
            input: RightOuterJoinInputPlan::Hash(HashJoinPlan {
                input: HashJoinInputPlan::Source(table("Base")),
                right: table("Side"),
                input_key: expr("Base.id"),
                right_key: expr("Side.base_id"),
                right_filter: Some(expr("Side.label != 'filtered out'")),
            }),
            null_extend: NullExtendPlan {
                relations: vec!["Base".to_owned()],
            },
        })),
        projection: ProjectionPlan::SelectItems(vec![
            select_item("Base.id", "base_id"),
            select_item("Side.label", "label"),
        ]),
    }));

    let expected = Payload::Select {
        labels: vec!["base_id".to_owned(), "label".to_owned()],
        rows: vec![
            vec![Value::I64(1), Value::Str("matched".to_owned())],
            vec![Value::Null, Value::Str("null key".to_owned())],
            vec![Value::Null, Value::Str("filtered out".to_owned())],
            vec![Value::Null, Value::Str("no match".to_owned())],
        ],
    };

    assert_eq!(glue.execute_stmt(&statement), Ok(expected));
}

// Match state is tracked by row position, not by row value, so two identical unmatched right rows
// must both survive instead of collapsing into one.
#[test]
fn right_outer_join_preserves_duplicate_unmatched_rows() {
    use gluesql_core::prelude::{Glue, Payload, Value};

    let mut glue = Glue::new(MemoryStorage::default());
    exec!(glue "CREATE TABLE Base (id INTEGER);");
    exec!(glue "INSERT INTO Base VALUES (1);");
    exec!(glue "CREATE TABLE Side (base_id INTEGER);");
    exec!(glue "INSERT INTO Side VALUES (9), (9), (1), (1);");

    let expected = Payload::Select {
        labels: vec!["id".to_owned(), "base_id".to_owned()],
        rows: vec![
            vec![Value::I64(1), Value::I64(1)],
            vec![Value::I64(1), Value::I64(1)],
            vec![Value::Null, Value::I64(9)],
            vec![Value::Null, Value::I64(9)],
        ],
    };

    assert_eq!(
        glue.execute(
            "SELECT Base.id, Side.base_id FROM Base RIGHT JOIN Side ON Side.base_id = Base.id"
        ),
        Ok(vec![expected])
    );
}
