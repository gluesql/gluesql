use {
    super::{expect_native_source, expect_native_source_under_filter, expect_query, select},
    gluesql_core::{
        data::Value,
        error::Result,
        prelude::{Glue, Payload},
        store::NativeQuery,
    },
    gluesql_sqlite_storage::SqliteStorage,
};

#[test]
fn native_filter_reduces_source_rows_and_keeps_common_projection() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT PRIMARY KEY, name TEXT, qty INT);
        INSERT INTO Item VALUES
            (3, 'PEAR', 20),
            (1, 'APPLE', 7),
            (2, 'LIME', 12);
        ",
    )
    .unwrap();

    let sql = "
        SELECT LOWER(i.name) AS label
        FROM Item i
        WHERE i.qty >= 10
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let source = expect_native_source(expect_query(&plan));
    let types = source
        .columns
        .iter()
        .map(|column| column.data_type.clone())
        .collect::<Vec<_>>();

    // SQLite returns only the matching rows, before the common LOWER projection.
    let actual = glue
        .storage
        .query_native(&source.query, &source.params, &types)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        vec![Value::Str("LIME".to_owned()), Value::I64(12)],
        vec![Value::Str("PEAR".to_owned()), Value::I64(20)],
    ];
    assert_eq!(actual, expected);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["label"],
        vec![
            vec![Value::Str("lime".to_owned())],
            vec![Value::Str("pear".to_owned())],
        ],
    );
    assert_eq!(actual, expected);
}

#[test]
fn partial_native_filter_reduces_rows_before_evaluating_the_remaining_condition() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT PRIMARY KEY, name TEXT, qty INT);
        INSERT INTO Item VALUES
            (1, 'APPLE', 7),
            (2, 'LIME', 12),
            (3, 'APPLE', 20),
            (4, NULL, 25),
            (5, 'APPLE', NULL);
        ",
    )
    .unwrap();

    let sql = "
        SELECT id
        FROM Item
        WHERE qty >= 10 AND LOWER(name) = 'apple'
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let source = expect_native_source_under_filter(expect_query(&plan));
    let types = source
        .columns
        .iter()
        .map(|column| column.data_type.clone())
        .collect::<Vec<_>>();

    // The native source filters qty and retains name for the remaining LOWER condition.
    let actual = glue
        .storage
        .query_native(&source.query, &source.params, &types)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![
        vec![Value::I64(2), Value::Str("LIME".to_owned()), Value::I64(12)],
        vec![
            Value::I64(3),
            Value::Str("APPLE".to_owned()),
            Value::I64(20),
        ],
        vec![Value::I64(4), Value::Null, Value::I64(25)],
    ];
    assert_eq!(actual, expected);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["id"], vec![vec![Value::I64(3)]]);
    assert_eq!(actual, expected);
}

#[test]
fn native_numeric_bindings_preserve_comparison_boundaries() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Numbers (id INT, n INT8, f FLOAT32, d FLOAT);
        INSERT INTO Numbers VALUES (1, 10, 0.1, 0.1), (2, 100, 0.5, 0.5);
        ",
    )
    .unwrap();

    let sql = "SELECT id FROM Numbers WHERE n < 100";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(1)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Numbers WHERE f <= 0.1";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(1)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Numbers WHERE d > 0.1";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(2)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Numbers WHERE f BETWEEN 0.1 AND 0.5";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert_eq!(actual, expected);
}

#[test]
fn native_nullable_conditions_preserve_three_valued_logic() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, enabled BOOLEAN, n INT);
        INSERT INTO Item VALUES (1, TRUE, NULL), (2, FALSE, 2), (3, NULL, 3);
        ",
    )
    .unwrap();

    let sql = "SELECT id FROM Item WHERE enabled OR n = 2";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE NOT (enabled AND n = 2)";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(2)], vec![Value::I64(3)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE NOT (n IN (2, NULL))";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE n NOT IN (2, NULL)";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE NOT (n BETWEEN NULL AND 0)";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(2)], vec![Value::I64(3)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE n NOT BETWEEN 4 AND NULL";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(2)], vec![Value::I64(3)]]);
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE (n > 1) IS NULL";
    let actual = execute_fully_delegated_filter(&mut glue, sql);
    let expected = select(&["id"], vec![vec![Value::I64(1)]]);
    assert_eq!(actual, expected);
}

fn execute_fully_delegated_filter(glue: &mut Glue<SqliteStorage>, sql: &str) -> Payload {
    let plan = glue.plan(sql).expect(sql).remove(0);
    expect_native_source(expect_query(&plan));

    glue.execute_prepared(&plan).expect(sql)
}
