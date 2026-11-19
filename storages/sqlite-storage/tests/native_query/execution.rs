use {
    super::{expect_native_source, expect_native_source_under_filter, expect_query, select},
    gluesql_core::{
        data::Value,
        error::Error,
        plan::{AggregationInputPlan, FilterInputPlan, FilterPlan, ProjectInputPlan, SourcePlan},
        prelude::Glue,
        query_builder::{Build, native_query},
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Error as SqliteError, ffi},
};

#[test]
fn native_output_decodes_extended_types_with_the_existing_codec() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        r#"
        CREATE TABLE Item (id INT, doc MAP, items LIST);
        INSERT INTO Item VALUES (1, '{"qty": 2}', '[1, true]'), (2, NULL, NULL);
        "#,
    )
    .unwrap();

    let sql = "SELECT doc, items FROM Item WHERE id > 0 AND doc IS NOT NULL";
    let plan = glue.plan(sql).unwrap().remove(0);
    expect_native_source(expect_query(&plan));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["doc", "items"],
        vec![vec![
            Value::Map([("qty".to_owned(), Value::I64(2))].into()),
            Value::List(vec![Value::I64(1), Value::Bool(true)]),
        ]],
    );
    assert_eq!(actual, expected);
}

#[test]
fn bound_native_rows_feed_common_distinct_order_and_limit() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES
            (1, 'PLUM', 7),
            (2, 'BANANA', 12),
            (3, 'PEAR', 20),
            (4, 'LIME', 15),
            (5, 'pear', 30);
        ",
    )
    .unwrap();

    // Exclude plum, merge the two pear labels, sort, then keep two of three labels.
    let sql = "
        SELECT DISTINCT LOWER(name) AS label
        FROM Item
        WHERE qty >= $1
        ORDER BY label DESC
        LIMIT 2
    ";
    let plan = glue.plan_with_params(sql, [10_i64]).unwrap().remove(0);
    let actual = &expect_native_source(expect_query(&plan)).params;
    let expected = vec![Value::I64(10)];
    assert_eq!(actual, &expected);

    let actual = glue.execute_with_params(sql, [10_i64]).unwrap();
    let expected = vec![select(
        &["label"],
        vec![
            vec![Value::Str("pear".to_owned())],
            vec![Value::Str("lime".to_owned())],
        ],
    )];
    assert_eq!(actual, expected);
}

#[test]
fn native_filtered_rows_feed_common_aggregates() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES
            (1, 'APPLE', 7),
            (2, 'LIME', 12),
            (3, 'PEAR', 20);
        ",
    )
    .unwrap();

    let sql = "
        SELECT COUNT(*) AS count, SUM(qty) AS total
        FROM Item
        WHERE qty > 10
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let ProjectInputPlan::Aggregation(aggregation) = &expect_query(&plan).project().unwrap().input
    else {
        panic!("expected common aggregation: {plan:?}");
    };
    let AggregationInputPlan::Source(SourcePlan::Native(source)) = &aggregation.input else {
        panic!("expected native rows below the aggregate: {plan:?}");
    };
    let actual = source
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let expected = vec!["qty"];
    assert_eq!(actual, expected);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["count", "total"],
        vec![vec![Value::I64(2), Value::I64(32)]],
    );
    assert_eq!(actual, expected);

    // Replanning must keep both native output and the aggregate bindings valid.
    let actual = glue.execute_stmt(plan).unwrap();
    assert_eq!(actual, expected);

    let sql = "
        SELECT COUNT(*) AS count, SUM(qty) AS total
        FROM Item
        WHERE qty > 10 AND LOWER(name) = 'pear'
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let ProjectInputPlan::Aggregation(aggregation) = &expect_query(&plan).project().unwrap().input
    else {
        panic!("expected common aggregation: {plan:?}");
    };
    let AggregationInputPlan::Filter(FilterPlan {
        input: FilterInputPlan::Source(SourcePlan::Native(_)),
        ..
    }) = &aggregation.input
    else {
        panic!("expected the residual condition below the aggregate: {plan:?}");
    };

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["count", "total"],
        vec![vec![Value::I64(1), Value::I64(20)]],
    );
    assert_eq!(actual, expected);
}

#[test]
fn replanning_native_sources_preserves_common_processing() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT PRIMARY KEY, name TEXT, qty INT);
        INSERT INTO Item VALUES
            (1, 'APPLE', 7),
            (2, 'LIME', 12),
            (3, 'PEAR', 20);
        ",
    )
    .unwrap();

    // execute_stmt runs the planner a second time.
    let sql = "SELECT LOWER(name) AS label FROM Item WHERE qty >= 10";
    let plan = glue.plan(sql).unwrap().remove(0);
    expect_native_source(expect_query(&plan));

    let actual = glue.execute_stmt(plan).unwrap();
    let expected = select(
        &["label"],
        vec![
            vec![Value::Str("lime".to_owned())],
            vec![Value::Str("pear".to_owned())],
        ],
    );
    assert_eq!(actual, expected);

    let sql = "SELECT id FROM Item WHERE qty >= 10 AND LOWER(name) = 'pear'";
    let plan = glue.plan(sql).unwrap().remove(0);
    expect_native_source_under_filter(expect_query(&plan));

    let actual = glue.execute_stmt(plan).unwrap();
    let expected = select(&["id"], vec![vec![Value::I64(3)]]);
    assert_eq!(actual, expected);
}

#[test]
fn native_reads_use_the_current_transaction() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT);
        INSERT INTO Item VALUES (1);
        BEGIN;
        INSERT INTO Item VALUES (2);
        ",
    )
    .unwrap();

    let plan = glue
        .plan("SELECT id FROM Item WHERE id > 0")
        .unwrap()
        .remove(0);
    expect_native_source(expect_query(&plan));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["id"], vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    assert_eq!(actual, expected);

    glue.execute("ROLLBACK").unwrap();
    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["id"], vec![vec![Value::I64(1)]]);
    assert_eq!(actual, expected);
}

#[test]
fn prepared_native_query_reports_a_renamed_column() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT);
        INSERT INTO Item VALUES (1, 'Apple');
        ",
    )
    .unwrap();

    let plan = glue
        .plan("SELECT i.name FROM Item AS i WHERE i.id > 0")
        .unwrap()
        .remove(0);
    expect_native_source(expect_query(&plan));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["name"], vec![vec![Value::Str("Apple".to_owned())]]);
    assert_eq!(actual, expected);

    glue.execute("ALTER TABLE Item RENAME COLUMN name TO label")
        .unwrap();

    let error = glue.execute_prepared(&plan).unwrap_err();
    let Error::StorageMsg(message) = error else {
        panic!("expected a SQLite error, got {error:?}");
    };

    assert!(
        message.starts_with("no such column: Item.name"),
        "{message}"
    );
}

#[test]
fn native_query_reports_custom_function_planning_error() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());

    let statement = native_query("SELECT 1 AS qty")
        .columns([("qty", "INT")])
        .alias_as("native")
        .select()
        .project("missing(qty)")
        .build()
        .unwrap();
    let actual = glue.execute_stmt(statement);
    let expected = Err(Error::StorageMsg(
        "[Storage] CustomFunction is not supported".to_owned(),
    ));
    assert_eq!(actual, expected);
}

#[test]
fn native_execution_reports_missing_table() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    let statement = native_query("SELECT * FROM missing")
        .columns([("id", "INT")])
        .alias_as("native")
        .select()
        .build()
        .unwrap();
    let actual = glue.execute_stmt(statement);
    let expected = Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_ERROR),
        Some("no such table: missing".to_owned()),
    ))
    .into());
    assert_eq!(actual, expected);
}
