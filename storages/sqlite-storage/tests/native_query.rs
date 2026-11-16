use {
    gluesql_core::{
        ast::DataType,
        data::Value,
        error::{Error, Result},
        plan::{
            AggregationInputPlan, ExprPlan, FilterInputPlan, FilterPlan, InnerJoinInputPlan,
            NativeSourcePlan, ProjectInputPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
            SourcePlan, StatementPlan, StatementPlanKind, TableAccessPlan,
        },
        prelude::{Glue, Payload},
        query_builder::{Build, native_query},
        store::NativeQuery,
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::{Connection, Error as SqliteError, ffi},
    std::time::Duration,
    tempfile::tempdir,
};

fn expect_query(plan: &StatementPlan) -> &QueryPlan {
    let StatementPlanKind::Query(query) = &plan.kind else {
        panic!("expected query: {plan:?}");
    };

    query
}

fn expect_native_source(query: &QueryPlan) -> &NativeSourcePlan {
    let ProjectInputPlan::Source(SourcePlan::Native(native)) = &query.project().unwrap().input
    else {
        panic!("expected native source: {query:?}");
    };

    native
}

fn expect_partial_native_source(query: &QueryPlan) -> &NativeSourcePlan {
    let ProjectInputPlan::Filter(FilterPlan {
        input: FilterInputPlan::Source(SourcePlan::Native(native)),
        ..
    }) = &query.project().unwrap().input
    else {
        panic!("expected a remaining filter over a native source: {query:?}");
    };

    native
}

fn select(labels: &[&str], rows: Vec<Vec<Value>>) -> Payload {
    Payload::Select {
        labels: labels.iter().map(|name| (*name).to_owned()).collect(),
        rows,
    }
}

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
    let source = expect_partial_native_source(expect_query(&plan));
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

    // execute_stmt plans again; both common projection and a residual filter must survive.
    for (sql, expected) in [
        (
            "SELECT LOWER(name) AS label FROM Item WHERE qty >= 10",
            select(
                &["label"],
                vec![
                    vec![Value::Str("lime".to_owned())],
                    vec![Value::Str("pear".to_owned())],
                ],
            ),
        ),
        (
            "SELECT id FROM Item WHERE qty >= 10 AND LOWER(name) = 'pear'",
            select(&["id"], vec![vec![Value::I64(3)]]),
        ),
    ] {
        let plan = glue.plan(sql).unwrap().remove(0);
        let source = expect_query(&plan).project().unwrap().input.base_source();
        assert!(matches!(source, SourcePlan::Native(_)), "{sql}: {source:?}");

        let actual = glue.execute_stmt(plan).unwrap();
        assert_eq!(actual, expected, "{sql}");
    }
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
fn empty_native_projection_preserves_row_count() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT PRIMARY KEY);
        INSERT INTO Item VALUES (2), (1), (3);
        ",
    )
    .unwrap();

    // The native source returns rows even though the common projection needs no columns.
    let sql = "SELECT 1 AS one FROM Item WHERE TRUE LIMIT 2";
    let plan = glue.plan(sql).unwrap().remove(0);
    assert!(expect_native_source(expect_query(&plan)).columns.is_empty());

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["one"], vec![vec![Value::I64(1)], vec![Value::I64(1)]]);
    assert_eq!(actual, expected);
}

#[test]
fn primary_key_lookup_keeps_the_existing_storage_path() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT PRIMARY KEY, qty INT);
        INSERT INTO Item VALUES (2, 12), (1, 7), (3, 20);
        ",
    )
    .unwrap();

    let plan = glue
        .plan("SELECT id FROM Item WHERE id = 2")
        .unwrap()
        .remove(0);
    let source = expect_query(&plan).project().unwrap().input.base_source();
    let SourcePlan::Table(table) = source else {
        panic!("expected table source: {source:?}");
    };
    assert!(matches!(table.access, TableAccessPlan::PrimaryKey { .. }));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["id"], vec![vec![Value::I64(2)]]);
    assert_eq!(actual, expected);

    // Native can translate qty, but the arithmetic key must retain its lookup.
    let sql = "
        SELECT qty
        FROM Item
        WHERE id = 1 + 1 AND qty > 10
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let ProjectInputPlan::Filter(FilterPlan {
        input: FilterInputPlan::Source(SourcePlan::Table(table)),
        ..
    }) = &expect_query(&plan).project().unwrap().input
    else {
        panic!("expected a residual filter over a table lookup: {plan:?}");
    };
    assert!(matches!(table.access, TableAccessPlan::PrimaryKey { .. }));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["qty"], vec![vec![Value::I64(12)]]);
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
fn native_bytea_primary_keys_keep_logical_key_order() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id BYTEA PRIMARY KEY, qty INT);
        INSERT INTO Item VALUES (X'FF', 3), (X'00FF', 2), (X'00', 1);
        ",
    )
    .unwrap();

    let sql = "SELECT id FROM Item WHERE qty > 0";
    let plan = glue.plan(sql).unwrap().remove(0);
    expect_native_source(expect_query(&plan));

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["id"],
        vec![
            vec![Value::Bytea(vec![0])],
            vec![Value::Bytea(vec![0, 255])],
            vec![Value::Bytea(vec![255])],
        ],
    );
    assert_eq!(actual, expected);
}

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
fn native_numeric_bindings_preserve_comparison_boundaries() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Numbers (id INT, n INT8, f FLOAT32, d FLOAT);
        INSERT INTO Numbers VALUES (1, 10, 0.1, 0.1), (2, 100, 0.5, 0.5);
        ",
    )
    .unwrap();

    for (sql, rows) in [
        (
            "SELECT id FROM Numbers WHERE n < 100",
            vec![vec![Value::I64(1)]],
        ),
        (
            "SELECT id FROM Numbers WHERE f <= 0.1",
            vec![vec![Value::I64(1)]],
        ),
        (
            "SELECT id FROM Numbers WHERE d > 0.1",
            vec![vec![Value::I64(2)]],
        ),
        (
            "SELECT id FROM Numbers WHERE f BETWEEN 0.1 AND 0.5",
            vec![vec![Value::I64(1)], vec![Value::I64(2)]],
        ),
    ] {
        let plan = glue.plan(sql).unwrap().remove(0);
        expect_native_source(expect_query(&plan));

        let actual = glue.execute_prepared(&plan).unwrap();
        let expected = select(&["id"], rows);
        assert_eq!(actual, expected, "{sql}");
    }
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

    for (sql, rows) in [
        (
            "SELECT id FROM Item WHERE enabled OR n = 2",
            vec![vec![Value::I64(1)], vec![Value::I64(2)]],
        ),
        (
            "SELECT id FROM Item WHERE NOT (enabled AND n = 2)",
            vec![vec![Value::I64(2)], vec![Value::I64(3)]],
        ),
        ("SELECT id FROM Item WHERE NOT (n IN (2, NULL))", vec![]),
        ("SELECT id FROM Item WHERE n NOT IN (2, NULL)", vec![]),
        (
            "SELECT id FROM Item WHERE NOT (n BETWEEN NULL AND 0)",
            vec![vec![Value::I64(2)], vec![Value::I64(3)]],
        ),
        (
            "SELECT id FROM Item WHERE n NOT BETWEEN 4 AND NULL",
            vec![vec![Value::I64(2)], vec![Value::I64(3)]],
        ),
        (
            "SELECT id FROM Item WHERE (n > 1) IS NULL",
            vec![vec![Value::I64(1)]],
        ),
    ] {
        let plan = glue.plan(sql).unwrap().remove(0);
        expect_native_source(expect_query(&plan));

        let actual = glue.execute_prepared(&plan).unwrap();
        let expected = select(&["id"], rows);
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn native_scalar_subquery_preserves_the_outer_row_projection() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES (1, 'APPLE', 7), (2, 'LIME', 12), (3, 'PEAR', 20);
        ",
    )
    .unwrap();

    // Each native outer row opens a native inner cursor; its projection reads o.name.
    let sql = "
        SELECT (
            SELECT o.name
            FROM Item
            WHERE qty = 12
        ) AS label
        FROM Item o
        WHERE o.qty > 10
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let outer = expect_query(&plan);
    expect_native_source(outer);

    let ProjectionPlan::SelectItems(items) = &outer.project().unwrap().projection else {
        panic!("expected select items");
    };

    let [
        SelectItemPlan::Expr {
            expr: ExprPlan::Subquery(inner),
            ..
        },
    ] = items.as_slice()
    else {
        panic!("expected scalar subquery: {items:?}");
    };

    expect_native_source(inner);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["label"],
        vec![
            vec![Value::Str("LIME".to_owned())],
            vec![Value::Str("PEAR".to_owned())],
        ],
    );
    assert_eq!(actual, expected);
}

#[test]
fn native_subqueries_feed_common_query_consumers() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES (1, 'APPLE', 7), (2, 'LIME', 12), (3, 'PEAR', 20);
        ",
    )
    .unwrap();

    // A common derived-table projection and ordering consume the native inner rows.
    let sql = "
        SELECT d.label
        FROM (
            SELECT name AS label
            FROM Item
            WHERE qty > 10
        ) d
        ORDER BY d.label DESC
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let source = expect_query(&plan).project().unwrap().input.base_source();
    let SourcePlan::Derived(derived) = source else {
        panic!("expected derived source: {source:?}");
    };
    expect_native_source(&derived.query);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["label"],
        vec![
            vec![Value::Str("PEAR".to_owned())],
            vec![Value::Str("LIME".to_owned())],
        ],
    );
    assert_eq!(actual, expected);

    // IN stays in the common filter and obtains its candidates from a native subquery.
    let sql = "
        SELECT name
        FROM Item
        WHERE id IN (
            SELECT id
            FROM Item
            WHERE qty > 10
        )
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let input = &expect_query(&plan).project().unwrap().input;
    let ProjectInputPlan::Filter(FilterPlan {
        expr: ExprPlan::InSubquery { subquery, .. },
        ..
    }) = input
    else {
        panic!("expected IN subquery in the common filter: {input:?}");
    };
    expect_native_source(subquery);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["name"],
        vec![
            vec![Value::Str("LIME".to_owned())],
            vec![Value::Str("PEAR".to_owned())],
        ],
    );
    assert_eq!(actual, expected);

    // The common EXISTS condition checks whether the native subquery yields a row.
    let sql = "
        SELECT id
        FROM Item
        WHERE EXISTS (
            SELECT id
            FROM Item
            WHERE qty > 10
        )
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let input = &expect_query(&plan).project().unwrap().input;
    let ProjectInputPlan::Filter(FilterPlan {
        expr: ExprPlan::Exists { subquery, .. },
        ..
    }) = input
    else {
        panic!("expected EXISTS subquery in the common filter: {input:?}");
    };
    expect_native_source(subquery);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["id"],
        vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(3)],
        ],
    );
    assert_eq!(actual, expected);
}

#[test]
fn nested_native_filters_preserve_the_correlated_condition() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES (1, 'APPLE', 7), (2, 'LIME', 12), (3, 'PEAR', 20);
        ",
    )
    .unwrap();

    // SQLite filters qty on both sides; the common inner filter compares i.id to o.id.
    let sql = "
        SELECT o.name
        FROM Item o
        WHERE o.qty > 10 AND EXISTS (
            SELECT i.id
            FROM Item i
            WHERE i.qty > 15 AND i.id = o.id
        )
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let outer = expect_query(&plan);
    expect_partial_native_source(outer);
    let ProjectInputPlan::Filter(FilterPlan {
        expr: ExprPlan::Exists { subquery, .. },
        ..
    }) = &outer.project().unwrap().input
    else {
        panic!("expected correlated EXISTS in the remaining filter: {outer:?}");
    };
    expect_partial_native_source(subquery);

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["name"], vec![vec![Value::Str("PEAR".to_owned())]]);
    assert_eq!(actual, expected);
}

#[test]
fn native_outer_rows_feed_a_correlated_hash_join() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        CREATE TABLE A (id INT);
        CREATE TABLE B (id INT);
        INSERT INTO Item VALUES
            (1, 'APPLE', 7),
            (2, 'LIME', 12),
            (3, 'PEAR', 20);
        INSERT INTO A VALUES (10);
        INSERT INTO B VALUES (11), (13);
        ",
    )
    .unwrap();

    // Native filters Item; the common hash key still depends on each outer id.
    let sql = "
        SELECT o.name
        FROM Item o
        WHERE o.qty > 10 AND EXISTS (
            SELECT a.id
            FROM A a
            JOIN B b ON b.id = a.id + o.id
        )
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let outer = expect_query(&plan);
    expect_partial_native_source(outer);
    let ProjectInputPlan::Filter(FilterPlan {
        expr: ExprPlan::Exists { subquery, .. },
        ..
    }) = &outer.project().unwrap().input
    else {
        panic!("expected a residual EXISTS: {plan:?}");
    };
    let ProjectInputPlan::InnerJoin(join) = &subquery.project().unwrap().input else {
        panic!("expected a common inner join: {subquery:?}");
    };
    assert!(
        matches!(join.input, InnerJoinInputPlan::Hash(_)),
        "{join:?}"
    );

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["name"], vec![vec![Value::Str("PEAR".to_owned())]]);
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

#[test]
fn native_api_validates_statements_before_iteration() {
    let storage = SqliteStorage::in_memory().unwrap();

    // NativeQuery is a read interface; reject mutation before stepping it.
    let actual = storage
        .query_native("CREATE TABLE forbidden (id INTEGER)", &[], &[])
        .err();
    let expected = Some(SqliteStorageError::NativeStatementNotReadOnly.into());
    assert_eq!(actual, expected);

    // Result shape is checked even when the query returns no rows.
    let actual = storage
        .query_native("SELECT 1 WHERE FALSE", &[], &[DataType::Int, DataType::Int])
        .err();
    let expected = Some(
        SqliteStorageError::NativeColumnCountMismatch {
            expected: 2,
            actual: 1,
        }
        .into(),
    );
    assert_eq!(actual, expected);

    // A missing parameter is a binding error, before any row is requested.
    let actual = storage
        .query_native("SELECT ?1", &[], &[DataType::Int])
        .err();
    let expected =
        Some(SqliteStorageError::Sqlite(SqliteError::InvalidParameterCount(0, 1)).into());
    assert_eq!(actual, expected);

    // A typed parameter still has to pass the SQLite value codec.
    let actual = storage
        .query_native("SELECT ?1", &[Value::F64(f64::NAN)], &[DataType::Float])
        .err();
    let expected = Some(SqliteStorageError::NanNotSupported.into());
    assert_eq!(actual, expected);
}

#[test]
fn native_rows_outlive_query_arguments() {
    let storage = SqliteStorage::in_memory().unwrap();
    let mut rows = {
        let sql = "SELECT ?1".to_owned();
        let params = vec![Value::Str("retained".to_owned())];
        let types = vec![DataType::Text];
        storage.query_native(&sql, &params, &types).unwrap()
    };

    // All three argument buffers have been dropped before the first row is read.
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Str("retained".to_owned())]));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn native_cursor_reports_late_decode_errors_and_releases_its_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("decode-error.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();

    let plan = glue
        .plan("SELECT flag FROM Item WHERE TRUE")
        .unwrap()
        .remove(0);
    expect_native_source(expect_query(&plan));

    // SQLite accepts 2 as INTEGER, but the second row cannot decode as GlueSQL BOOLEAN.
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();
    writer
        .execute("UPDATE Item SET flag = 2 WHERE rowid = 2", [])
        .unwrap();

    let actual = glue.execute_prepared(&plan);
    let expected = Err(SqliteStorageError::InvalidBoolean(2).into());
    assert_eq!(actual, expected);

    // Direct iteration proves the valid first row is available before the later error.
    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Bool(true)]));
    assert_eq!(actual, expected);

    let actual = writer.execute("UPDATE Item SET flag = 0 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = Some(Err(SqliteStorageError::InvalidBoolean(2).into()));
    assert_eq!(actual, expected);

    // The error itself must release the lock, without another next() or drop().
    let actual = writer
        .execute("UPDATE Item SET flag = 0 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);

    // Repairing the stored row must not revive the failed iterator.
    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn dropping_a_native_cursor_releases_its_read_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("drop.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();

    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    let actual = rows.next();
    let expected = Some(Ok(vec![Value::Bool(true)]));
    assert_eq!(actual, expected);

    let actual = writer.execute("UPDATE Item SET flag = 1 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    // Drop with the second row still unread, then write from the other connection.
    drop(rows);
    let actual = writer
        .execute("UPDATE Item SET flag = 1 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);
}

#[test]
fn exhausting_a_native_cursor_releases_its_read_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exhaustion.sqlite");
    let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
    glue.execute(
        "
        CREATE TABLE Item (flag BOOLEAN);
        INSERT INTO Item VALUES (TRUE), (FALSE);
        ",
    )
    .unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();

    let mut rows = glue
        .storage
        .query_native(
            "SELECT flag FROM Item ORDER BY rowid",
            &[],
            &[DataType::Boolean],
        )
        .unwrap();
    for flag in [true, false] {
        let actual = rows.next();
        let expected = Some(Ok(vec![Value::Bool(flag)]));
        assert_eq!(actual, expected);
    }

    let actual = writer.execute("UPDATE Item SET flag = 1 WHERE rowid = 2", []);
    let expected = Err(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("database is locked".to_owned()),
    ));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);

    // The iterator is still in scope, but reaching EOF has released its statement.
    let actual = writer
        .execute("UPDATE Item SET flag = 1 WHERE rowid = 2", [])
        .unwrap();
    let expected = 1;
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}

#[test]
fn a_sqlite_step_error_ends_the_native_cursor() {
    let storage = SqliteStorage::in_memory().unwrap();
    let mut rows = storage
        .query_native("SELECT abs(-9223372036854775808)", &[], &[DataType::Int])
        .unwrap();

    // Preparation succeeds; SQLite raises integer overflow when stepping the query.
    let actual = rows.next();
    let expected = Some(Err(SqliteStorageError::Sqlite(SqliteError::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_ERROR),
        Some("integer overflow".to_owned()),
    ))
    .into()));
    assert_eq!(actual, expected);

    let actual = rows.next();
    let expected = None;
    assert_eq!(actual, expected);
}
