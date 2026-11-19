use {
    super::{expect_native_source, expect_native_source_under_filter, expect_query, select},
    gluesql_core::{
        data::Value,
        plan::{
            ExprPlan, FilterPlan, InnerJoinInputPlan, ProjectInputPlan, ProjectionPlan,
            SelectItemPlan, SourcePlan,
        },
        prelude::Glue,
    },
    gluesql_sqlite_storage::SqliteStorage,
};

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
    expect_native_source_under_filter(outer);
    let ProjectInputPlan::Filter(FilterPlan {
        expr: ExprPlan::Exists { subquery, .. },
        ..
    }) = &outer.project().unwrap().input
    else {
        panic!("expected correlated EXISTS in the remaining filter: {outer:?}");
    };
    expect_native_source_under_filter(subquery);

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
    expect_native_source_under_filter(outer);
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
