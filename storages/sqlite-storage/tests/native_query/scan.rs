use {
    super::{expect_native_source, expect_native_source_under_filter, expect_query, select},
    gluesql_core::{
        data::Value,
        plan::{
            FilterInputPlan, FilterPlan, HashJoinInputPlan, InnerJoinInputPlan, ProjectInputPlan,
            QueryPlan, SourcePlan, TableAccessPlan,
        },
        prelude::Glue,
        store::Planner,
    },
    gluesql_sqlite_storage::{SqliteStorage, SqliteStorageError},
    rusqlite::Connection,
    tempfile::tempdir,
};

#[test]
fn table_scan_reads_columns_for_projection_and_common_filter() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (id INT, name TEXT, qty INT);
        INSERT INTO Item VALUES (1, 'Pear', 0), (2, 'Lime', 3);
        ",
    )
    .unwrap();

    let sql = "SELECT name FROM Item";
    let plan = glue.plan(sql).unwrap().remove(0);
    let source = expect_native_source(expect_query(&plan));
    assert_eq!(
        source.query,
        r#"SELECT "Item"."name" FROM "Item" ORDER BY "Item"."_rowid_" ASC"#,
    );

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(
        &["name"],
        vec![
            vec![Value::Str("Pear".to_owned())],
            vec![Value::Str("Lime".to_owned())],
        ],
    );
    assert_eq!(actual, expected);

    let sql = "
        SELECT i.label
        FROM Item AS i (id, label)
        WHERE i.qty + 1 > 2
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let source = expect_native_source_under_filter(expect_query(&plan));
    assert_eq!(
        source.query,
        concat!(
            r#"SELECT "Item"."name", "Item"."qty" FROM "Item" "#,
            r#"ORDER BY "Item"."_rowid_" ASC"#,
        ),
    );
    assert!(source.params.is_empty());

    let actual = glue.execute_prepared(&plan).unwrap();
    let expected = select(&["label"], vec![vec![Value::Str("Lime".to_owned())]]);
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
fn sqlite_primary_key_order_only_decodes_rows_consumed_by_limit() {
    for key_type in ["INT", "DECIMAL"] {
        let directory = tempdir().unwrap();
        let path = directory.path().join("limit.sqlite");
        let mut glue = Glue::new(SqliteStorage::new(&path).unwrap());
        glue.execute(format!(
            "
            CREATE TABLE Item (id {key_type} PRIMARY KEY, flag BOOLEAN);
            INSERT INTO Item VALUES (10, FALSE), (2, TRUE);
            ",
        ))
        .unwrap();

        // The later key holds an INTEGER that cannot decode as a GlueSQL BOOLEAN.
        let writer = Connection::open(&path).unwrap();
        writer
            .execute("UPDATE Item SET flag = 2 WHERE flag = 0", [])
            .unwrap();

        let actual = glue.execute("SELECT flag FROM Item LIMIT 1").unwrap();
        let expected = vec![select(&["flag"], vec![vec![Value::Bool(true)]])];
        assert_eq!(actual, expected, "{key_type}");

        let actual = glue.execute("SELECT flag FROM Item");
        let expected = Err(SqliteStorageError::InvalidBoolean(2).into());
        assert_eq!(actual, expected, "{key_type}");
    }
}

#[test]
fn gluesql_key_order_preserves_visible_columns_and_user_ordering() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE Item (name TEXT, id INT128 PRIMARY KEY);
        INSERT INTO Item VALUES ('ten', 10), ('two', 2);
        ",
    )
    .unwrap();

    // The inner read needs id, even when the caller only asks for the name.
    let actual = glue
        .execute("SELECT i.label FROM Item i(label) LIMIT 1")
        .unwrap();
    let expected = vec![select(&["label"], vec![vec![Value::Str("two".to_owned())]])];
    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT * FROM Item i(v, v)").unwrap();
    let expected = vec![select(
        &["v", "v"],
        vec![
            vec![Value::Str("two".to_owned()), Value::I128(2)],
            vec![Value::Str("ten".to_owned()), Value::I128(10)],
        ],
    )];
    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT 1 AS one FROM Item").unwrap();
    let expected = vec![select(
        &["one"],
        vec![vec![Value::I64(1)], vec![Value::I64(1)]],
    )];
    assert_eq!(actual, expected);

    let actual = glue
        .execute("SELECT name FROM Item ORDER BY name LIMIT 1 OFFSET 1")
        .unwrap();
    let expected = vec![select(&["name"], vec![vec![Value::Str("two".to_owned())]])];
    assert_eq!(actual, expected);
}

#[test]
fn hash_join_keeps_its_sorted_native_input_when_replanned() {
    let mut glue = Glue::new(SqliteStorage::in_memory().unwrap());
    glue.execute(
        "
        CREATE TABLE L (code INT);
        CREATE TABLE R (id INT128 PRIMARY KEY, code INT);
        INSERT INTO L VALUES (7), (8);
        INSERT INTO R VALUES (10, 7), (2, 7), (3, 8);
        ",
    )
    .unwrap();

    let sql = "
        SELECT l.code, r.id
        FROM L l JOIN R r ON l.code = r.code
        ORDER BY l.code, r.id
    ";
    let plan = glue.plan(sql).unwrap().remove(0);
    let ProjectInputPlan::InnerJoin(join) = &expect_query(&plan).project().unwrap().input else {
        panic!("expected an inner join: {plan:?}");
    };
    let InnerJoinInputPlan::Hash(hash) = &join.input else {
        panic!("expected a hash join: {join:?}");
    };
    assert!(matches!(
        hash.input,
        HashJoinInputPlan::Source(SourcePlan::Native(_))
    ));

    let SourcePlan::Derived(right) = &hash.right else {
        panic!("expected the right input to sort its primary key: {hash:?}");
    };
    assert!(matches!(*right.query, QueryPlan::SelectOrderBy(_)));
    assert_eq!(expect_native_source(&right.query).alias, "r");

    let replanned = glue.storage.plan(plan.clone()).unwrap();
    assert_eq!(replanned, plan);

    let actual = glue.execute_prepared(&replanned).unwrap();
    let expected = select(
        &["code", "id"],
        vec![
            vec![Value::I64(7), Value::I128(2)],
            vec![Value::I64(7), Value::I128(10)],
            vec![Value::I64(8), Value::I128(3)],
        ],
    );
    assert_eq!(actual, expected);
}
