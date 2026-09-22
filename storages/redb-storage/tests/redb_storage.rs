use {
    gluesql_core::{
        data::{Key, Value},
        prelude::Glue,
        store::{ColumnStatistics, Statistic, Statistics, StoreMut, Transaction},
    },
    gluesql_redb_storage::RedbStorage,
    redb::{Database, TableHandle},
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

generate_store_tests!(test, RedbTester);
generate_transaction_tests!(test, RedbTester);

fn exact_row_count(glue: &Glue<RedbStorage>, table_name: &str) -> u64 {
    let statistics = glue
        .storage
        .fetch_table_statistics(table_name)
        .expect("table statistics should be available");
    assert!(matches!(statistics.size_bytes, Statistic::Unknown));
    assert_eq!(
        glue.storage
            .fetch_column_statistics(table_name, "id")
            .expect("unsupported column statistics should be available"),
        ColumnStatistics::default()
    );

    match statistics.row_count {
        Statistic::Exact(row_count) => row_count,
        other => panic!("expected exact row count, got {other:?}"),
    }
}

#[test]
fn table_statistics_track_rows_and_transaction_state() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_table_statistics";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 0);

    glue.execute("INSERT INTO Foo VALUES (1), (2), (3);")
        .unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 3);

    glue.execute("DELETE FROM Foo WHERE id = 2;").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 2);

    glue.execute("BEGIN;").unwrap();
    glue.execute("INSERT INTO Foo VALUES (4);").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 3);
    glue.execute("ROLLBACK;").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 2);

    glue.execute("BEGIN;").unwrap();
    glue.execute("INSERT INTO Foo VALUES (5);").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 3);
    glue.execute("COMMIT;").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 3);

    glue.execute("BEGIN;").unwrap();
    glue.execute("DROP TABLE Foo;").unwrap();
    assert!(
        glue.storage.fetch_table_statistics("Foo").is_err(),
        "dropped tables should not expose statistics inside a transaction"
    );
    glue.execute("ROLLBACK;").unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 3);

    glue.execute("DROP TABLE Foo;").unwrap();
    assert!(
        glue.storage.fetch_table_statistics("Foo").is_err(),
        "dropped tables should not expose statistics"
    );

    drop(glue);
    remove_file(path).expect("remove test storage");
}

#[test]
fn table_statistics_do_not_create_missing_tables_in_transactions() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_table_statistics_missing_table";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);

    glue.execute("BEGIN;").unwrap();
    assert!(
        glue.storage.fetch_table_statistics("Missing").is_err(),
        "missing tables should not be created by statistics lookup"
    );
    glue.execute("COMMIT;").unwrap();
    drop(glue);

    let db = Database::open(path).expect("reopen storage database");
    let txn = db.begin_read().expect("begin read transaction");
    assert!(
        !txn.list_tables()
            .expect("list tables")
            .any(|table| table.name() == "Missing")
    );
    drop(txn);
    drop(db);

    remove_file(path).expect("remove test storage");
}

#[test]
fn table_statistics_count_unique_redb_entries() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_table_statistics_unique_entries";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();

    glue.storage.begin(true).unwrap();
    glue.storage
        .insert_data("Foo", vec![(Key::I64(1), vec![Value::I64(1)])])
        .unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 1);

    glue.storage
        .insert_data("Foo", vec![(Key::I64(1), vec![Value::I64(2)])])
        .unwrap();
    assert_eq!(exact_row_count(&glue, "Foo"), 1);
    glue.storage.commit().unwrap();

    assert_eq!(exact_row_count(&glue, "Foo"), 1);
    drop(glue);

    let storage = RedbStorage::new(path).expect("reopen storage");
    let glue = Glue::new(storage);
    assert_eq!(exact_row_count(&glue, "Foo"), 1);

    drop(glue);
    remove_file(path).expect("remove test storage");
}

#[test]
fn plan_statistics_preserve_the_plan_and_use_exact_row_counts() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_plan_statistics";
    let _ = remove_file(path);

    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();
    glue.execute("INSERT INTO Foo VALUES (1), (2), (3), (4), (5);")
        .unwrap();

    let plan = glue.plan("SELECT * FROM Foo WHERE id = 1").unwrap();
    let planned = glue
        .plan_with_statistics("SELECT * FROM Foo WHERE id = 1")
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(planned.plan, plan[0]);
    assert_eq!(
        planned.statistics.full_scans[0].cardinality,
        Statistic::Exact(5)
    );
    assert_eq!(
        planned.statistics.full_scans[0].cost,
        Statistic::Estimated(5)
    );
    assert_eq!(
        planned.statistics.filters[0].input_cardinality,
        Statistic::Exact(5)
    );
    assert_eq!(
        planned.statistics.filters[0].selectivity,
        Statistic::Estimated(0.1)
    );
    assert_eq!(
        planned.statistics.filters[0].cardinality,
        Statistic::Estimated(0)
    );
    assert_eq!(planned.statistics.filters[0].cost, Statistic::Estimated(5));

    drop(glue);
    remove_file(path).expect("remove test storage");
}

#[test]
fn plan_statistics_with_parameters_preserve_the_plan() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_plan_statistics_params";
    let _ = remove_file(path);
    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();

    let plan = glue
        .plan_with_params("SELECT * FROM Foo WHERE id = $1", [1])
        .unwrap();
    let planned = glue
        .plan_with_statistics_and_params("SELECT * FROM Foo WHERE id = $1", [1])
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(planned.plan, plan[0]);
    assert_eq!(
        planned.statistics.full_scans[0].cardinality,
        Statistic::Exact(0)
    );
    assert_eq!(
        planned.statistics.filters[0].selectivity,
        Statistic::Estimated(0.1)
    );
    assert_eq!(
        planned.statistics.filters[0].cardinality,
        Statistic::Estimated(0)
    );
    assert_eq!(planned.statistics.filters[0].cost, Statistic::Estimated(0));

    drop(glue);
    remove_file(path).expect("remove test storage");
}

#[test]
fn plan_statistics_expose_boolean_filter_estimates() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_plan_statistics_boolean";
    let _ = remove_file(path);
    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();
    for id in 0..100 {
        glue.execute(format!("INSERT INTO Foo VALUES ({id});"))
            .unwrap();
    }

    for (operator, selectivity, cardinality) in [("AND", 0.01, 1), ("OR", 0.19, 19)] {
        let sql = format!("SELECT * FROM Foo WHERE id = 1 {operator} id = 2");
        let plan = glue.plan(&sql).unwrap();
        let planned = glue.plan_with_statistics(&sql).unwrap().pop().unwrap();

        assert_eq!(planned.plan, plan[0]);
        assert_eq!(
            planned.statistics.filters[0].input_cardinality,
            Statistic::Exact(100)
        );
        let Statistic::Estimated(actual) = planned.statistics.filters[0].selectivity else {
            panic!("expected estimated selectivity");
        };
        assert!((actual - selectivity).abs() < f64::EPSILON);
        assert_eq!(
            planned.statistics.filters[0].cardinality,
            Statistic::Estimated(cardinality)
        );
        assert_eq!(
            planned.statistics.filters[0].cost,
            Statistic::Estimated(100)
        );
    }

    drop(glue);
    remove_file(path).expect("remove test storage");
}

#[test]
fn plan_statistics_use_fallback_for_outer_derived_filters() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_plan_statistics_derived";
    let _ = remove_file(path);
    let storage = RedbStorage::new(path).expect("open storage");
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();
    glue.execute("INSERT INTO Foo VALUES (1), (2), (3), (4), (5);")
        .unwrap();

    let sql = "SELECT * FROM (SELECT * FROM Foo) AS sub WHERE id = 1";
    let plan = glue.plan(sql).unwrap();
    let planned = glue.plan_with_statistics(sql).unwrap().pop().unwrap();

    assert_eq!(planned.plan, plan[0]);
    assert_eq!(planned.statistics.full_scans.len(), 1);
    assert_eq!(
        planned.statistics.full_scans[0].cardinality,
        Statistic::Exact(5)
    );
    assert_eq!(planned.statistics.filters.len(), 1);
    assert_eq!(
        planned.statistics.filters[0].input_cardinality,
        Statistic::Estimated(1_000)
    );
    assert_eq!(
        planned.statistics.filters[0].selectivity,
        Statistic::Estimated(0.1)
    );
    assert_eq!(
        planned.statistics.filters[0].cardinality,
        Statistic::Estimated(100)
    );
    assert_eq!(
        planned.statistics.filters[0].cost,
        Statistic::Estimated(1_000)
    );

    drop(glue);
    remove_file(path).expect("remove test storage");
}
