use {
    gluesql_core::{
        prelude::Glue,
        store::{Statistic, Statistics},
    },
    gluesql_csv_storage::CsvStorage,
    std::fs::remove_dir_all,
    test_suite::*,
};

struct CsvTester {
    glue: Glue<CsvStorage>,
}

impl Tester<CsvStorage> for CsvTester {
    fn new(namespace: &str) -> Self {
        let path = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {e:?}");
        }

        let storage = CsvStorage::new(&path).expect("CsvStorage::new");
        let glue = Glue::new(storage);
        CsvTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<CsvStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, CsvTester);
generate_alter_table_tests!(test, CsvTester);

#[test]
fn plan_statistics_use_fallbacks_without_scanning_csv_data() {
    let path = "tmp/csv_plan_statistics";
    let _ = remove_dir_all(path);
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);
    glue.execute("CREATE TABLE Foo (id INTEGER);").unwrap();
    glue.execute("INSERT INTO Foo VALUES (1), (2);").unwrap();

    assert_eq!(
        glue.storage
            .fetch_table_statistics("Foo")
            .unwrap()
            .row_count,
        Statistic::Unknown
    );

    let planned = glue
        .plan_with_statistics("SELECT * FROM Foo WHERE id = 1")
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(
        planned.statistics.full_scans[0].cardinality,
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

    remove_dir_all(path).unwrap();
}
