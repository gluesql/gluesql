use {
    gluesql_core::prelude::Glue, gluesql_csv_storage::CsvStorage, std::fs::remove_dir_all,
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
