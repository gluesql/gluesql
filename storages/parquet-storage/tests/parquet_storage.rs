use {
    gluesql_core::prelude::Glue, gluesql_parquet_storage::ParquetStorage, std::fs::remove_dir_all,
    test_suite::*,
};

struct ParquetTester {
    glue: Glue<ParquetStorage>,
}

impl Tester<ParquetStorage> for ParquetTester {
    fn new(namespace: &str) -> Self {
        let path: String = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {e:?}");
        }
        let storage = ParquetStorage::new(&path).expect("ParquetStorage::new");
        let glue = Glue::new(storage);

        ParquetTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<ParquetStorage> {
        &mut self.glue
    }
}

generate_store_tests!(test, ParquetTester);
generate_alter_table_tests!(test, ParquetTester);
