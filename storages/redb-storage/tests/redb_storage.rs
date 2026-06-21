use {
    gluesql_core::prelude::Glue,
    gluesql_redb_storage::RedbStorage,
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
