use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_parquet_storage::ParquetStorage,
    std::fs::remove_dir_all, test_suite::*,
};

struct ParquetTester {
    glue: Glue<ParquetStorage>,
}

#[async_trait(?Send)]
impl Tester<ParquetStorage> for ParquetTester {
    async fn new(namespace: &str) -> Self {
        let path: String = format!("tmp/{namespace}");

        if let Err(e) = remove_dir_all(&path) {
            println!("fs::remove_file {:?}", e);
        }
        let storage = ParquetStorage::new(&path).expect("ParquetStorage::new");
        let glue = Glue::new(storage);

        ParquetTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<ParquetStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, ParquetTester);
generate_alter_table_tests!(tokio::test, ParquetTester);

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).await.unwrap();
    };
}

#[tokio::test]
async fn scan_data_with_columns_parquet() {
    use futures::TryStreamExt;
    use gluesql_core::{
        data::{Key, Value},
        store::{DataRow, Store},
    };

    let path = "tmp/scan_columns_parquet";
    let _ = remove_dir_all(path);
    let storage = ParquetStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    exec!(glue "CREATE TABLE Foo (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);");
    exec!(glue "INSERT INTO Foo VALUES (1, 'a', 10), (2, 'b', 20);");

    let rows = Store::scan_data_with_columns(&glue.storage, "Foo", &["name".to_owned()])
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            (Key::I64(1), DataRow::Vec(vec![Value::Str("a".to_owned())])),
            (Key::I64(2), DataRow::Vec(vec![Value::Str("b".to_owned())]))
        ]
    );
}
