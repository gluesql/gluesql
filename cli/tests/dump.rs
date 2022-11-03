use gluesql_core::store::Transaction;

use {
    gluesql_cli::dump_database,
    gluesql_core::prelude::Glue,
    gluesql_core::store::Store,
    gluesql_sled_storage::{sled, SledStorage},
    std::{fs::File, io::Read, path::PathBuf},
};

// #[test]
#[tokio::test]
async fn dump_and_import() {
    let data_path = "tmp/src";
    let dump_path = PathBuf::from("tmp/dump.sql");

    let config = sled::Config::default().path(data_path).temporary(true);
    let source_storage = SledStorage::try_from(config).unwrap();
    let mut source_glue = Glue::new(source_storage);

    let sqls = vec![
        "CREATE TABLE User (id INT, name TEXT);",
        "INSERT INTO User SELECT N, 'a' FROM SERIES(101);",
    ];

    for sql in sqls {
        source_glue.execute(sql).unwrap();
    }

    let sql = "SELECT * FROM User;";
    let source_data = source_glue.execute(sql).unwrap();

    let source_storage = dump_database(source_glue.storage.unwrap(), dump_path.clone()).unwrap();

    let data_path = "tmp/target";
    let config = sled::Config::default().path(data_path).temporary(true);
    let target_storage = SledStorage::try_from(config).unwrap();
    let mut target_glue = Glue::new(target_storage);

    let mut sqls = String::new();
    File::open(dump_path)
        .unwrap()
        .read_to_string(&mut sqls)
        .unwrap();

    for sql in sqls.split(';').filter(|sql| !sql.trim().is_empty()) {
        target_glue.execute(sql).unwrap();
    }

    let target_data = target_glue.execute(sql).unwrap();
    assert_eq!(source_data, target_data);

    let (source_storage, _) = source_storage.begin(true).await.unwrap();
    let source_schemas = source_storage.fetch_all_schemas().await.unwrap();

    let (target_storage, _) = target_glue.storage.unwrap().begin(true).await.unwrap();
    let target_schemas = target_storage.fetch_all_schemas().await.unwrap();

    assert_eq!(source_schemas, target_schemas);
}
