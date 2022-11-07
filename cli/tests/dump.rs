use {
    gluesql_cli::dump_database,
    gluesql_core::{
        prelude::Glue,
        store::{Store, Transaction},
    },
    gluesql_sled_storage::{sled, SledStorage},
    std::{fs::File, io::Read, path::PathBuf},
};

#[tokio::test]
async fn dump_and_import() {
    let data_path = "tmp/src";
    let dump_path = PathBuf::from("tmp/dump.sql");

    let config = sled::Config::default().path(data_path).temporary(true);
    let source_storage = SledStorage::try_from(config).unwrap();
    let mut source_glue = Glue::new(source_storage);

    let sqls = vec![
        "CREATE TABLE Foo (
            boolean BOOLEAN,
            int8 INT8,
            int16 INT16,
            int32 INT32,
            int INT,
            int128 INT128,
            uinti8 UINT8,
            text TEXT,
            bytea BYTEA,
            date DATE,
            timestamp TIMESTAMP,
            time TIME,
            interval INTERVAL,
            uuid UUID,
            map MAP,
            list LIST,
         );",
        r#"INSERT INTO Foo
         VALUES (
         true,
         1,
         2,
         3,
         4,
         5,
         6,
         'a',
         X'123456',
         DATE '2022-11-01',
         TIMESTAMP '2022-11-02',
         TIME '23:59:59',
         INTERVAL '1' DAY,
         '550e8400-e29b-41d4-a716-446655440000',
         '{"a": {"red": "apple", "blue": 1}, "b": 10}',
         '[{ "foo": 100, "bar": [true, 0, [10.5, false] ] }, 10, 20]'
         );"#,
        "CREATE INDEX Foo_int ON Foo (int);",
    ];

    for sql in sqls {
        source_glue.execute(sql).unwrap();
    }

    let sql = "SELECT * FROM Foo;";
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
