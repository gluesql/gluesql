use {
    gluesql_cli::dump_database,
    gluesql_core::prelude::Glue,
    gluesql_sled_storage::{sled, SledStorage},
    std::{fs::File, io::Read, path::PathBuf},
};

#[test]
fn dump_and_import() {
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
    let source = source_glue.execute(sql).unwrap();

    dump_database(source_glue.storage.unwrap(), dump_path.clone()).unwrap();

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

    let target = target_glue.execute(sql).unwrap();
    assert_eq!(source, target);
}
