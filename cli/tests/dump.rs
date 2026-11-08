use {
    gluesql_cli::dump_database,
    gluesql_core::{
        data::SchemaParseError,
        prelude::{Error, Glue, Payload, Value},
        store::Transaction,
    },
    gluesql_file_storage::FileStorage,
    gluesql_memory_storage::MemoryStorage,
    gluesql_redb_storage::RedbStorage,
    std::{
        fs,
        io::ErrorKind,
        process::{Command, Stdio},
    },
};

#[test]
fn dump_and_import() {
    let dir = tempfile::tempdir().unwrap();
    let dump_path = dir.path().join("dump.sql");
    let source_storage = RedbStorage::new(dir.path().join("source.redb")).unwrap();
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
            inet INET,
            date DATE,
            timestamp TIMESTAMP,
            time TIME,
            interval INTERVAL,
            uuid UUID,
            map MAP,
            list LIST
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
         '::1',
         DATE '2022-11-01',
         TIMESTAMP '2022-11-02',
         TIME '23:59:59',
         INTERVAL '1' DAY,
         '550e8400-e29b-41d4-a716-446655440000',
         '{"a": {"red": "apple", "blue": 1}, "b": 10}',
         '[{ "foo": 100, "bar": [true, 0, [10.5, false] ] }, 10, 20]'
         );"#,
        "CREATE INDEX Foo_int ON Foo (int);",
        "CREATE TABLE Bar AS SELECT N FROM SERIES(101);",
        "CREATE TABLE Baz;",
        r#"
        INSERT INTO Baz VALUES
            ('{"a": {"red": "apple", "blue": 1}, "b": 10}'),
            ('{"a": 100, "c": true}');
        "#,
        "CREATE TABLE Empty (id INTEGER);",
    ];

    for sql in sqls {
        source_glue.execute(sql).unwrap();
    }

    dump_database(&mut source_glue.storage, dump_path.clone()).unwrap();

    // Dump must finish its automatic transaction before returning the storage.
    assert_eq!(source_glue.storage.begin(false), Ok(false));
    source_glue.storage.rollback().unwrap();

    let target_storage = RedbStorage::new(dir.path().join("target.redb")).unwrap();
    let mut target_glue = Glue::new(target_storage);

    let sqls = fs::read_to_string(dump_path).unwrap();

    let bar_inserts = sqls
        .lines()
        .filter(|line| line.starts_with(r#"INSERT INTO "Bar""#))
        .count();
    assert_eq!(bar_inserts, 2);
    assert!(sqls.contains(r#"CREATE TABLE "Empty" ("id" INT NULL);"#));
    assert!(!sqls.contains(r#"INSERT INTO "Empty""#));

    for sql in sqls.split(';').filter(|sql| !sql.trim().is_empty()) {
        target_glue.execute(sql).unwrap();
    }

    // schemas should be identical
    let sql = "SELECT OBJECT_TYPE, OBJECT_NAME FROM GLUE_OBJECTS";
    let source_data = source_glue.execute(sql).unwrap();
    let target_data = target_glue.execute(sql).unwrap();
    assert_eq!(source_data, target_data);

    // data should be identical
    let sql = "SELECT * FROM Foo JOIN Bar;";
    let source_data = source_glue.execute(sql).unwrap();
    let target_data = target_glue.execute(sql).unwrap();
    assert_eq!(source_data, target_data);

    let sql = "SELECT * FROM Baz;";
    let source_data = source_glue.execute(sql).unwrap();
    let target_data = target_glue.execute(sql).unwrap();
    assert_eq!(source_data, target_data);
}

#[test]
fn dump_memory_storage() {
    let dir = tempfile::tempdir().unwrap();
    let dump_path = dir.path().join("dump.sql");
    let mut glue = Glue::new(MemoryStorage::default());
    glue.execute("CREATE TABLE Items (n INT); INSERT INTO Items VALUES (10)")
        .unwrap();

    dump_database(&mut glue.storage, dump_path.clone()).unwrap();

    let actual = fs::read_to_string(dump_path).unwrap();
    let expected = r#"
        CREATE TABLE "Items" ("n" INT NULL);
        INSERT INTO "Items" VALUES (10);
    "#;
    test_dump(&actual, expected);
}

#[test]
fn dump_keeps_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let dump_path = dir.path().join("dump.sql");
    let mut glue = Glue::new(RedbStorage::new(dir.path().join("data.redb")).unwrap());
    glue.execute(
        "CREATE TABLE Items (n INT);
         INSERT INTO Items VALUES (10);
         BEGIN;
         INSERT INTO Items VALUES (20)",
    )
    .unwrap();

    dump_database(&mut glue.storage, dump_path.clone()).unwrap();

    let actual = fs::read_to_string(dump_path).unwrap();
    let expected = r#"
        CREATE TABLE "Items" ("n" INT NULL);
        INSERT INTO "Items" VALUES (10), (20);
    "#;
    test_dump(&actual, expected);

    // Dump must neither commit nor roll back the caller's pending insert.
    let actual = glue.execute("SELECT n FROM Items ORDER BY n").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["n".to_owned()],
        rows: vec![vec![Value::I64(10)], vec![Value::I64(20)]],
    }];
    assert_eq!(actual, expected);

    glue.execute("ROLLBACK").unwrap();
    let actual = glue.execute("SELECT n FROM Items").unwrap();
    let expected = vec![Payload::Select {
        labels: vec!["n".to_owned()],
        rows: vec![vec![Value::I64(10)]],
    }];
    assert_eq!(actual, expected);
}

#[test]
fn dump_file_creation_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RedbStorage::new(dir.path().join("data.redb")).unwrap();

    let actual = dump_database(&mut storage, dir.path().join("missing/dump.sql")).unwrap_err();
    assert_eq!(
        actual.downcast_ref::<std::io::Error>().unwrap().kind(),
        ErrorKind::NotFound,
    );
    assert_eq!(storage.begin(false), Ok(false));
    storage.rollback().unwrap();
}

#[test]
fn dump_schema_error_without_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = FileStorage::new(dir.path().join("data")).unwrap();
    fs::write(storage.path("Broken.sql"), "SELECT 1").unwrap();
    let dump_path = dir.path().join("dump.sql");

    let actual = dump_database(&mut storage, dump_path.clone()).unwrap_err();
    let expected = Error::Schema(SchemaParseError::CannotParseDDL);
    assert_eq!(actual.downcast_ref::<Error>(), Some(&expected));
    assert_eq!(fs::read_to_string(dump_path).unwrap(), "");
}

#[test]
fn cli_dump_redb() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("data.redb");
    let dump_path = dir.path().join("dump.sql");
    let mut glue = Glue::new(RedbStorage::new(&data_path).unwrap());
    glue.execute("CREATE TABLE Items (n INT); INSERT INTO Items VALUES (10)")
        .unwrap();
    drop(glue);

    let output = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"))
        .args(["--storage", "redb", "--path"])
        .arg(data_path)
        .arg("--dump")
        .arg(&dump_path)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let actual = fs::read_to_string(dump_path).unwrap();
    let expected = r#"
        CREATE TABLE "Items" ("n" INT NULL);
        INSERT INTO "Items" VALUES (10);
    "#;
    test_dump(&actual, expected);
    assert!(!String::from_utf8(output.stdout).unwrap().contains("bye"));
}

#[test]
fn cli_dump_file_storage() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("data");
    let dump_path = dir.path().join("dump.sql");
    let mut glue = Glue::new(FileStorage::new(&data_path).unwrap());
    glue.execute("CREATE TABLE Items (n INT); INSERT INTO Items VALUES (10)")
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"))
        .args(["--storage", "file", "--path"])
        .arg(data_path)
        .arg("--dump")
        .arg(&dump_path)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let actual = fs::read_to_string(dump_path).unwrap();
    let expected = r#"
        CREATE TABLE "Items" ("n" INT NULL);
        INSERT INTO "Items" VALUES (10);
    "#;
    test_dump(&actual, expected);
}

#[test]
fn cli_dump_empty_memory_storage() {
    let dir = tempfile::tempdir().unwrap();
    let dump_path = dir.path().join("dump.sql");

    let output = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"))
        .args(["--storage", "memory", "--dump"])
        .arg(&dump_path)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::read_to_string(dump_path).unwrap(), "");
}

#[test]
fn cli_dump_requires_storage() {
    let dir = tempfile::tempdir().unwrap();
    let dump_path = dir.path().join("dump.sql");
    fs::write(&dump_path, "previous dump").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"))
        .arg("--dump")
        .arg(&dump_path)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let actual = fs::read_to_string(dump_path).unwrap();
    let expected = "previous dump";
    assert_eq!(actual, expected);
}

#[test]
fn cli_dump_error_exit_status() {
    let dir = tempfile::tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"))
        .args(["--storage", "memory", "--dump"])
        .arg(dir.path().join("missing/dump.sql"))
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
}

fn test_dump(actual: &str, expected: &str) {
    let actual = actual.trim().lines().map(str::trim).collect::<Vec<_>>();
    let expected = expected.trim().lines().map(str::trim).collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
