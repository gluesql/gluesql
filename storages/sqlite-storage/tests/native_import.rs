use {
    futures::TryStreamExt,
    gluesql_core::{
        data::{Key, Value},
        error::Result,
        executor::Payload,
        prelude::Glue,
        store::{DataRow, Store},
    },
    gluesql_sqlite_storage::SqliteStorage,
    rusqlite::{Connection, params, types::Null},
    rust_decimal::Decimal,
    std::{fs, path::PathBuf},
    uuid::Uuid,
};

fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push("gluesql-sqlite-tests");
    fs::create_dir_all(&path).expect("create sqlite test directory");

    path.push(format!("{prefix}-{}.db", Uuid::new_v4()));
    path
}

#[tokio::test]
async fn read_native_sqlite_schema() -> Result<()> {
    let db_path = temp_db_path("native-schema");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute_batch(
            "
            CREATE TABLE typed_data (
                id INTEGER PRIMARY KEY,
                int_col INTEGER NOT NULL,
                real_col REAL,
                text_col TEXT,
                blob_col BLOB,
                numeric_col NUMERIC
            );
            ",
        )
        .expect("create typed_data table");

        let blob: Vec<u8> = vec![0, 1, 2, 255];
        conn.execute(
            "INSERT INTO typed_data (id, int_col, real_col, text_col, blob_col, numeric_col)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![1_i64, 123_i64, 1.5_f64, "hello", &blob, 42.5_f64],
        )
        .expect("insert first typed_data row");

        conn.execute(
            "INSERT INTO typed_data (id, int_col, real_col, text_col, blob_col, numeric_col)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![2_i64, -45_i64, Null, "world", Null, 7.0_f64],
        )
        .expect("insert second typed_data row");
    }

    let storage = SqliteStorage::new(&db_path).await?;
    let mut glue = Glue::new(storage);

    let payloads = glue
        .execute(
            "SELECT id, int_col, real_col, text_col, blob_col, numeric_col \
             FROM typed_data ORDER BY id",
        )
        .await?;

    let rows = match payloads.as_slice() {
        [Payload::Select { rows, .. }] => rows.clone(),
        other => panic!("unexpected payload: {other:?}"),
    };

    drop(glue);
    fs::remove_file(&db_path).ok();

    assert_eq!(
        rows,
        vec![
            vec![
                Value::I64(1),
                Value::I64(123),
                Value::F64(1.5),
                Value::Str("hello".to_owned()),
                Value::Bytea(vec![0, 1, 2, 255]),
                Value::Decimal(Decimal::new(425, 1)),
            ],
            vec![
                Value::I64(2),
                Value::I64(-45),
                Value::Null,
                Value::Str("world".to_owned()),
                Value::Null,
                Value::Decimal(Decimal::new(7, 0)),
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn read_rowid_backed_table() -> Result<()> {
    let db_path = temp_db_path("native-rowid");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE log_entries (
                message TEXT NOT NULL
            )",
            [],
        )
        .expect("create log_entries table");

        conn.execute(
            "INSERT INTO log_entries (message) VALUES (?1)",
            params!["first entry"],
        )
        .expect("insert first log entry");

        conn.execute(
            "INSERT INTO log_entries (message) VALUES (?1)",
            params!["second entry"],
        )
        .expect("insert second log entry");
    }

    let storage = SqliteStorage::new(&db_path).await?;
    let rows = {
        let stream = storage.scan_data("log_entries").await?;
        stream.try_collect::<Vec<_>>().await?
    };
    drop(storage);
    fs::remove_file(&db_path).ok();

    assert_eq!(
        rows,
        vec![
            (
                Key::I64(1),
                DataRow::Vec(vec![Value::Str("first entry".to_owned())])
            ),
            (
                Key::I64(2),
                DataRow::Vec(vec![Value::Str("second entry".to_owned())])
            ),
        ]
    );

    Ok(())
}
