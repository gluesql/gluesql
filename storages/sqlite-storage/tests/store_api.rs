use {
    futures::TryStreamExt,
    gluesql_core::{
        data::{Key, Value},
        error::Result,
        store::{DataRow, Store, StoreMut},
    },
    gluesql_sqlite_storage::SqliteStorage,
    rusqlite::{Connection, OptionalExtension},
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
async fn native_table_fetched_via_load_schema() -> Result<()> {
    let db_path = temp_db_path("fetch-all-schemas");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE native_items (
                id INTEGER PRIMARY KEY,
                value TEXT
            )",
            [],
        )
        .expect("create native_items");
    }

    let storage = SqliteStorage::new(&db_path).await?;
    let schemas = storage.fetch_all_schemas().await?;
    drop(storage);
    fs::remove_file(&db_path).ok();

    let native_schema = schemas
        .iter()
        .find(|schema| schema.table_name == "native_items")
        .expect("native_items schema present");

    assert!(
        native_schema
            .column_defs
            .as_ref()
            .is_some_and(|cols| cols.len() == 2),
        "column definitions reflect PRAGMA fallback"
    );

    Ok(())
}

#[tokio::test]
async fn fetch_data_missing_table_returns_none() -> Result<()> {
    let storage = SqliteStorage::memory().await?;
    let result = storage.fetch_data("missing_table", &Key::I64(1)).await?;

    assert!(
        result.is_none(),
        "fetch_data should yield None for unknown tables"
    );

    Ok(())
}

#[tokio::test]
async fn scan_data_missing_table_is_empty() -> Result<()> {
    let storage = SqliteStorage::memory().await?;
    let rows = storage.scan_data("missing_table").await?;
    let collected = rows.try_collect::<Vec<_>>().await?;

    assert!(
        collected.is_empty(),
        "scan_data should return empty stream for unknown tables"
    );

    Ok(())
}

#[tokio::test]
async fn fetch_data_rowid_table_uses_rowid_branch() -> Result<()> {
    let db_path = temp_db_path("fetch-rowid");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE log_entries (
                message TEXT NOT NULL
            )",
            [],
        )
        .expect("create log_entries");

        conn.execute(
            "INSERT INTO log_entries (message) VALUES (?1)",
            ["first entry"],
        )
        .expect("insert log entry");
    }

    let storage = SqliteStorage::new(&db_path).await?;
    let row = storage
        .fetch_data("log_entries", &Key::I64(1))
        .await?
        .expect("row exists");
    drop(storage);
    fs::remove_file(&db_path).ok();

    assert_eq!(
        row,
        DataRow::Vec(vec![Value::Str("first entry".to_owned())])
    );

    Ok(())
}

#[tokio::test]
async fn append_data_no_rows_is_noop() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;
    storage.append_data("nonexistent", Vec::new()).await?;
    Ok(())
}

#[tokio::test]
async fn ensure_schema_persists_meta_for_native_tables() -> Result<()> {
    let db_path = temp_db_path("ensure-schema");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE metrics (
                name TEXT NOT NULL,
                value INTEGER
            )",
            [],
        )
        .expect("create metrics table");
    }

    {
        let storage = SqliteStorage::new(&db_path).await?;
        let schema = storage
            .fetch_schema("metrics")
            .await?
            .expect("schema exists");
        assert_eq!(schema.table_name, "metrics");
    }

    {
        let conn = Connection::open(&db_path).expect("reopen sqlite file");
        let json: Option<String> = conn
            .query_row(
                "SELECT schema_json FROM gluesql_schema WHERE table_name = 'metrics'",
                [],
                |row| row.get(0),
            )
            .optional()
            .expect("query gluesql_schema");
        assert!(json.is_some(), "schema metadata should be persisted");
    }

    fs::remove_file(&db_path).ok();
    Ok(())
}
