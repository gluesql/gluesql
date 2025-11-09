use {
    gluesql_core::{data::Value, error::Result, executor::Payload, prelude::Glue},
    gluesql_sqlite_storage::SqliteStorage,
    rusqlite::{Connection, params},
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
async fn native_glue_native_roundtrip() -> Result<()> {
    let db_path = temp_db_path("roundtrip-ngn");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                note TEXT
            )",
            [],
        )
        .expect("create contacts table");

        conn.execute(
            "INSERT INTO contacts (id, name, note) VALUES (?1, ?2, ?3)",
            params![1_i64, "Alice", "native"],
        )
        .expect("insert native row");
    }

    {
        let storage = SqliteStorage::new(&db_path).await?;
        let mut glue = Glue::new(storage);

        glue.execute("INSERT INTO contacts (id, name, note) VALUES (2, 'Bob', 'added by glue')")
            .await?;

        glue.execute("UPDATE contacts SET note = 'updated by glue' WHERE id = 1")
            .await?;
    }

    let records = {
        let conn = Connection::open(&db_path).expect("reopen sqlite file");
        let mut stmt = conn
            .prepare("SELECT id, name, note FROM contacts ORDER BY id")
            .expect("prepare select");
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .expect("query contacts")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect contacts")
    };

    assert_eq!(
        records,
        vec![
            (1, "Alice".to_owned(), Some("updated by glue".to_owned())),
            (2, "Bob".to_owned(), Some("added by glue".to_owned())),
        ]
    );

    fs::remove_file(&db_path).ok();
    Ok(())
}

#[tokio::test]
async fn glue_native_glue_roundtrip() -> Result<()> {
    let db_path = temp_db_path("roundtrip-gng");

    {
        let storage = SqliteStorage::new(&db_path).await?;
        let mut glue = Glue::new(storage);

        glue.execute(
            "CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                price FLOAT,
                name TEXT
            )",
        )
        .await?;

        glue.execute("INSERT INTO products (id, price, name) VALUES (1, 19.99, 'Keyboard')")
            .await?;
    }

    {
        let conn = Connection::open(&db_path).expect("open sqlite for native step");

        conn.execute(
            "UPDATE products SET price = ?1 WHERE id = 1",
            params![24.99_f64],
        )
        .expect("update via native");

        conn.execute(
            "INSERT INTO products (id, price, name) VALUES (?1, ?2, ?3)",
            params![2_i64, 49.5_f64, "Mouse"],
        )
        .expect("insert via native");
    }

    let payloads = {
        let storage = SqliteStorage::new(&db_path).await?;
        let mut glue = Glue::new(storage);
        glue.execute("SELECT id, price, name FROM products ORDER BY id")
            .await?
    };

    fs::remove_file(&db_path).ok();

    assert_eq!(
        payloads,
        vec![Payload::Select {
            labels: vec!["id".into(), "price".into(), "name".into()],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::F64(24.99),
                    Value::Str("Keyboard".into())
                ],
                vec![Value::I64(2), Value::F64(49.5), Value::Str("Mouse".into())],
            ],
        }]
    );

    Ok(())
}

#[tokio::test]
async fn join_native_and_glue_tables() -> Result<()> {
    let db_path = temp_db_path("roundtrip-join");

    {
        let conn = Connection::open(&db_path).expect("open sqlite file");
        conn.execute(
            "CREATE TABLE native_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
            [],
        )
        .expect("create native_users");

        conn.execute(
            "INSERT INTO native_users (id, name) VALUES (?1, ?2)",
            params![1_i64, "Alice"],
        )
        .expect("insert Alice");
        conn.execute(
            "INSERT INTO native_users (id, name) VALUES (?1, ?2)",
            params![2_i64, "Bob"],
        )
        .expect("insert Bob");
    }

    {
        let storage = SqliteStorage::new(&db_path).await?;
        let mut glue = Glue::new(storage);

        glue.execute(
            "CREATE TABLE glue_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount FLOAT
            )",
        )
        .await?;

        glue.execute("INSERT INTO glue_orders (id, user_id, amount) VALUES (1, 1, 10.0)")
            .await?;
        glue.execute("INSERT INTO glue_orders (id, user_id, amount) VALUES (2, 2, 20.5)")
            .await?;

        let result = glue
            .execute(
                "SELECT u.name, o.amount
                 FROM native_users u
                 JOIN glue_orders o ON o.user_id = u.id
                 ORDER BY o.id",
            )
            .await?;

        assert_eq!(
            result,
            vec![Payload::Select {
                labels: vec!["name".into(), "amount".into()],
                rows: vec![
                    vec![Value::Str("Alice".into()), Value::F64(10.0)],
                    vec![Value::Str("Bob".into()), Value::F64(20.5)],
                ],
            }]
        );
    }

    fs::remove_file(&db_path).ok();
    Ok(())
}
