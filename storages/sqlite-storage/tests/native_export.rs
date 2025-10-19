use {
    gluesql_core::{error::Result, prelude::Glue},
    gluesql_sqlite_storage::SqliteStorage,
    rusqlite::Connection,
    serde_json::{Value as JsonValue, json},
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
async fn write_with_gluesql_read_with_rusqlite() -> Result<()> {
    let db_path = temp_db_path("glue-to-native");

    let storage = SqliteStorage::new(&db_path).await?;
    let mut glue = Glue::new(storage.clone());

    glue.execute(
        "
        CREATE TABLE glue_structured (
            id INTEGER PRIMARY KEY,
            uuid_field UUID,
            decimal_field DECIMAL,
            bool_field BOOLEAN,
            bytea_field BYTEA,
            text_field TEXT
        );
        ",
    )
    .await?;

    glue.execute(
        "
        INSERT INTO glue_structured
            (id, uuid_field, decimal_field, bool_field, bytea_field, text_field)
        VALUES
            (
                1,
                '550e8400-e29b-41d4-a716-446655440000',
                123.456,
                TRUE,
                X'000102',
                'alpha'
            ),
            (
                2,
                'c8fe3e9d-9f4d-4ac7-8a4f-37d6b7f31a0b',
                -987.0001,
                FALSE,
                X'FFEE',
                'beta'
            );
        ",
    )
    .await?;

    glue.execute("CREATE TABLE glue_schemaless;").await?;
    glue.execute(
        r#"
        INSERT INTO glue_schemaless VALUES
            ('{"id":1,"message":"hello","flag":true,"values":[1,2,3]}'),
            ('{"id":2,"message":"world","flag":false,"values":[4,5]}');
        "#,
    )
    .await?;

    drop(glue);
    drop(storage);

    let conn = Connection::open(&db_path).expect("open sqlite file");

    let structured_rows = {
        let mut stmt = conn
            .prepare(
                "SELECT id, uuid_field, decimal_field, bool_field, bytea_field, text_field \
                 FROM glue_structured ORDER BY id",
            )
            .expect("prepare structured select");
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .expect("query structured rows")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect structured rows")
    };

    assert_eq!(
        structured_rows,
        vec![
            (
                1,
                "550e8400-e29b-41d4-a716-446655440000".to_owned(),
                "123.456".to_owned(),
                1,
                vec![0x00, 0x01, 0x02],
                "alpha".to_owned(),
            ),
            (
                2,
                "c8fe3e9d-9f4d-4ac7-8a4f-37d6b7f31a0b".to_owned(),
                "-987.0001".to_owned(),
                0,
                vec![0xFF, 0xEE],
                "beta".to_owned(),
            ),
        ]
    );

    let schemaless_rows = {
        let mut stmt = conn
            .prepare(r#"SELECT rowid, "_gluesql_payload" FROM glue_schemaless ORDER BY rowid"#)
            .expect("prepare schemaless select");
        stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query schemaless rows")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect schemaless rows")
    };

    let parsed_json: Vec<(i64, JsonValue)> = schemaless_rows
        .into_iter()
        .map(|(rowid, payload)| {
            let value: JsonValue =
                serde_json::from_str(&payload).expect("parse schemaless payload as json");
            (rowid, value)
        })
        .collect();

    assert_eq!(
        parsed_json,
        vec![
            (
                1,
                json!({"flag": true, "id": 1, "message": "hello", "values": [1, 2, 3]})
            ),
            (
                2,
                json!({"flag": false, "id": 2, "message": "world", "values": [4, 5]})
            ),
        ]
    );

    fs::remove_file(&db_path).ok();

    Ok(())
}
