use {
    gluesql_core::{
        ast::{Expr, Literal},
        data::{Interval, Key, Point, Schema, Value},
        store::{Store, StoreMut, Transaction},
    },
    gluesql_sqlite_storage::SqliteStorage,
    rusqlite::Connection,
    std::collections::BTreeMap,
    tempfile::tempdir,
};

#[test]
fn reopen_preserves_types_defaults_and_comments() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("values.sqlite");

    // Reopening must recover the logical types that share SQLite storage classes.
    let mut schema = Schema::from_ddl(
        "CREATE TABLE typed (
             id INT PRIMARY KEY,
             boolean BOOLEAN,
             i8 INT8,
             i16 INT16,
             i32 INT32,
             i128 INT128,
             u8 UINT8,
             u16 UINT16,
             u32 UINT32,
             u64 UINT64,
             u128 UINT128,
             f32 FLOAT32,
             f64 FLOAT,
             text TEXT,
             bytes BYTEA,
             address INET,
             date DATE,
             time TIME,
             stamp TIMESTAMP,
             interval INTERVAL,
             uuid UUID,
             map MAP,
             list LIST,
             decimal DECIMAL DEFAULT 1.2300 COMMENT '',
             point POINT,
             fallback TEXT DEFAULT TRIM(LEADING 'a' FROM 'apple') COMMENT 'fallback',
             birthday DATE DEFAULT DATE '2026-11-11'
         ) COMMENT = 'types'",
    )
    .unwrap();

    // SQLite identifier quoting must also work for names supplied through Store.
    schema.table_name = "typed\"table".to_owned();
    let text_column = schema
        .column_defs
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|column| column.name == "text")
        .unwrap();
    text_column.name = "text\"column".to_owned();
    text_column.default = Some(Expr::Literal(Literal::QuotedString("한글\0'".to_owned())));

    let row = vec![
        Value::I64(7),
        Value::Bool(true),
        Value::I8(-128),
        Value::I16(-32768),
        Value::I32(i32::MIN),
        Value::I128(i128::MIN),
        Value::U8(255),
        Value::U16(65535),
        Value::U32(u32::MAX),
        Value::U64(u64::MAX),
        Value::U128(u128::MAX),
        Value::F32(1.25),
        Value::F64(-2.5),
        Value::Str("한글\0'".to_owned()),
        Value::Bytea(vec![0, 128, 255]),
        Value::Inet("2001:db8::1".parse().unwrap()),
        Value::Date("2026-11-11".parse().unwrap()),
        Value::Time("12:34:56.123456789".parse().unwrap()),
        Value::Timestamp("2026-11-11T12:34:56.123456789".parse().unwrap()),
        Value::Interval(Interval::Microsecond(i64::MIN)),
        Value::Uuid(u128::MAX),
        Value::Map(BTreeMap::from([(
            "price".to_owned(),
            Value::Decimal("1.2300".parse().unwrap()),
        )])),
        Value::List(vec![Value::Null, Value::I16(3)]),
        Value::Decimal("1.2300".parse().unwrap()),
        Value::Point(Point::new(1.25, -0.0)),
        Value::Null,
        Value::Null,
    ];

    {
        let mut storage = SqliteStorage::new(&path).unwrap();
        storage.begin(false).unwrap();
        storage.insert_schema(&schema).unwrap();
        storage
            .insert_data(&schema.table_name, vec![(Key::I64(7), row.clone())])
            .unwrap();
        storage.commit().unwrap();
    }

    let storage = SqliteStorage::new(&path).unwrap();
    assert_eq!(
        storage.fetch_schema(&schema.table_name).unwrap(),
        Some(schema.clone())
    );
    assert_eq!(
        storage
            .fetch_data(&schema.table_name, &Key::I64(7))
            .unwrap(),
        Some(row)
    );

    // Inspect the physical representation independently of GlueSQL's decoding.
    let sqlite = Connection::open(path).unwrap();
    let physical: (String, String, String, String, String, String) = sqlite
        .query_row(
            r#"SELECT typeof(i8), typeof(f32), typeof(bytes), typeof(decimal), decimal, interval
               FROM "typed""table""#,
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    let (integer_type, real_type, blob_type, decimal_type, decimal, interval) = physical;
    assert_eq!(integer_type, "integer");
    assert_eq!(real_type, "real");
    assert_eq!(blob_type, "blob");
    assert_eq!(decimal_type, "text");
    assert_eq!(decimal, "1.2300");
    assert_eq!(interval, "Interval(Microsecond(-9223372036854775808))");

    // Defaults SQLite cannot express must be retained as GlueSQL SQL in metadata.
    let defaults = sqlite
        .prepare(
            "SELECT column_name, default_sql
             FROM __gluesql_column_meta
             WHERE table_name = ?1 AND default_sql IS NOT NULL
             ORDER BY column_name",
        )
        .unwrap()
        .query_map([&schema.table_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        defaults,
        vec![
            ("birthday".to_owned(), "DATE '2026-11-11'".to_owned()),
            (
                "fallback".to_owned(),
                "TRIM(LEADING 'a' FROM 'apple')".to_owned()
            ),
            ("text\"column".to_owned(), "'한글\0'''".to_owned()),
        ]
    );
}

#[test]
fn reopen_preserves_schemaless_documents() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("documents.sqlite");
    let items = Schema::from_ddl("CREATE TABLE items (id INT)").unwrap();
    let mut documents = Schema::from_ddl("CREATE TABLE documents").unwrap();
    documents.comment = Some(String::new());
    let row = vec![Value::Map(BTreeMap::from([(
        "items".to_owned(),
        Value::List(vec![Value::U128(u128::MAX), Value::Bool(false)]),
    )]))];

    {
        let mut storage = SqliteStorage::new(&path).unwrap();
        storage.begin(false).unwrap();
        storage.insert_schema(&items).unwrap();
        storage.insert_schema(&documents).unwrap();
        storage.append_data("documents", vec![row.clone()]).unwrap();
        storage.commit().unwrap();
    }

    let storage = SqliteStorage::new(&path).unwrap();
    assert_eq!(
        storage.fetch_schema("documents").unwrap(),
        Some(documents.clone())
    );
    assert_eq!(
        storage.fetch_data("documents", &Key::I64(1)).unwrap(),
        Some(row)
    );

    let schemas = storage.fetch_all_schemas().unwrap();
    assert_eq!(schemas.len(), 2);
    assert!(schemas.contains(&documents));
    assert!(schemas.contains(&items));

    let sqlite = Connection::open(path).unwrap();
    let strict_count: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM pragma_table_list WHERE strict = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(strict_count, 4, "two user tables and two metadata tables");
}
