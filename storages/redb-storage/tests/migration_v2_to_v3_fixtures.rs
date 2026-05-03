use {
    bincode::serialize,
    gluesql_core::{
        data::{Key, Schema, Value},
        prelude::{
            Glue, Payload,
            Value::{Bool, I64, List, Map, Str},
        },
    },
    gluesql_redb_storage::{RedbStorage, migrate_to_latest},
    redb::{Database, TableDefinition},
    std::{
        collections::BTreeMap,
        fs::{copy, create_dir, remove_file},
    },
    uuid::Uuid,
};

const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("__SCHEMA__");
const META_TABLE: TableDefinition<&str, u32> = TableDefinition::new("__GLUESQL_META__");
const STORAGE_META_VERSION_KEY: &str = "storage_format_version";
const V2_FORMAT_VERSION: u32 = 2;

struct FileGuard {
    path: String,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Err(err) = remove_file(&self.path) {
            eprintln!("remove_file error: {err:?}");
        }
    }
}

fn fixture_to_tmp(fixture_name: &str) -> (String, FileGuard) {
    let _ = create_dir("tmp");
    let source = format!("./tests/fixtures/v2/{fixture_name}.redb");
    let target = format!("./tmp/{fixture_name}-v2-{}.redb", Uuid::now_v7());
    copy(&source, &target).expect("copy fixture");
    (target.clone(), FileGuard { path: target })
}

/// Writes a `GlueSQL` v2 format database (redb file format v2) for use as a test fixture.
/// Run with: cargo test -p gluesql-redb-storage `generate_v2_mixed_schema_schemaless_fixture` -- --ignored
#[test]
#[ignore = "fixture generator: run manually to regenerate tests/fixtures/v2/mixed_schema_schemaless.redb"]
fn generate_v2_mixed_schema_schemaless_fixture() {
    let output = "./tests/fixtures/v2/mixed_schema_schemaless.redb";

    // Database::create uses the default builder → redb file format v2.
    let db = Database::create(output).expect("create v2 fixture database");
    let txn = db.begin_write().expect("begin write transaction");

    // --- User table (schema-based) ---
    let user_schema =
        Schema::from_ddl("CREATE TABLE User (id INTEGER, name TEXT, active BOOLEAN);")
            .expect("parse User schema");
    let user_table_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("User");

    let mut schema_table = txn.open_table(SCHEMA_TABLE).expect("open schema table");
    schema_table
        .insert(
            "User",
            serialize(&user_schema).expect("serialize User schema"),
        )
        .expect("insert User schema");
    drop(schema_table);

    let mut user_table = txn.open_table(user_table_def).expect("open User table");
    for (key, row) in [
        (
            Key::I64(1),
            vec![
                Value::I64(1),
                Value::Str("Alice".to_owned()),
                Value::Bool(true),
            ],
        ),
        (
            Key::I64(2),
            vec![
                Value::I64(2),
                Value::Str("Bob".to_owned()),
                Value::Bool(false),
            ],
        ),
    ] {
        let k = key.to_cmp_be_bytes().expect("key bytes");
        user_table
            .insert(
                k.as_slice(),
                serialize(&(&key, row)).expect("serialize row"),
            )
            .expect("insert User row");
    }
    drop(user_table);

    // --- Event table (schemaless) ---
    let event_schema = Schema::from_ddl("CREATE TABLE Event;").expect("parse Event schema");
    let event_table_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("Event");

    let mut schema_table = txn.open_table(SCHEMA_TABLE).expect("open schema table");
    schema_table
        .insert(
            "Event",
            serialize(&event_schema).expect("serialize Event schema"),
        )
        .expect("insert Event schema");
    drop(schema_table);

    let mut event_table = txn.open_table(event_table_def).expect("open Event table");

    let event1_meta = Value::Map(BTreeMap::from([(
        "ip".to_owned(),
        Value::Str("10.0.0.1".to_owned()),
    )]));
    let event1 = vec![Value::Map(BTreeMap::from([
        ("event_id".to_owned(), Value::I64(1)),
        ("kind".to_owned(), Value::Str("login".to_owned())),
        ("meta".to_owned(), event1_meta),
        (
            "tags".to_owned(),
            Value::List(vec![
                Value::Str("auth".to_owned()),
                Value::Str("web".to_owned()),
            ]),
        ),
    ]))];

    let event2_meta = Value::Map(BTreeMap::from([(
        "ip".to_owned(),
        Value::Str("10.0.0.2".to_owned()),
    )]));
    let event2 = vec![Value::Map(BTreeMap::from([
        ("event_id".to_owned(), Value::I64(2)),
        ("kind".to_owned(), Value::Str("purchase".to_owned())),
        ("amount".to_owned(), Value::I64(199)),
        ("meta".to_owned(), event2_meta),
    ]))];

    for (key, row) in [(Key::I64(1), event1), (Key::I64(2), event2)] {
        let k = key.to_cmp_be_bytes().expect("key bytes");
        event_table
            .insert(
                k.as_slice(),
                serialize(&(&key, row)).expect("serialize row"),
            )
            .expect("insert Event row");
    }
    drop(event_table);

    // Write GlueSQL format version 2.
    let mut meta = txn.open_table(META_TABLE).expect("open metadata table");
    meta.insert(STORAGE_META_VERSION_KEY, &V2_FORMAT_VERSION)
        .expect("insert format version");
    drop(meta);

    txn.commit().expect("commit");

    println!("Fixture written to {output}");
}

#[tokio::test]
async fn migrate_v2_mixed_schema_schemaless_fixture() {
    let (path, _guard) = fixture_to_tmp("mixed_schema_schemaless");

    let first = migrate_to_latest(&path).expect("migrate fixture");
    assert_eq!(first.migrated_tables, 0);
    assert_eq!(first.unchanged_tables, 2);
    assert_eq!(first.rewritten_rows, 0);

    let second = migrate_to_latest(&path).expect("migrate fixture twice");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 2);
    assert_eq!(second.rewritten_rows, 0);

    let storage = RedbStorage::new(&path).expect("open migrated storage");
    let mut glue = Glue::new(storage);

    let user_rows = glue
        .execute("SELECT id, name, active FROM User ORDER BY id;")
        .await
        .expect("select User");
    assert_eq!(
        user_rows,
        vec![Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned(), "active".to_owned()],
            rows: vec![
                vec![I64(1), Str("Alice".to_owned()), Bool(true)],
                vec![I64(2), Str("Bob".to_owned()), Bool(false)],
            ],
        }],
    );

    let inserted_user = glue
        .execute("INSERT INTO User VALUES (3, 'Carol', TRUE);")
        .await
        .expect("insert User row");
    assert_eq!(inserted_user, vec![Payload::Insert(1)]);

    let user_count = glue
        .execute("SELECT COUNT(*) AS cnt FROM User;")
        .await
        .expect("count User rows");
    assert_eq!(
        user_count,
        vec![Payload::Select {
            labels: vec!["cnt".to_owned()],
            rows: vec![vec![I64(3)]],
        }],
    );

    let event_query = glue
        .execute("SELECT kind, meta['ip'] AS ip FROM Event WHERE event_id = 1;")
        .await
        .expect("select Event projection");
    assert_eq!(
        event_query,
        vec![Payload::Select {
            labels: vec!["kind".to_owned(), "ip".to_owned()],
            rows: vec![vec![Str("login".to_owned()), Str("10.0.0.1".to_owned())]],
        }],
    );

    let inserted_event = glue
        .execute(
            "INSERT INTO Event VALUES ('{\"event_id\":3,\"kind\":\"logout\",\"meta\":{\"ip\":\"10.0.0.3\"}}');",
        )
        .await
        .expect("insert Event row");
    assert_eq!(inserted_event, vec![Payload::Insert(1)]);

    let inserted_event_kind = glue
        .execute("SELECT kind FROM Event WHERE event_id = 3;")
        .await
        .expect("select inserted Event");
    assert_eq!(
        inserted_event_kind,
        vec![Payload::Select {
            labels: vec!["kind".to_owned()],
            rows: vec![vec![Str("logout".to_owned())]],
        }],
    );

    let event_map_rows = glue
        .execute("SELECT * FROM Event WHERE event_id = 1;")
        .await
        .expect("select schemaless Event rows");
    assert_eq!(
        event_map_rows,
        vec![Payload::SelectMap(vec![BTreeMap::from([
            ("event_id".to_owned(), I64(1)),
            ("kind".to_owned(), Str("login".to_owned())),
            (
                "meta".to_owned(),
                Map(BTreeMap::from([(
                    "ip".to_owned(),
                    Str("10.0.0.1".to_owned())
                )])),
            ),
            (
                "tags".to_owned(),
                List(vec![Str("auth".to_owned()), Str("web".to_owned())]),
            ),
        ])])],
    );
}
