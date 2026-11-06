//! Fixed source databases exercised through the public migration API.
#[path = "migration/database.rs"]
pub mod database;

use {
    bincode::deserialize,
    database::{TestFile, snapshot},
    gluesql_core::{
        data::{Key, Schema, Value},
        executor::ValidateError,
        prelude::{
            Glue, Payload,
            Value::{Bool, I64, List, Map, Str},
        },
        store::Store,
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    redb::Database,
    std::collections::BTreeMap,
};

const DEFAULTS_DDL: &str = "CREATE TABLE Defaults (
    id INT PRIMARY KEY,
    label TEXT DEFAULT 'legacy-default',
    score INT DEFAULT (2 + 3),
    ready BOOLEAN DEFAULT NOT FALSE,
    pattern BOOLEAN DEFAULT 'abc' LIKE 'a%'
);";

fn select(glue: &mut Glue<RedbStorage>, sql: &str) -> Vec<Vec<Value>> {
    let payloads = glue.execute(sql).expect("execute SQL after migration");
    let [Payload::Select { rows, .. }] = payloads.as_slice() else {
        panic!("expected SELECT: {payloads:?}");
    };
    rows.clone()
}

fn assert_defaults(file: &TestFile) {
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    let expected_schema = Schema::from_ddl(DEFAULTS_DDL).unwrap();
    assert_eq!(
        glue.storage.fetch_schema("Defaults").unwrap(),
        Some(expected_schema)
    );
    let expected_row = |id| {
        vec![
            Value::I64(id),
            Value::Str("legacy-default".to_owned()),
            Value::I64(5),
            Value::Bool(true),
            Value::Bool(true),
        ]
    };
    assert_eq!(
        select(&mut glue, "SELECT * FROM Defaults WHERE id = 1;"),
        vec![expected_row(1)]
    );
    glue.execute("INSERT INTO Defaults (id) VALUES (2);")
        .unwrap();
    assert_eq!(
        select(&mut glue, "SELECT * FROM Defaults WHERE id = 2;"),
        vec![expected_row(2)]
    );
    assert_eq!(
        glue.execute("INSERT INTO Defaults (id) VALUES (2);"),
        Err(ValidateError::DuplicateEntryOnPrimaryKeyField(Key::I64(2)).into())
    );
    glue.execute("ALTER TABLE Defaults ADD COLUMN extra INT DEFAULT (4 + 5);")
        .unwrap();
    glue.execute("INSERT INTO Defaults (id) VALUES (3);")
        .unwrap();
    assert_eq!(
        select(&mut glue, "SELECT extra FROM Defaults WHERE id = 3;"),
        vec![vec![Value::I64(9)]]
    );
    drop(glue);
    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        select(&mut reopened, "SELECT extra FROM Defaults WHERE id = 3;"),
        vec![vec![Value::I64(9)]]
    );
}

#[test]
fn defaults_migrate_from_v1_to_latest_and_remain_usable_after_reopen() {
    let file = TestFile::fixture("v1/defaults.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, None);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 1
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(5));
    assert_ne!(after.schemas, before.schemas);
    assert_eq!(after.rows["Defaults"][0].0, before.rows["Defaults"][0].0);
    let (key, row): (Key, Vec<Value>) = deserialize(&after.rows["Defaults"][0].1).unwrap();
    assert_eq!(key, Key::I64(1));
    assert_eq!(
        row,
        vec![
            Value::I64(1),
            Value::Str("legacy-default".into()),
            Value::I64(5),
            Value::Bool(true),
            Value::Bool(true)
        ]
    );
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 0,
            unchanged_tables: 1,
            rewritten_rows: 0
        }
    );
    assert_eq!(snapshot(&file), after);
    let mut db = Database::open(&file.0).unwrap();
    assert!(!db.upgrade().unwrap());
    drop(db);
    assert_defaults(&file);
}

#[test]
fn typed_defaults_use_the_known_v1_origin() {
    let file = TestFile::fixture("v1/typed_default.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, None);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 1
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(5));
    let expected = Schema::from_ddl("CREATE TABLE Ambiguous (id INT PRIMARY KEY, flag BOOLEAN DEFAULT TEXT '2' BETWEEN TEXT '10' AND TEXT '9');").unwrap();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        glue.storage.fetch_schema("Ambiguous").unwrap(),
        Some(expected)
    );
    glue.execute("INSERT INTO Ambiguous (id) VALUES (2);")
        .unwrap();
    drop(glue);
    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        select(&mut reopened, "SELECT flag FROM Ambiguous ORDER BY id;"),
        vec![vec![Value::Bool(true)], vec![Value::Bool(true)]]
    );
}

#[test]
fn migrate_v1_mixed_schema_schemaless_fixture() {
    let file = TestFile::fixture("v1/mixed_schema_schemaless.redb");
    let path = &file.0;
    let before = snapshot(&file);
    assert_eq!(before.version, None);

    let first = migrate_to_latest(path).expect("migrate fixture");
    assert_eq!(first.migrated_tables, 2);
    assert_eq!(first.unchanged_tables, 0);
    assert_eq!(first.rewritten_rows, 4);

    let second = migrate_to_latest(path).expect("migrate fixture twice");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 2);
    assert_eq!(second.rewritten_rows, 0);

    assert_eq!(snapshot(&file).version, Some(5));
    let storage = RedbStorage::new(path).expect("open migrated storage");
    let mut glue = Glue::new(storage);

    let user_rows = glue
        .execute("SELECT id, name, active FROM User ORDER BY id;")
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
        .expect("insert User row");
    assert_eq!(inserted_user, vec![Payload::Insert(1)]);

    let user_count = glue
        .execute("SELECT COUNT(*) AS cnt FROM User;")
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
        .expect("insert Event row");
    assert_eq!(inserted_event, vec![Payload::Insert(1)]);

    let inserted_event_kind = glue
        .execute("SELECT kind FROM Event WHERE event_id = 3;")
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

const QUERY_DDL: &str = "CREATE TABLE Queries (
    id INT,
    value INT DEFAULT (
        SELECT SUM(A.id + 1) FROM Numbers AS A
        INNER JOIN Numbers AS B ON A.id = B.id
        WHERE A.id > 1 GROUP BY A.id
        HAVING SUM(A.id) > 1 ORDER BY A.id LIMIT 3 OFFSET 1
    )
);";

#[test]
fn released_hash_join_schema_is_restored_and_persisted() {
    let file = TestFile::fixture("v1/hash_join.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, None);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 0
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(5));
    assert_eq!(after.rows, before.rows);
    assert_eq!(
        RedbStorage::new(&file.0)
            .unwrap()
            .fetch_schema("Queries")
            .unwrap(),
        Some(Schema::from_ddl(QUERY_DDL).unwrap())
    );
}

#[test]
fn unrecoverable_released_index_plan_keeps_the_v1_database_intact() {
    let file = TestFile::fixture("v1/index_plan.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, None);
    let error = migrate_to_latest(&file.0).unwrap_err().to_string();
    assert!(
        error.contains("index access plan requires explicit recovery"),
        "{error}"
    );
    assert_eq!(snapshot(&file), before);
}
