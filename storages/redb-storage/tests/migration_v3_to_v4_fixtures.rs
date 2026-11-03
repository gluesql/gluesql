//! Fixed source databases exercised through the public migration API.
#[path = "migration/database.rs"]
pub mod database;

use {
    bincode::deserialize,
    database::{TestFile, snapshot},
    gluesql_core::{
        data::{Key, Schema, Value},
        prelude::{Glue, Payload},
        store::Store,
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    redb::Database,
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
    assert!(
        glue.execute("INSERT INTO Defaults (id) VALUES (2);")
            .is_err()
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
fn defaults_migrate_from_v3_to_v4_and_remain_usable_after_reopen() {
    let file = TestFile::fixture("v3/upgraded_v1_defaults.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, Some(3));
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 0
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(4));
    assert_ne!(after.schemas, before.schemas);
    assert_eq!(after.rows, before.rows);
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
fn current_only_schema_and_rows_are_not_rewritten() {
    let file = TestFile::fixture("v3/current_defaults.redb");
    let before = snapshot(&file);
    assert_eq!(before.version, Some(3));
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 0,
            unchanged_tables: 1,
            rewritten_rows: 0,
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(4));
    assert_eq!(after.schemas, before.schemas);
    assert_eq!(after.rows, before.rows);
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("INSERT INTO Modern (id) VALUES (2);").unwrap();
    drop(glue);
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        select(&mut glue, "SELECT * FROM Modern ORDER BY id;"),
        vec![
            vec![
                Value::I64(1),
                Value::Str("modern-default".into()),
                Value::Bool(true)
            ],
            vec![
                Value::I64(2),
                Value::Str("modern-default".into()),
                Value::Bool(true)
            ],
        ]
    );
}

#[test]
fn identical_bytes_with_different_default_meanings_stop_both_origins() {
    let old = TestFile::fixture("v3/upgraded_v1_typed_default.redb");
    let new = TestFile::fixture("v3/current_ambiguous.redb");
    let old_snapshot = snapshot(&old);
    let new_snapshot = snapshot(&new);
    assert_eq!(old_snapshot.version, Some(3));
    assert_eq!(new_snapshot.version, Some(3));
    assert_eq!(old_snapshot.schemas, new_snapshot.schemas);
    // Actual old SQL stored true; actual v0.20 Decimal AST stored false.
    for (file, expected) in [(&old, true), (&new, false)] {
        let before = snapshot(file);
        let (_, payload) = &before.rows["Ambiguous"][0];
        let (_, row): (Key, Vec<Value>) = deserialize(payload).unwrap();
        assert_eq!(row[1], Value::Bool(expected));
        let error = migrate_to_latest(&file.0).unwrap_err().to_string();
        assert!(
            error.contains("ambiguous schema in table 'Ambiguous'"),
            "{error}"
        );
        assert_eq!(snapshot(file), before);
    }
}
