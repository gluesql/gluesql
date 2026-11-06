//! Constructed database scenarios for v2 -> v4.
#[path = "migration/database.rs"]
pub mod database;

use {
    bincode::serialize,
    database::{Snapshot, TestFile, snapshot, write_database},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
        prelude::{Glue, Payload},
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    redb::Database,
    std::collections::BTreeMap,
};

#[test]
fn physical_upgrade_preserves_schema_row_and_lookup_key_bytes() {
    let file = TestFile::empty();
    let key = Key::I64(17);
    let row = serialize(&(&key, vec![Value::I64(17), Value::Str("stored".into())])).unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Item (id INT PRIMARY KEY, label TEXT);").unwrap();
    write_database(
        &file,
        false,
        &Snapshot {
            version: Some(2),
            schemas: BTreeMap::from([("Item".into(), serialize(&schema).unwrap())]),
            rows: BTreeMap::from([("Item".into(), vec![(key.to_cmp_be_bytes().unwrap(), row)])]),
        },
    );
    let before = snapshot(&file);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 0,
            unchanged_tables: 1,
            rewritten_rows: 0
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(4));
    assert_eq!(after.schemas, before.schemas);
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
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        glue.execute("SELECT label FROM Item WHERE id = 17;")
            .unwrap(),
        vec![Payload::Select {
            labels: vec!["label".into()],
            rows: vec![vec![Value::Str("stored".into())]]
        }]
    );
}

#[test]
fn physical_upgrade_failure_keeps_v2_payloads_and_allows_retry() {
    let file = TestFile::fixture("v2/defaults.redb");
    let savepoint = {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        let id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        id
    };
    let before = snapshot(&file);
    assert_eq!(before.version, Some(2));
    assert_eq!(
        migrate_to_latest(&file.0),
        Err(Error::StorageMsg(
            "Persistent savepoint exists. Operation cannot be performed.".into()
        ))
    );
    assert_eq!(snapshot(&file), before);
    {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        assert!(txn.delete_persistent_savepoint(savepoint).unwrap());
        txn.commit().unwrap();
    }
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
    assert_eq!(after.rows, before.rows);
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("INSERT INTO Defaults (id) VALUES (2);")
        .unwrap();
    drop(glue);
    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        reopened
            .execute("SELECT score FROM Defaults WHERE id = 2;")
            .unwrap(),
        vec![Payload::Select {
            labels: vec!["score".into()],
            rows: vec![vec![Value::I64(5)]]
        }]
    );
}
