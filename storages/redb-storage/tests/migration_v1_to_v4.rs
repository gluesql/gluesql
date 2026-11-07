//! Constructed database scenarios for the complete v1 -> v4 -> latest path.
#[path = "migration/database.rs"]
pub mod database;

use {
    bincode::serialize,
    database::{META, SCHEMAS, Snapshot, TestFile, replace_row, snapshot, write_database},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
        prelude::{Glue, Payload},
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    redb::{Database, TableDefinition, TableError},
    std::collections::BTreeMap,
};

#[test]
fn missing_row_table_stops_before_physical_upgrade_or_payload_changes() {
    let file = TestFile::fixture("v1/defaults.redb");
    let original = snapshot(&file);
    let rows = TableDefinition::<&[u8], Vec<u8>>::new("Defaults");
    {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        assert!(txn.delete_table(rows).unwrap());
        txn.commit().unwrap();
    }

    assert_eq!(
        migrate_to_latest(&file.0),
        Err(Error::StorageMsg("Table 'Defaults' does not exist".into()))
    );
    let mut db = Database::open(&file.0).unwrap();
    {
        let txn = db.begin_read().unwrap();
        assert!(matches!(
            txn.open_table(rows),
            Err(TableError::TableDoesNotExist(name)) if name == "Defaults"
        ));
        assert!(matches!(
            txn.open_table(META),
            Err(TableError::TableDoesNotExist(name)) if name == "__GLUESQL_META__"
        ));
        assert_eq!(
            txn.open_table(SCHEMAS)
                .unwrap()
                .get("Defaults")
                .unwrap()
                .unwrap()
                .value(),
            original.schemas["Defaults"]
        );
    }
    assert!(
        db.upgrade().unwrap(),
        "preflight must leave the old file format"
    );
}

#[test]
fn failed_v1_row_conversion_rolls_back_payloads_and_retry_uses_upgraded_file() {
    let file = TestFile::fixture("v1/defaults.redb");
    let original = snapshot(&file);
    // The valid Defaults table is rewritten before the later table fails, so
    // this checks transaction rollback after actual writes, not just preflight.
    let (key, valid_row) = &original.rows["Defaults"][0];
    {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        let mut schema = original.schemas["Defaults"].clone();
        assert_eq!(&schema[8..16], b"Defaults");
        schema[8..16].copy_from_slice(b"ZFailure");
        txn.open_table(SCHEMAS)
            .unwrap()
            .insert("ZFailure", schema)
            .unwrap();
        txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("ZFailure"))
            .unwrap()
            .insert(key.as_slice(), vec![255])
            .unwrap();
        txn.commit().unwrap();
    }
    let before = snapshot(&file);
    assert!(
        migrate_to_latest(&file.0)
            .unwrap_err()
            .to_string()
            .contains("failed to parse v1 row payload")
    );
    assert_eq!(snapshot(&file), before);
    let mut db = Database::open(&file.0).unwrap();
    assert!(
        !db.upgrade().unwrap(),
        "the logical payload rollback keeps the independent physical upgrade"
    );
    drop(db);
    replace_row(&file, "ZFailure", key, valid_row);
    assert_eq!(migrate_to_latest(&file.0).unwrap().rewritten_rows, 2);
    assert_eq!(snapshot(&file).version, Some(5));
}

#[test]
fn physical_upgrade_failure_preserves_logical_payloads_and_can_be_retried() {
    let file = TestFile::fixture("v1/defaults.redb");
    let savepoint = {
        let db = Database::open(&file.0).unwrap();
        let txn = db.begin_write().unwrap();
        let id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        id
    };
    let before = snapshot(&file);
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
    assert_eq!(migrate_to_latest(&file.0).unwrap().rewritten_rows, 1);
    assert_eq!(snapshot(&file).version, Some(5));
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

#[test]
fn batching_visits_every_key_with_old_and_already_converted_rows_interleaved() {
    let file = TestFile::empty();
    let schema = Schema::from_ddl("CREATE TABLE Mixed;").unwrap();
    let mut source_rows = Vec::new();
    let mut expected_rows = Vec::new();
    // 4500 rewrites cross the 4096 batch boundary; 1500 current rows also
    // advance the scan cursor without adding a pending rewrite.
    for id in 0..6000_i64 {
        let key = Key::I64(id);
        let table_key = key.to_cmp_be_bytes().unwrap();
        let (input, expected) = match id % 4 {
            0 => {
                let values = vec![Value::I64(id), Value::Str("already current".into())];
                (serialize(&(&key, &values)).unwrap(), values)
            }
            2 => {
                let values = BTreeMap::from([("id".into(), Value::I64(id))]);
                (
                    serialize(&(&key, 1_u32, &values)).unwrap(),
                    vec![Value::Map(values)],
                )
            }
            _ => {
                let values = vec![Value::I64(id), Value::Str("old vector".into())];
                (serialize(&(&key, 0_u32, &values)).unwrap(), values)
            }
        };
        source_rows.push((table_key.clone(), input));
        expected_rows.push((table_key, serialize(&(key, expected)).unwrap()));
    }
    source_rows.sort_by(|a, b| a.0.cmp(&b.0));
    expected_rows.sort_by(|a, b| a.0.cmp(&b.0));
    write_database(
        &file,
        false,
        &Snapshot {
            version: None,
            schemas: BTreeMap::from([("Mixed".into(), serialize(&schema).unwrap())]),
            rows: BTreeMap::from([("Mixed".into(), source_rows)]),
        },
    );
    let before = snapshot(&file);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 0,
            rewritten_rows: 4500
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(5));
    assert_eq!(after.schemas, before.schemas);
    assert_eq!(after.rows["Mixed"], expected_rows);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 0,
            unchanged_tables: 1,
            rewritten_rows: 0
        }
    );
    assert_eq!(snapshot(&file), after);
    RedbStorage::new(&file.0).unwrap();
}

#[test]
fn invalid_schema_table_type_fails_before_creating_metadata() {
    let file = TestFile::empty();
    let db = Database::create(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("__SCHEMA__"))
        .unwrap()
        .insert(b"Foo".as_slice(), vec![1])
        .unwrap();
    txn.commit().unwrap();
    let expected = db
        .begin_read()
        .unwrap()
        .open_table(SCHEMAS)
        .unwrap_err()
        .to_string();
    drop(db);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap_err(),
        Error::StorageMsg(expected)
    );
    let db = Database::open(&file.0).unwrap();
    assert!(matches!(
        db.begin_read().unwrap().open_table(database::META),
        Err(redb::TableError::TableDoesNotExist(_))
    ));
}
