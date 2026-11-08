//! Constructed database scenarios for v3 -> v4 -> latest.
#[path = "migration/database.rs"]
pub mod database;

use {
    bincode::serialize,
    database::{Snapshot, TestFile, snapshot, write_database},
    gluesql_core::{
        data::{Schema, Value},
        prelude::{Glue, Payload},
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    std::collections::BTreeMap,
};

#[test]
fn mixed_historical_and_current_tables_commit_together_and_remain_usable() {
    let old = snapshot(&TestFile::fixture("v3/upgraded_v1_defaults.redb"));
    let new = snapshot(&TestFile::fixture("v3/current_defaults.redb"));
    let file = TestFile::empty();
    write_database(
        &file,
        true,
        &Snapshot {
            version: Some(3),
            schemas: old.schemas.into_iter().chain(new.schemas).collect(),
            rows: old.rows.into_iter().chain(new.rows).collect(),
        },
    );
    let before = snapshot(&file);
    assert_eq!(
        migrate_to_latest(&file.0).unwrap(),
        MigrationReport {
            migrated_tables: 1,
            unchanged_tables: 1,
            rewritten_rows: 0
        }
    );
    let after = snapshot(&file);
    assert_eq!(after.version, Some(5));
    assert_eq!(after.rows, before.rows);
    assert_ne!(after.schemas["Defaults"], before.schemas["Defaults"]);
    assert_eq!(after.schemas["Modern"], before.schemas["Modern"]);
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("INSERT INTO Defaults (id) VALUES (2); INSERT INTO Modern (id) VALUES (2);")
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
    assert_eq!(
        reopened
            .execute("SELECT matches FROM Modern WHERE id = 2;")
            .unwrap(),
        vec![Payload::Select {
            labels: vec!["matches".into()],
            rows: vec![vec![Value::Bool(true)]]
        }]
    );
}

#[test]
fn a_later_malformed_schema_preserves_every_table_and_metadata() {
    let mut source = snapshot(&TestFile::fixture("v3/upgraded_v1_defaults.redb"));
    let mut broken =
        serialize(&Schema::from_ddl("CREATE TABLE ZBroken (id INT);").unwrap()).unwrap();
    broken.push(255);
    source.schemas.insert("ZBroken".into(), broken);
    source.rows.insert("ZBroken".into(), vec![]);
    let file = TestFile::empty();
    write_database(&file, true, &source);
    let before = snapshot(&file);
    // redb scans Defaults before ZBroken. The earlier valid schema must not
    // be committed when another table prevents migration of the database.
    assert_eq!(
        before
            .schemas
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["Defaults", "ZBroken"]
    );
    let error = migrate_to_latest(&file.0).unwrap_err().to_string();
    assert!(
        error.contains("failed to decode schema in table 'ZBroken'"),
        "{error}"
    );
    assert_eq!(snapshot(&file), before);
}

#[test]
fn a_later_ambiguous_schema_preserves_an_earlier_rewrite_candidate() {
    let old = snapshot(&TestFile::fixture("v3/upgraded_v1_defaults.redb"));
    let ambiguous = snapshot(&TestFile::fixture("v3/upgraded_v1_typed_default.redb"));
    let mut schema = old.schemas["Defaults"].clone();
    // Both names have eight bytes; retain the old schema layout while putting
    // the valid rewrite candidate before Ambiguous in redb's key order.
    assert_eq!(&schema[8..16], b"Defaults");
    schema[8..16].copy_from_slice(b"Aardvark");
    let file = TestFile::empty();
    write_database(
        &file,
        true,
        &Snapshot {
            version: Some(3),
            schemas: BTreeMap::from([
                ("Aardvark".into(), schema),
                ("Ambiguous".into(), ambiguous.schemas["Ambiguous"].clone()),
            ]),
            rows: BTreeMap::from([
                ("Aardvark".into(), old.rows["Defaults"].clone()),
                ("Ambiguous".into(), ambiguous.rows["Ambiguous"].clone()),
            ]),
        },
    );
    let before = snapshot(&file);
    assert_eq!(
        before
            .schemas
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["Aardvark", "Ambiguous"]
    );
    let error = migrate_to_latest(&file.0).unwrap_err().to_string();
    assert!(
        error.contains("ambiguous schema in table 'Ambiguous'"),
        "{error}"
    );
    assert_eq!(snapshot(&file), before);
}

#[test]
fn historical_index_plan_in_a_v3_database_requires_recovery() {
    let mut source = snapshot(&TestFile::fixture("v1/index_plan.redb"));
    source.version = Some(3);
    let file = TestFile::empty();
    write_database(&file, true, &source);
    let before = snapshot(&file);
    let error = migrate_to_latest(&file.0).unwrap_err().to_string();
    assert!(
        error.contains("index access plan requires explicit recovery"),
        "{error}"
    );
    assert_eq!(snapshot(&file), before);
}
