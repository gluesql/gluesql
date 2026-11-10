#[path = "migration/database.rs"]
pub mod database;

use {
    database::{Snapshot, TestFile, snapshot, write_database},
    gluesql_core::{
        ast::IndexOperator,
        data::{Key, Schema, Value},
        error::{Error, Result},
        prelude::{Glue, Payload},
        store::{Index, IndexError, Store, StoreMut, Transaction},
    },
    gluesql_redb_storage::{MigrationReport, RedbStorage, migrate_to_latest},
    redb::{Database, MultimapTableHandle, TableDefinition},
    std::collections::BTreeMap,
};

fn select(glue: &mut Glue<RedbStorage>, sql: &str) -> Vec<Vec<Value>> {
    let result = glue.execute(sql).expect(sql);
    let [Payload::Select { rows, .. }] = result.as_slice() else {
        panic!("expected SELECT: {result:?}");
    };
    rows.clone()
}

fn scan_index(
    storage: &RedbStorage,
    table_name: &str,
    index_name: &str,
    asc: Option<bool>,
    cmp_value: Option<(&IndexOperator, Value)>,
) -> Vec<(Key, Vec<Value>)> {
    storage
        .scan_indexed_data(table_name, index_name, asc, cmp_value)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap()
}

fn lookup_index(
    storage: &RedbStorage,
    table_name: &str,
    index_name: &str,
    value: Value,
) -> Vec<Vec<Value>> {
    scan_index(
        storage,
        table_name,
        index_name,
        None,
        Some((&IndexOperator::Eq, value)),
    )
    .into_iter()
    .map(|(_, row)| row)
    .collect()
}

#[test]
fn indexed_alter_survives_reopen_and_future_writes() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "
        CREATE TABLE Items (pad TEXT, n INT);
        INSERT INTO Items VALUES ('x', 10), ('y', 20);
        CREATE INDEX \"by/n\" ON Items (n * 2);
        ALTER TABLE Items DROP COLUMN pad;
        ALTER TABLE Items RENAME COLUMN n TO renamed;
        ALTER TABLE Items RENAME TO RenamedItems;
        ALTER TABLE RenamedItems ADD COLUMN tag TEXT DEFAULT 'new';
    ",
    )
    .unwrap();
    drop(glue);

    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(reopened.storage.fetch_schema("Items").unwrap(), None);
    assert_eq!(
        lookup_index(&reopened.storage, "RenamedItems", "by/n", Value::I64(20)),
        vec![vec![Value::I64(10), Value::Str("new".into())]]
    );
    reopened
        .execute("UPDATE RenamedItems SET renamed = 30 WHERE renamed = 10")
        .unwrap();
    assert_eq!(
        lookup_index(&reopened.storage, "RenamedItems", "by/n", Value::I64(20)),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        lookup_index(&reopened.storage, "RenamedItems", "by/n", Value::I64(60)),
        vec![vec![Value::I64(30), Value::Str("new".into())]]
    );
    reopened.execute("DROP TABLE RenamedItems").unwrap();
    drop(reopened);
    let db = Database::open(&file.0).unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .list_multimap_tables()
            .unwrap()
            .count(),
        0
    );
    drop(db);

    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    reopened
        .execute("CREATE TABLE Items (n INT); CREATE INDEX by_n ON Items (n)")
        .unwrap();
    drop(reopened);

    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(10)),
        Vec::<Vec<Value>>::new()
    );
    reopened.execute("DROP TABLE Items").unwrap();
    drop(reopened);
    let db = Database::open(&file.0).unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .list_multimap_tables()
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn index_transaction_changes_survive_reopen() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (n INT); INSERT INTO Items VALUES (10); CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();
    glue.execute("BEGIN; UPDATE Items SET n = 20; ROLLBACK")
        .unwrap();
    drop(glue);

    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(10)),
        vec![vec![Value::I64(10)]]
    );
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(20)),
        Vec::<Vec<Value>>::new()
    );
    reopened
        .execute("BEGIN; UPDATE Items SET n = 30; COMMIT")
        .unwrap();
    drop(reopened);
    let reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(10)),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(30)),
        vec![vec![Value::I64(30)]]
    );
}

#[test]
fn v4_payloads_are_preserved_before_creating_persistent_indexes() {
    let file = TestFile::empty();
    let schema = Schema::from_ddl("CREATE TABLE Items (n INT)").unwrap();
    let source = Snapshot {
        version: Some(4),
        schemas: BTreeMap::from([("Items".into(), bincode::serialize(&schema).unwrap())]),
        rows: BTreeMap::from([(
            "Items".into(),
            vec![(
                Key::I64(1).to_cmp_be_bytes().unwrap(),
                bincode::serialize(&(Key::I64(1), vec![Value::I64(10)])).unwrap(),
            )],
        )]),
    };
    write_database(&file, true, &source);
    let expected = MigrationReport {
        migrated_tables: 0,
        unchanged_tables: 1,
        rewritten_rows: 0,
    };
    assert_eq!(migrate_to_latest(&file.0).unwrap(), expected);
    assert_eq!(migrate_to_latest(&file.0).unwrap(), expected);
    let migrated = snapshot(&file);
    assert_eq!(migrated.version, Some(5));
    assert_eq!(migrated.schemas, source.schemas);
    assert_eq!(migrated.rows, source.rows);
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("CREATE INDEX by_n ON Items (n)").unwrap();
    drop(glue);

    let mut reopened = Glue::new(RedbStorage::new(&file.0).unwrap());
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(10)),
        vec![vec![Value::I64(10)]]
    );
    reopened.execute("UPDATE Items SET n = 20").unwrap();
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(10)),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        lookup_index(&reopened.storage, "Items", "by_n", Value::I64(20)),
        vec![vec![Value::I64(20)]]
    );
}

#[test]
fn repeated_row_keys_replace_every_index_entry() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (n INT);
         CREATE INDEX by_n ON Items (n);
         CREATE INDEX negative ON Items (-n)",
    )
    .unwrap();
    glue.storage.begin(false).unwrap();
    glue.storage
        .insert_data(
            "Items",
            vec![
                (Key::I64(1), vec![Value::I64(10)]),
                (Key::I64(1), vec![Value::I64(20)]),
                (Key::I64(2), vec![Value::I64(20)]),
                (Key::I64(1), vec![Value::I64(30)]),
            ],
        )
        .unwrap();
    assert_eq!(
        scan_index(&glue.storage, "Items", "by_n", None, None),
        vec![
            (Key::I64(2), vec![Value::I64(20)]),
            (Key::I64(1), vec![Value::I64(30)]),
        ]
    );
    assert_eq!(
        scan_index(
            &glue.storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(20))),
        ),
        vec![(Key::I64(2), vec![Value::I64(20)])]
    );
    assert_eq!(
        scan_index(&glue.storage, "Items", "negative", None, None),
        vec![
            (Key::I64(1), vec![Value::I64(30)]),
            (Key::I64(2), vec![Value::I64(20)]),
        ]
    );

    glue.storage
        .delete_data("Items", vec![Key::I64(2), Key::I64(2), Key::I64(99)])
        .unwrap();
    assert_eq!(
        glue.storage.fetch_data("Items", &Key::I64(2)).unwrap(),
        None
    );
    assert_eq!(
        scan_index(&glue.storage, "Items", "by_n", None, None),
        vec![(Key::I64(1), vec![Value::I64(30)])]
    );
    assert_eq!(
        scan_index(&glue.storage, "Items", "negative", None, None),
        vec![(Key::I64(1), vec![Value::I64(30)])]
    );
    assert_eq!(
        scan_index(
            &glue.storage,
            "Items",
            "negative",
            None,
            Some((&IndexOperator::Eq, Value::I64(-30))),
        ),
        vec![(Key::I64(1), vec![Value::I64(30)])]
    );
    glue.storage.rollback().unwrap();
}

#[test]
fn insert_schema_rejects_missing_index_table() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (id INT PRIMARY KEY, n INT);
         INSERT INTO Items VALUES (1, 10);
         CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();

    let mut storage = glue.storage;
    let schema = storage.fetch_schema("Items").unwrap().unwrap();

    let mut updated_schema = schema.clone();
    let mut missing = updated_schema.indexes[0].clone();
    missing.name = "missing".into();
    updated_schema.indexes.push(missing);

    storage.begin(false).unwrap();
    assert_eq!(
        storage.insert_schema(&updated_schema),
        Err(Error::StorageMsg(
            "conflict - cannot insert schema: referenced index table does not exist: __GLUESQL_INDEX__/Items/missing".into(),
        ))
    );

    assert_eq!(storage.fetch_schema("Items").unwrap(), Some(schema));
    assert_eq!(
        storage
            .scan_data("Items", None)
            .unwrap()
            .collect::<Result<Vec<_>>>(),
        Ok(vec![(Key::I64(1), vec![Value::I64(1), Value::I64(10)])])
    );
    assert_eq!(
        scan_index(&storage, "Items", "by_n", None, None),
        vec![(Key::I64(1), vec![Value::I64(1), Value::I64(10)])]
    );

    // Commit so rollback cannot hide an index table created by the rejected schema write.
    storage.commit().unwrap();
    drop(storage);

    let db = Database::open(&file.0).unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .list_multimap_tables()
            .unwrap()
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>(),
        vec!["__GLUESQL_INDEX__/Items/by_n".to_owned()]
    );
}

#[test]
fn failed_append_keeps_rows_and_indexes_unchanged() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (n INT);
         INSERT INTO Items VALUES (10);
         CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();
    let before = glue
        .storage
        .scan_data("Items", None)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    glue.storage.begin(false).unwrap();
    assert_eq!(
        glue.storage
            .append_data("Items", vec![vec![Value::I64(20)], vec![Value::F64(1.5)]]),
        Err(Error::StorageMsg(
            "key: FLOAT data type cannot be converted to Big-Endian bytes for comparison".into()
        ))
    );
    assert_eq!(
        select(&mut glue, "SELECT n FROM Items"),
        vec![vec![Value::I64(10)]]
    );
    assert_eq!(
        scan_index(&glue.storage, "Items", "by_n", None, None),
        before
    );
    assert_eq!(
        scan_index(
            &glue.storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(20))),
        ),
        vec![]
    );
    glue.storage.rollback().unwrap();
}

#[test]
fn failed_insert_keeps_the_previous_index_value() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (id INT PRIMARY KEY, n INT);
         INSERT INTO Items VALUES (1, 10);
         CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();
    glue.storage.begin(false).unwrap();
    assert_eq!(
        glue.storage.insert_data(
            "Items",
            vec![
                (Key::I64(1), vec![Value::I64(1), Value::I64(20)]),
                (Key::I64(2), vec![Value::I64(2)]),
            ],
        ),
        Err(Error::StorageMsg(
            "evaluate: identifier not found: n".into()
        ))
    );
    assert_eq!(
        select(&mut glue, "SELECT id, n FROM Items"),
        vec![vec![Value::I64(1), Value::I64(10)]]
    );
    assert_eq!(
        scan_index(
            &glue.storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(10))),
        ),
        vec![(Key::I64(1), vec![Value::I64(1), Value::I64(10)])]
    );
    assert_eq!(
        scan_index(
            &glue.storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(20))),
        ),
        vec![]
    );
    glue.storage.rollback().unwrap();
}

#[test]
fn failed_delete_keeps_rows_before_the_corrupt_row() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (id INT PRIMARY KEY, n INT);
         INSERT INTO Items VALUES (1, 10), (2, 20);
         CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();
    drop(glue);

    let db = Database::open(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("Items"))
        .unwrap()
        .insert(
            Key::I64(2).to_cmp_be_bytes().unwrap().as_slice(),
            bincode::serialize(&(Key::I64(2), vec![Value::I64(2)])).unwrap(),
        )
        .unwrap();
    txn.commit().unwrap();
    drop(db);

    let mut storage = RedbStorage::new(&file.0).unwrap();
    storage.begin(false).unwrap();
    assert_eq!(
        storage.delete_data("Items", vec![Key::I64(1), Key::I64(2)]),
        Err(Error::StorageMsg(
            "evaluate: identifier not found: n".into()
        ))
    );
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(1), Value::I64(10)])
    );
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(2)).unwrap(),
        Some(vec![Value::I64(2)])
    );
    assert_eq!(
        scan_index(
            &storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(10))),
        ),
        vec![(Key::I64(1), vec![Value::I64(1), Value::I64(10)])]
    );
    assert_eq!(
        scan_index(
            &storage,
            "Items",
            "by_n",
            None,
            Some((&IndexOperator::Eq, Value::I64(20))),
        ),
        vec![(Key::I64(2), vec![Value::I64(2)])]
    );
    storage.rollback().unwrap();
}

#[test]
fn failed_index_creation_leaves_no_metadata_or_index_table() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (n FLOAT);
         INSERT INTO Items VALUES (NULL), (1.5);
         BEGIN",
    )
    .unwrap();
    assert_eq!(
        glue.execute("CREATE INDEX by_n ON Items (n)"),
        Err(Error::StorageMsg(
            "key: FLOAT data type cannot be converted to Big-Endian bytes for comparison".into()
        ))
    );
    assert_eq!(
        glue.storage.fetch_schema("Items").unwrap().unwrap().indexes,
        vec![]
    );
    // Commit instead of rolling back so rollback cannot hide partial index writes.
    glue.execute("COMMIT").unwrap();
    drop(glue);
    let db = Database::open(&file.0).unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .list_multimap_tables()
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn index_scan_reports_a_missing_row_when_the_iterator_reaches_it() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute(
        "CREATE TABLE Items (id INT PRIMARY KEY, n INT);
         INSERT INTO Items VALUES (1, 10), (2, 20);
         CREATE INDEX by_n ON Items (n)",
    )
    .unwrap();
    drop(glue);

    let db = Database::open(&file.0).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TableDefinition::<&[u8], Vec<u8>>::new("Items"))
        .unwrap()
        .remove(Key::I64(2).to_cmp_be_bytes().unwrap().as_slice())
        .unwrap();
    txn.commit().unwrap();
    drop(db);

    let storage = RedbStorage::new(&file.0).unwrap();
    let mut rows = storage
        .scan_indexed_data("Items", "by_n", None, None)
        .unwrap();
    assert_eq!(
        rows.next(),
        Some(Ok((Key::I64(1), vec![Value::I64(1), Value::I64(10)])))
    );
    assert_eq!(
        rows.next(),
        Some(Err(Error::Index(IndexError::ConflictOnEmptyIndexValueScan)))
    );
    assert_eq!(rows.next(), None);
}
