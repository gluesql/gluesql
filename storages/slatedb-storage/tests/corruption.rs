use {
    gluesql_core::{
        data::{Key, Value},
        error::Error,
        store::{ScanColumns, Store},
    },
    gluesql_slatedb_storage::{SlatedbStorage, object_store::memory::InMemory},
    slatedb::Db,
    std::sync::Arc,
    tokio::runtime::Builder,
};

fn write_raw(prefix: &str, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Arc<InMemory> {
    let objects = Arc::new(InMemory::new());
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    runtime.block_on(async {
        let db = Db::open(prefix, objects.clone()).await.unwrap();

        for (key, value) in entries {
            let handle = db.put(key, value).await.unwrap();
            handle.await_durable().await.unwrap();
        }

        db.close().await.unwrap();
    });

    objects
}

#[test]
fn schema_decode_errors_reach_the_store_boundary() {
    let objects = write_raw("schema", vec![(b"schema/Items".to_vec(), vec![])]);
    let storage = SlatedbStorage::new(objects, "schema").unwrap();

    let expected = Error::StorageMsg("io error: unexpected end of file".into());
    assert_eq!(storage.fetch_schema("Items").unwrap_err(), expected);
    assert_eq!(storage.fetch_all_schemas(), Err(expected));
    storage.close().unwrap();
}

#[test]
fn scan_decodes_rows_lazily_and_stops_after_an_error() {
    let first_key = [
        b"data/Items/".as_slice(),
        &Key::I64(1).to_cmp_be_bytes().unwrap(),
    ]
    .concat();
    let bad_key = [
        b"data/Items/".as_slice(),
        &Key::I64(2).to_cmp_be_bytes().unwrap(),
    ]
    .concat();
    let objects = write_raw(
        "rows",
        vec![
            (
                first_key,
                bincode::serialize(&(Key::I64(1), vec![Value::I64(1)])).unwrap(),
            ),
            (bad_key, vec![]),
        ],
    );

    let storage = SlatedbStorage::new(objects, "rows").unwrap();
    let expected = Error::StorageMsg("io error: unexpected end of file".into());
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(2)).unwrap_err(),
        expected
    );

    let mut rows = storage.scan_data("Items", ScanColumns::All).unwrap();
    assert_eq!(rows.next(), Some(Ok((Key::I64(1), vec![Value::I64(1)]))));
    assert_eq!(rows.next(), Some(Err(expected)));
    assert_eq!(rows.next(), None);
    assert_eq!(rows.next(), None);

    drop(rows);
    storage.close().unwrap();
}
