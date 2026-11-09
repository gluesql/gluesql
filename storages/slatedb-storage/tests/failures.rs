use {
    async_trait::async_trait,
    futures::{
        executor::block_on,
        stream::{self, BoxStream},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Error,
        store::{Store, StoreMut, Transaction},
    },
    gluesql_slatedb_storage::{
        SlatedbStorage,
        object_store::{
            self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
            ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
            path::Path,
        },
    },
    std::{
        fmt, io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

#[derive(Debug, Default)]
struct FailingStore {
    inner: InMemory,
    fail_list: AtomicBool,
    fail_wal: AtomicBool,
    fail_sst_writes: AtomicBool,
    fail_sst_reads: AtomicBool,
}

impl fmt::Display for FailingStore {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!("not used by these failure tests")
    }
}

#[async_trait]
impl ObjectStore for FailingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_wal.load(Ordering::SeqCst) && location.as_ref().contains("/wal/") {
            // A permanent error prevents the engine's normal transient retries
            // from hiding the adapter's failure handling or hanging this test.
            return Err(object_store::Error::NotSupported {
                source: Box::new(io::Error::other("injected WAL failure")),
            });
        }

        if self.fail_sst_writes.load(Ordering::SeqCst) && location.as_ref().contains("/compacted/")
        {
            return Err(object_store::Error::NotSupported {
                source: Box::new(io::Error::other("injected SST write failure")),
            });
        }

        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!("not used by these failure tests")
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Restrict faults to SST reads so background metadata refresh cannot
        // close the database while testing schema and row reads.
        if self.fail_sst_reads.load(Ordering::SeqCst) && location.as_ref().contains("/compacted/") {
            return Err(object_store::Error::NotSupported {
                source: Box::new(io::Error::other("injected read failure")),
            });
        }

        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        unimplemented!("not used by these failure tests")
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.fail_list.load(Ordering::SeqCst) {
            return Box::pin(stream::once(async {
                Err(object_store::Error::NotSupported {
                    source: Box::new(io::Error::other("injected list failure")),
                })
            }));
        }

        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        unimplemented!("not used by these failure tests")
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        unimplemented!("not used by these failure tests")
    }
}

// These required trait methods are unused by the storage failure scenarios.
#[test]
#[should_panic(expected = "not used by these failure tests")]
fn failing_store_display_is_unimplemented() {
    let _ = FailingStore::default().to_string();
}

#[test]
#[should_panic(expected = "not used by these failure tests")]
fn failing_store_put_multipart_opts_is_unimplemented() {
    let objects = FailingStore::default();
    let path = Path::from("unused");

    let _ = block_on(objects.put_multipart_opts(&path, PutMultipartOptions::default()));
}

#[test]
#[should_panic(expected = "not used by these failure tests")]
fn failing_store_delete_stream_is_unimplemented() {
    let objects = FailingStore::default();

    let _ = objects.delete_stream(Box::pin(stream::empty()));
}

#[test]
#[should_panic(expected = "not used by these failure tests")]
fn failing_store_list_with_delimiter_is_unimplemented() {
    let objects = FailingStore::default();

    let _ = block_on(objects.list_with_delimiter(None));
}

#[test]
#[should_panic(expected = "not used by these failure tests")]
fn failing_store_copy_opts_is_unimplemented() {
    let objects = FailingStore::default();
    let from = Path::from("source");
    let to = Path::from("destination");

    let _ = block_on(objects.copy_opts(&from, &to, CopyOptions::default()));
}

#[test]
fn object_list_failure_prevents_open() {
    let objects = Arc::new(FailingStore::default());
    objects.fail_list.store(true, Ordering::SeqCst);

    assert_eq!(
        SlatedbStorage::new(objects, "open_failure").err(),
        Some(Error::StorageMsg(
            "Unavailable error: object store error (Operation not supported: injected list failure)"
                .into()
        ))
    );
}

#[test]
fn close_flush_failure_preserves_committed_data_in_the_wal() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "close_failure").unwrap();

    storage.begin(false).unwrap();
    storage
        .insert_data("Items", vec![(Key::I64(1), vec![Value::I64(1)])])
        .unwrap();
    storage.commit().unwrap();

    objects.fail_sst_writes.store(true, Ordering::SeqCst);
    // SlateDB marks the DB closed before flushing, so this failure is reported
    // as Closed rather than the underlying object-store error.
    assert_eq!(
        storage.close(),
        Err(Error::StorageMsg("Closed error: db is closed".into()))
    );

    objects.fail_sst_writes.store(false, Ordering::SeqCst);
    let storage = SlatedbStorage::new(objects, "close_failure").unwrap();
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(1)])
    );
    storage.close().unwrap();
}

#[test]
fn failed_wal_write_reports_unconfirmed_durability() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "wal_failure").unwrap();

    storage.begin(false).unwrap();
    storage
        .insert_schema(&Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap())
        .unwrap();
    storage
        .insert_data("Items", vec![(Key::I64(1), vec![Value::I64(1)])])
        .unwrap();

    objects.fail_wal.store(true, Ordering::SeqCst);
    let error = storage.commit().unwrap_err();
    assert_eq!(
        error,
        Error::StorageMsg(
            "SlateDB commit durability could not be confirmed: \
             Unavailable error: object store error (Operation not supported: injected WAL failure)"
                .into()
        )
    );

    // The consumed transaction cannot be committed or retried a second time.
    storage.commit().unwrap();
    storage.rollback().unwrap();
    assert_eq!(
        storage.begin(false),
        Err(Error::StorageMsg(
            "Unavailable error: object store error (Operation not supported: injected WAL failure)"
                .into()
        ))
    );
    drop(storage);

    objects.fail_wal.store(false, Ordering::SeqCst);
    let storage = SlatedbStorage::new(objects, "wal_failure").unwrap();
    assert_eq!(storage.fetch_schema("Items").unwrap(), None);
    assert_eq!(storage.fetch_data("Items", &Key::I64(1)).unwrap(), None);
    storage.close().unwrap();
}

#[test]
fn object_read_failure_reaches_fetch_and_scan_open() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "read_failure").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data("Items", vec![(Key::I64(1), vec![Value::I64(1)])])
        .unwrap();
    storage.commit().unwrap();
    storage.close().unwrap();

    let mut storage = SlatedbStorage::new(objects.clone(), "read_failure").unwrap();
    objects.fail_sst_reads.store(true, Ordering::SeqCst);
    let expected = Error::StorageMsg(
        "Unavailable error: object store error (Operation not supported: injected read failure)"
            .into(),
    );
    assert_eq!(storage.fetch_all_schemas().unwrap_err(), expected);
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(1)).unwrap_err(),
        expected
    );
    assert_eq!(storage.scan_data("Items").err().unwrap(), expected);

    storage.begin(false).unwrap();
    assert_eq!(storage.delete_schema("Items"), Err(expected));
    storage.rollback().unwrap();

    objects.fail_sst_reads.store(false, Ordering::SeqCst);
    assert_eq!(storage.fetch_schema("Items").unwrap(), Some(schema));
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(1)).unwrap(),
        Some(vec![Value::I64(1)])
    );
    storage.close().unwrap();
}

#[test]
fn fetch_all_schemas_propagates_iteration_failure() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "schema_iteration_failure").unwrap();

    // Spread schemas across blocks while leaving several in the first block
    // for the iterator's initial lookahead.
    let schemas: Vec<_> = (0..32)
        .map(|id| {
            let mut schema = Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap();
            schema.table_name = format!("Items{id:03}");
            schema.comment = Some("x".repeat(512));
            schema
        })
        .collect();

    storage.begin(false).unwrap();
    for schema in &schemas {
        storage.insert_schema(schema).unwrap();
    }
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects.clone(), "schema_iteration_failure").unwrap();

    // Cache the first block; later blocks still need object-store reads.
    assert_eq!(
        storage.fetch_schema("Items000").unwrap(),
        Some(schemas[0].clone())
    );

    objects.fail_sst_reads.store(true, Ordering::SeqCst);
    assert_eq!(
        storage.fetch_all_schemas(),
        Err(Error::StorageMsg(
            "Unavailable error: object store error (Operation not supported: injected read failure)"
                .into()
        ))
    );

    objects.fail_sst_reads.store(false, Ordering::SeqCst);
    assert_eq!(storage.fetch_all_schemas().unwrap(), schemas);
    storage.close().unwrap();
}

#[test]
fn failed_schema_deletion_can_be_rolled_back() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "delete_iteration_failure").unwrap();
    let schema = Schema::from_ddl("CREATE TABLE Items (value TEXT)").unwrap();

    // Keep several rows in the first block so deletion begins before a later
    // block read fails.
    let rows: Vec<_> = (0..32)
        .map(|id| (Key::I64(id), vec![Value::Str("x".repeat(512))]))
        .collect();

    storage.begin(false).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage.insert_data("Items", rows.clone()).unwrap();
    storage.commit().unwrap();
    storage.close().unwrap();

    let mut storage = SlatedbStorage::new(objects.clone(), "delete_iteration_failure").unwrap();

    // Cache the first block so deletion starts before a later read fails.
    assert_eq!(
        storage.fetch_data("Items", &Key::I64(0)).unwrap(),
        Some(vec![Value::Str("x".repeat(512))])
    );

    storage.begin(false).unwrap();
    objects.fail_sst_reads.store(true, Ordering::SeqCst);
    assert_eq!(
        storage.delete_schema("Items"),
        Err(Error::StorageMsg(
            "Unavailable error: object store error (Operation not supported: injected read failure)"
                .into()
        ))
    );

    objects.fail_sst_reads.store(false, Ordering::SeqCst);
    assert_eq!(storage.fetch_data("Items", &Key::I64(0)).unwrap(), None);
    storage.rollback().unwrap();

    assert_eq!(storage.fetch_schema("Items").unwrap(), Some(schema));
    let restored_rows = storage
        .scan_data("Items")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(restored_rows, rows);
    storage.close().unwrap();
}

#[test]
fn object_read_failure_during_iteration_is_returned_once() {
    let objects = Arc::new(FailingStore::default());
    let mut storage = SlatedbStorage::new(objects.clone(), "lazy_read_failure").unwrap();

    // Leave unread SST blocks after consuming the first row.
    let payload = "x".repeat(2048);
    let row_count = 128;

    storage.begin(false).unwrap();
    storage
        .insert_data(
            "Items",
            (0..row_count)
                .map(|id| (Key::I64(id), vec![Value::Str(payload.clone())]))
                .collect(),
        )
        .unwrap();
    storage.commit().unwrap();
    storage.close().unwrap();

    let storage = SlatedbStorage::new(objects.clone(), "lazy_read_failure").unwrap();
    let mut rows = storage.scan_data("Items").unwrap();
    assert_eq!(
        rows.next().unwrap().unwrap(),
        (Key::I64(0), vec![Value::Str(payload)])
    );

    objects.fail_sst_reads.store(true, Ordering::SeqCst);
    let error = rows
        .find_map(Result::err)
        .expect("reading an uncached SST block must fail");

    assert_eq!(
        error,
        Error::StorageMsg(
            "Unavailable error: object store error (Operation not supported: injected read failure)"
                .into()
        )
    );
    assert_eq!(rows.next(), None);

    drop(rows);
    objects.fail_sst_reads.store(false, Ordering::SeqCst);
    storage.close().unwrap();
}

#[test]
fn fenced_writer_commit_failure_consumes_the_transaction() {
    let objects = Arc::new(InMemory::new());
    let mut older = SlatedbStorage::new(objects.clone(), "fenced").unwrap();

    older.begin(false).unwrap();
    older
        .insert_schema(&Schema::from_ddl("CREATE TABLE Items (id INTEGER)").unwrap())
        .unwrap();
    let newer = SlatedbStorage::new(objects, "fenced").unwrap();

    // SlateDB detects the new writer during its background manifest refresh.
    // Read an unbuffered key until that error reaches the public storage API.
    let deadline = Instant::now() + Duration::from_secs(10);
    let error = loop {
        match older.fetch_schema("Probe") {
            Err(error) => break error,
            Ok(schema) => assert_eq!(schema, None),
        }

        assert!(Instant::now() < deadline, "older writer was not fenced");
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        error,
        Error::StorageMsg("Closed error: detected newer DB client".into())
    );

    assert_eq!(
        older.commit(),
        Err(Error::StorageMsg(
            "SlateDB commit failed: Closed error: detected newer DB client".into()
        ))
    );

    older.commit().unwrap();
    older.rollback().unwrap();
    assert_eq!(
        older.begin(false),
        Err(Error::StorageMsg(
            "Closed error: detected newer DB client".into()
        ))
    );
    assert_eq!(newer.fetch_schema("Items").unwrap(), None);

    drop(older);
    newer.close().unwrap();
}
