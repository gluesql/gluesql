use {
    gluesql_core::store::Transaction,
    gluesql_redb_storage::RedbStorage,
    redb::{Database, StorageBackend, TableDefinition, backends::FileBackend},
    std::{
        fs::{OpenOptions, create_dir, remove_file},
        io::ErrorKind,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
};

#[derive(Debug)]
struct FailingBackend {
    inner: FileBackend,
    fail_flag: Arc<AtomicBool>,
}

impl FailingBackend {
    fn new(inner: FileBackend, fail_flag: Arc<AtomicBool>) -> Self {
        Self { inner, fail_flag }
    }
}

impl StorageBackend for FailingBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        self.inner.read(offset, len)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self, eventual: bool) -> Result<(), std::io::Error> {
        if self.fail_flag.load(Ordering::SeqCst) {
            Err(std::io::Error::from(ErrorKind::Other))
        } else {
            self.inner.sync_data(eventual)
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }
}

#[tokio::test]
async fn begin_write_after_io_error() {
    let _ = create_dir("tmp");
    let path = "tmp/redb_prev_io";
    let _ = remove_file(path);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("create file");
    let fail_flag = Arc::new(AtomicBool::new(false));
    let backend = FailingBackend::new(FileBackend::new(file).unwrap(), fail_flag.clone());
    let db = Database::builder()
        .create_with_backend(backend)
        .expect("create database");

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fail_flag.store(true, Ordering::SeqCst);
    let tx = db.begin_write().expect("begin write");
    {
        let mut table = tx.open_table(TABLE).expect("open table");
        table.insert(&0, &0).expect("insert");
    }
    let _ = tx.commit().expect_err("commit should fail");

    let mut storage = RedbStorage::from_database(db);
    let err = storage.begin(true).await.expect_err("begin should fail");
    assert!(err.to_string().contains("Previous I/O error"));
}
