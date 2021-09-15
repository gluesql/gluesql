#![cfg(feature = "sled-storage")]

//! # SledStorage transaction tests
//!
//! REPEATABLE READ or SNAPSHOT ISOLATION is a transaction level which SledStorage provides.
//! Therefore, SledStorage is safe from READ UNCOMMITTED or READ COMMITTED concurrency conflict
//! scenarios, but not PHANTOM READ safe.

use {
    gluesql::{tests::test_indexes, Value::*, *},
    std::{
        fs,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

const PATH_PREFIX: &str = "tmp/gluesql";

macro_rules! exec {
    ($glue: ident $sql: literal) => {
        $glue.execute($sql).unwrap();
    };
}

macro_rules! test {
    ($glue: ident $sql: literal, $result: expr) => {
        assert_eq!($glue.execute($sql), $result);
    };
}

macro_rules! test_idx {
    ($glue: ident $sql: literal, $idx: expr, $result: expr) => {
        let statement = $glue.plan($sql).unwrap();

        test_indexes(&statement, Some($idx));
        assert_eq!($glue.execute_stmt(statement), $result);
    };
}

#[test]
fn sled_transaction_basic() {
    let path = &format!("{}/basic", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage = SledStorage::new(path).unwrap();
    let storage2 = storage.clone();
    let mut glue = Glue::new(storage);
    let mut glue2 = Glue::new(storage2);

    exec!(glue "BEGIN");
    test!(glue "BEGIN", Err(Error::StorageMsg("nested transaction is not supported".to_owned())));
    exec!(glue "COMMIT;");

    test!(glue "ROLLBACK", Err(Error::StorageMsg("no transaction to rollback".to_owned())));
    test!(glue "COMMIT", Err(Error::StorageMsg("no transaction to commit".to_owned())));

    exec!(glue "BEGIN;");
    exec!(glue "CREATE TABLE AcquireLock (id INTEGER);");
    test!(
        glue2 "CREATE TABLE MeTooTheLock (id INTEGER);",
        Err(Error::StorageMsg("database is locked".to_owned()))
    );

    exec!(glue2 "BEGIN;");
    exec!(glue2 "COMMIT;");
    test!(
        glue2 "SELECT * FROM AcquireLock;",
        Err(FetchError::TableNotFound("AcquireLock".to_owned()).into())
    );
}

#[test]
fn sled_transaction_read_uncommitted() {
    let path = &format!("{}/read_uncommitted", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    exec!(glue1 "BEGIN;");
    exec!(glue1 "CREATE TABLE Sample (id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (30);");

    test!(
        glue2 "SELECT * FROM Sample",
        Err(FetchError::TableNotFound("Sample".to_owned()).into())
    );
    exec!(glue2 "BEGIN;");
    test!(
        glue2 "SELECT * FROM Sample",
        Err(FetchError::TableNotFound("Sample".to_owned()).into())
    );
    exec!(glue2 "COMMIT;");
    exec!(glue1 "COMMIT;");
}

#[test]
fn sled_transaction_read_committed() {
    let path = &format!("{}/read_committed", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    exec!(glue2 "BEGIN;");

    exec!(glue1 "BEGIN;");
    exec!(glue1 "CREATE TABLE Sample (id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (30);");
    exec!(glue1 "COMMIT;");

    test!(
        glue2 "SELECT * FROM Sample",
        Err(FetchError::TableNotFound("Sample".to_owned()).into())
    );
    exec!(glue2 "COMMIT;");

    test!(
        glue2 "SELECT * FROM Sample",
        Ok(select!(id I64; 30))
    );
}

#[test]
fn sled_transaction_schema_mut() {
    let path = &format!("{}/transaction_schema_mut", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    exec!(glue1 "CREATE TABLE Sample (id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (1);");

    exec!(glue2 "BEGIN;");
    exec!(glue1 "BEGIN;");
    exec!(glue1 "DROP TABLE Sample;");
    test!(
        glue1 "SELECT * FROM Sample;",
        Err(FetchError::TableNotFound("Sample".to_owned()).into())
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "COMMIT;");
    exec!(glue1 "CREATE TABLE Sample (new_id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (5);");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(select!(new_id I64; 5))
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );
    exec!(glue2 "COMMIT;");
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(new_id I64; 5))
    );
}

#[test]
fn sled_transaction_data_mut() {
    let path = &format!("{}/transaction_data_mut", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    exec!(glue1 "CREATE TABLE Sample (id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (1);");

    exec!(glue2 "BEGIN;");
    exec!(glue1 "BEGIN;");

    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "DELETE FROM Sample;");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        })
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "INSERT INTO Sample VALUES (3), (5);");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(select!(id I64; 3; 5))
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "UPDATE Sample SET id = id + 1;");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(select!(id I64; 4; 6))
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "COMMIT;");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(select!(id I64; 4; 6))
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 1))
    );

    exec!(glue2 "COMMIT;");
    test!(
        glue1 "SELECT * FROM Sample;",
        Ok(select!(id I64; 4; 6))
    );
    test!(
        glue2 "SELECT * FROM Sample;",
        Ok(select!(id I64; 4; 6))
    );
}

#[test]
fn sled_transaction_index_mut() {
    use ast::IndexOperator::Eq;

    let path = &format!("{}/transaction_index_mut", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    exec!(glue1 "CREATE TABLE Sample (id INTEGER);");
    exec!(glue1 "INSERT INTO Sample VALUES (1);");

    exec!(glue2 "BEGIN;");
    exec!(glue1 "BEGIN;");

    exec!(glue1 "CREATE INDEX idx_id ON Sample (id);");

    test_idx!(
        glue1 "SELECT * FROM Sample WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue2 "SELECT * FROM Sample WHERE id = 1;",
        idx!(),
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "COMMIT;");
    test_idx!(
        glue2 "SELECT * FROM Sample WHERE id = 1;",
        idx!(),
        Ok(select!(id I64; 1))
    );

    exec!(glue2 "COMMIT;");
    test_idx!(
        glue1 "SELECT * FROM Sample WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );

    exec!(glue2 "BEGIN;");
    exec!(glue1 "BEGIN;");

    exec!(glue1 "DROP INDEX Sample.idx_id;");

    test_idx!(
        glue2 "SELECT * FROM Sample WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue1 "SELECT * FROM Sample WHERE id = 1;",
        idx!(),
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "COMMIT;");
    test_idx!(
        glue2 "SELECT * FROM Sample WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );

    exec!(glue2 "COMMIT;");
    test_idx!(
        glue1 "SELECT * FROM Sample WHERE id = 1;",
        idx!(),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue2 "SELECT * FROM Sample WHERE id = 1;",
        idx!(),
        Ok(select!(id I64; 1))
    );
}

#[tokio::test]
async fn sled_transaction_gc() {
    let path = &format!("{}/transaction_gc", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let storage1 = SledStorage::new(path).unwrap();
    let storage2 = storage1.clone();
    let tree = storage1.clone().tree;

    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    macro_rules! assert_some {
        () => {
            assert!(tree.scan_prefix("temp_").next().is_some());
        };
    }
    macro_rules! assert_none {
        () => {
            assert!(tree.scan_prefix("temp_").next().is_none());
        };
    }

    // COMMIT runs GC and all temp_ data must be removed.
    exec!(glue1 "BEGIN;");
    exec!(glue1 "CREATE TABLE Garlic (id INTEGER);");
    assert_some!();
    exec!(glue1 "CREATE INDEX idx_id ON Garlic (id);");
    exec!(glue1 "INSERT INTO Garlic VALUES (1), (2);");
    exec!(glue1 "CREATE INDEX idx_gc ON Garlic (id + 2);");
    #[cfg(feature = "alter-table")]
    exec!(glue1 "ALTER TABLE Garlic ADD COLUMN num INTEGER NULL;");
    assert_some!();
    exec!(glue1 "COMMIT;");
    assert_none!();

    // Though glue1 COMMIT, glue2 transaction is still alive.
    // Until glue2 COMMIT, temp_ must survive.
    exec!(glue2 "BEGIN;");
    exec!(glue1 "BEGIN;");
    exec!(glue1 "CREATE TABLE NewGarlic (gar BOOLEAN);");
    exec!(glue1 "INSERT INTO NewGarlic VALUES (True);");
    assert_some!();
    exec!(glue1 "COMMIT;");
    assert_some!();
    exec!(glue2 "COMMIT;");
    assert_none!();

    // force change, txid -> 0
    exec!(glue1 "BEGIN;");

    let mut storage = glue1.storage.unwrap();
    storage.state = sled_storage::State::Transaction {
        txid: 0,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        autocommit: false,
    };
    let mut glue1 = Glue::new(storage);

    test!(glue1 "SELECT * FROM NewGarlic", Err(Error::StorageMsg("fetch failed - expired transaction has used (txid)".to_owned())));
    assert_eq!(
        glue1
            .storage
            .unwrap()
            .update_data("NewGarlic", vec![])
            .await
            .map(|(_, v)| v)
            .map_err(|(_, e)| e),
        Err(Error::StorageMsg(
            "acquire failed - expired transaction has used (txid)".to_owned()
        )),
    );
}

const TX_TIMEOUT: Option<u128> = Some(200);
const TX_SLEEP_TICK: Duration = Duration::from_millis(201);

#[tokio::test]
async fn sled_transaction_timeout_store() {
    let path = &format!("{}/transaction_timeout_store", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let mut storage1 = SledStorage::new(path).unwrap();
    storage1.set_transaction_timeout(TX_TIMEOUT);
    let storage2 = storage1.clone();

    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    let sleep = || {
        std::thread::sleep(TX_SLEEP_TICK);
    };

    exec!(glue1 "BEGIN;");

    // glue1 acquires lock
    exec!(glue1 "CREATE TABLE TxGarlic (id INTEGER);");

    // glue1 lock gets expired due to the timeout
    sleep();

    // timeout errors
    test!(glue1 "COMMIT;", Err(Error::StorageMsg("fetch failed - expired transaction has used (timeout)".to_owned())));
    test!(glue1 "SELECT * FROM TxGarlic", Err(Error::StorageMsg("fetch failed - expired transaction has used (timeout)".to_owned())));
    assert_eq!(
        glue1
            .storage
            .clone()
            .unwrap()
            .update_data("TxGarlic", vec![])
            .await
            .map(|(_, v)| v)
            .map_err(|(_, e)| e),
        Err(Error::StorageMsg(
            "acquire failed - expired transaction has used (timeout)".to_owned()
        )),
    );

    exec!(glue2 "BEGIN;");
    exec!(glue2 "CREATE TABLE RealGarlic (id INTEGER);");
    exec!(glue1 "ROLLBACK;");
    exec!(glue2 "ROLLBACK;");

    // glue2 lock gets expired
    sleep();

    // glue1 must succeed to create tables: TxGarlic & RealGarlic
    exec!(glue1 "CREATE TABLE TxGarlic (id2 INTEGER);");
    exec!(glue1 "CREATE TABLE RealGarlic (id2 INTEGER);");

    exec!(glue1 "BEGIN;");
    exec!(glue1 "INSERT INTO TxGarlic VALUES (10);");
    sleep();
    exec!(glue2 "INSERT INTO TxGarlic VALUES (20);");
    test!(glue1 "COMMIT;", Err(Error::StorageMsg("fetch failed - expired transaction has used (timeout)".to_owned())));
    exec!(glue1 "ROLLBACK;");

    // glue1 lock has expired, so TxGarlic table has only a single row (20)
    test!(
        glue1 "SELECT * FROM TxGarlic;",
        Ok(select!(id2 I64; 20))
    );

    exec!(glue1 "BEGIN;");
    exec!(glue1 "UPDATE TxGarlic SET id2 = id2 * 2;");
    test!(
        glue1 "SELECT * FROM TxGarlic;",
        Ok(select!(id2 I64; 40))
    );

    // glue1 lock gets expired
    sleep();

    // glue1 tx must rollback
    test!(
        glue2 "SELECT * FROM TxGarlic;",
        Ok(select!(id2 I64; 20))
    );

    exec!(glue1 "ROLLBACK;");
    test!(
        glue1 "SELECT * FROM TxGarlic;",
        Ok(select!(id2 I64; 20))
    );
    test!(
        glue2 "SELECT * FROM TxGarlic;",
        Ok(select!(id2 I64; 20))
    );

    // UPDATE
    exec!(glue1 "BEGIN;");
    exec!(glue1 "UPDATE TxGarlic SET id2 = id2 + 1;");
    sleep();
    exec!(glue2 "BEGIN;");
    exec!(glue2 "UPDATE TxGarlic SET id2 = id2 + 1;");
    exec!(glue2 "ROLLBACK;");
    exec!(glue1 "ROLLBACK;");

    // DELETE
    exec!(glue1 "BEGIN;");
    exec!(glue1 "DELETE FROM TxGarlic;");
    test!(glue1 "SELECT * FROM TxGarlic;", Ok(select!(id2)));
    sleep();
    test!(glue2 "SELECT * FROM TxGarlic;", Ok(select!(id2 I64; 20)));
    exec!(glue2 "BEGIN;");
    exec!(glue2 "DELETE FROM TxGarlic");
    exec!(glue1 "ROLLBACK;");
    exec!(glue2 "ROLLBACK;");
    test!(glue1 "SELECT * FROM TxGarlic;", Ok(select!(id2 I64; 20)));
    test!(glue2 "SELECT * FROM TxGarlic;", Ok(select!(id2 I64; 20)));

    // DROP TABLE
    exec!(glue2 "BEGIN;");
    exec!(glue2 "DROP TABLE TxGarlic;");
    sleep();
    test!(glue2 "COMMIT;", Err(Error::StorageMsg("fetch failed - expired transaction has used (timeout)".to_owned())));
    exec!(glue1 "DROP TABLE TxGarlic;");
    test!(
        glue1 "SELECT * FROM TxGarlic;",
        Err(FetchError::TableNotFound("TxGarlic".to_owned()).into())
    );
    exec!(glue2 "ROLLBACK;");
    test!(
        glue2 "SELECT * FROM TxGarlic;",
        Err(FetchError::TableNotFound("TxGarlic".to_owned()).into())
    );
}

#[cfg(feature = "alter-table")]
#[tokio::test]
async fn sled_transaction_timeout_alter() {
    let path = &format!("{}/transaction_timeout_alter", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let mut storage1 = SledStorage::new(path).unwrap();
    storage1.set_transaction_timeout(TX_TIMEOUT);
    let storage2 = storage1.clone();

    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    let sleep = || {
        std::thread::sleep(TX_SLEEP_TICK);
    };

    exec!(glue1 "CREATE TABLE TxAlter (id INTEGER, num INTEGER);");
    exec!(glue1 "INSERT INTO TxAlter VALUES (1, 100);");

    // DROP COLUMN
    exec!(glue1 "BEGIN;");
    exec!(glue1 "ALTER TABLE TxAlter DROP COLUMN num;");
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(id I64; 1)));
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));
    sleep();
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));
    exec!(glue2 "BEGIN;");
    exec!(glue2 "ALTER TABLE TxAlter DROP COLUMN num;");
    exec!(glue2 "ROLLBACK;");
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));

    // ADD COLUMN
    exec!(glue2 "BEGIN;");
    exec!(glue2 "ALTER TABLE TxAlter ADD COLUMN flag BOOLEAN DEFAULT TRUE;");
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num | flag I64 | I64 | Bool; 1 100 true)));

    exec!(glue1 "ROLLBACK;");
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));
    sleep();
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));
    exec!(glue2 "ROLLBACK;");
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));

    // RENAME COLUMN
    exec!(glue1 "BEGIN;");
    exec!(glue1 "ALTER TABLE TxAlter RENAME COLUMN id TO jd;");
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(jd | num I64 | I64; 1 100)));
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(id | num I64 | I64; 1 100)));
    sleep();
    exec!(glue2 "BEGIN;");
    exec!(glue2 "ALTER TABLE TxAlter RENAME COLUMN id TO kd;");
    exec!(glue2 "COMMIT;");
    exec!(glue1 "ROLLBACK;");
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(kd | num I64 | I64; 1 100)));
    test!(glue2 "SELECT * FROM TxAlter;", Ok(select!(kd | num I64 | I64; 1 100)));

    // RENAME TABLE
    exec!(glue2 "BEGIN;");
    exec!(glue2 "ALTER TABLE TxAlter RENAME TO TxAltericano;");
    test!(glue2 "SELECT * FROM TxAltericano;", Ok(select!(kd | num I64 | I64; 1 100)));
    test!(
        glue2 "SELECT * FROM TxAlter;",
        Err(FetchError::TableNotFound("TxAlter".to_owned()).into())
    );
    test!(glue1 "SELECT * FROM TxAlter;", Ok(select!(kd | num I64 | I64; 1 100)));
    test!(
        glue1 "SELECT * FROM TxAlterericano;",
        Err(FetchError::TableNotFound("TxAlterericano".to_owned()).into())
    );
    sleep();
    exec!(glue1 "ALTER TABLE TxAlter RENAME TO TxSoprano;");
    test!(glue1 "SELECT * FROM TxSoprano;", Ok(select!(kd | num I64 | I64; 1 100)));
    exec!(glue2 "ROLLBACK;");
    test!(glue2 "SELECT * FROM TxSoprano;", Ok(select!(kd | num I64 | I64; 1 100)));
}

#[tokio::test]
async fn sled_transaction_timeout_index() {
    use ast::IndexOperator::Eq;

    let path = &format!("{}/transaction_timeout_index", PATH_PREFIX);
    fs::remove_dir_all(path).unwrap_or(());

    let mut storage1 = SledStorage::new(path).unwrap();
    storage1.set_transaction_timeout(TX_TIMEOUT);
    let storage2 = storage1.clone();

    let mut glue1 = Glue::new(storage1);
    let mut glue2 = Glue::new(storage2);

    let sleep = || {
        std::thread::sleep(TX_SLEEP_TICK);
    };

    exec!(glue1 "CREATE TABLE TxIndex (id INTEGER);");
    exec!(glue1 "INSERT INTO TxIndex VALUES (1);");

    // CREATE INDEX
    exec!(glue1 "BEGIN;");
    exec!(glue1 "CREATE INDEX idx_id ON TxIndex (id);");
    test_idx!(
        glue1 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    sleep();
    exec!(glue2 "CREATE INDEX idx_id ON TxIndex (id);");
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    exec!(glue1 "ROLLBACK;");
    test_idx!(
        glue1 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );

    // DROP INDEX
    exec!(glue1 "BEGIN;");
    exec!(glue1 "DROP INDEX TxIndex.idx_id;");
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    sleep();
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    exec!(glue1 "ROLLBACK;");
    test_idx!(
        glue1 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );

    // DROP AND DROP INDEX
    exec!(glue1 "BEGIN;");
    exec!(glue1 "DROP INDEX TxIndex.idx_id;");
    exec!(glue1 "CREATE INDEX idx_id ON TxIndex (id);");
    sleep();
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    exec!(glue1 "ROLLBACK;");
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue1 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );

    exec!(glue1 "BEGIN;");
    exec!(glue1 "DROP INDEX TxIndex.idx_id;");
    sleep();
    exec!(glue2 "DROP INDEX TxIndex.idx_id;");
    sleep();
    exec!(glue1 "ROLLBACK;");
    exec!(glue1 "CREATE INDEX idx_id ON TxIndex (id);");
    test_idx!(
        glue2 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
    test_idx!(
        glue1 "SELECT * FROM TxIndex WHERE id = 1;",
        idx!(idx_id, Eq, "1"),
        Ok(select!(id I64; 1))
    );
}
