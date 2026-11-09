#[path = "migration/database.rs"]
pub mod database;

use {
    database::TestFile,
    gluesql_core::{
        data::{Schema, Value},
        prelude::{Error, Glue, Payload, execute},
        store::{Store, StoreMut, Transaction},
    },
    gluesql_redb_storage::RedbStorage,
};

#[test]
fn catalog_reads_follow_the_active_transaction() {
    let file = TestFile::empty();
    let mut storage = RedbStorage::new(&file.0).unwrap();
    let item = Schema::from_ddl("CREATE TABLE Item (id INT)").unwrap();
    let pending = Schema::from_ddl("CREATE TABLE Pending (id INT)").unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&item).unwrap();
    storage.commit().unwrap();

    assert_eq!(storage.fetch_all_schemas(), Ok(vec![item.clone()]));
    assert_eq!(storage.begin(true), Ok(true));
    storage.rollback().unwrap();

    storage.begin(false).unwrap();
    storage.insert_schema(&pending).unwrap();

    assert_eq!(storage.fetch_schema("Pending"), Ok(Some(pending.clone())));
    assert_eq!(storage.fetch_all_schemas(), Ok(vec![item.clone(), pending]));

    storage.rollback().unwrap();

    assert_eq!(storage.fetch_schema("Pending"), Ok(None));
    assert_eq!(storage.fetch_all_schemas(), Ok(vec![item]));
    assert_eq!(storage.begin(true), Ok(true));
    storage.rollback().unwrap();
}

#[test]
fn execution_transaction_ownership() {
    let file = TestFile::empty();
    let mut glue = Glue::new(RedbStorage::new(&file.0).unwrap());
    glue.execute("CREATE TABLE Item (id INT)").unwrap();
    let statement = glue.plan("INSERT INTO Item VALUES (1)").unwrap().remove(0);

    let actual = execute(&mut glue.storage, &statement);
    let expected = Err(Error::StorageMsg("transaction not found".to_owned()));
    assert_eq!(actual, expected);

    glue.storage.begin(false).unwrap();

    let actual = execute(&mut glue.storage, &statement);
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected);

    glue.storage.rollback().unwrap();

    let actual = glue.execute("SELECT * FROM Item");
    let expected = Ok(vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![],
    }]);
    assert_eq!(actual, expected);

    let actual = glue.execute_prepared(&statement);
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected);

    // Glue has already committed the row before returning.
    glue.storage.rollback().unwrap();

    let actual = glue.execute("SELECT * FROM Item");
    let expected = Ok(vec![Payload::Select {
        labels: vec!["id".to_owned()],
        rows: vec![vec![Value::I64(1)]],
    }]);
    assert_eq!(actual, expected);
}
