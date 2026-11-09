#[path = "migration/database.rs"]
pub mod database;

use {
    database::TestFile,
    gluesql_core::{
        data::Schema,
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
