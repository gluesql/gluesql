use {gluesql_core::prelude::*, gluesql_sled_storage::SledStorage, sled::Config};

#[tokio::test]
async fn export_and_import() {
    let path1 = "tmp/export_and_import1";
    let path2 = "tmp/export_and_import2";
    let config1 = Config::default().path(path1).temporary(true);
    let config2 = Config::default().path(path2).temporary(true);

    let storage1 = SledStorage::try_from(config1).unwrap();
    let mut glue1 = Glue::new(storage1);

    glue1
        .execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .unwrap();
    glue1
        .execute("INSERT INTO Foo VALUES (1), (2), (3);")
        .await
        .unwrap();

    let data1 = glue1.execute("SELECT * FROM Foo;").await.unwrap();
    let export = glue1.storage.export().unwrap();

    let mut storage2 = SledStorage::try_from(config2).unwrap();
    storage2.import(export).unwrap();
    let mut glue2 = Glue::new(storage2);

    let data2 = glue2.execute("SELECT * FROM Foo;").await.unwrap();

    assert_eq!(data1, data2);
}

#[tokio::test]
async fn export_and_import_multiple_times() {
    let path1 = "tmp/repeated_export_and_import1";
    let path2 = "tmp/repeated_export_and_import2";
    let path3 = "tmp/repeated_export_and_import3";
    let config1 = Config::default().path(path1).temporary(true);
    let config2 = Config::default().path(path2).temporary(true);
    let config3 = Config::default().path(path3).temporary(true);

    let storage1 = SledStorage::try_from(config1).unwrap();
    let mut glue1 = Glue::new(storage1);

    glue1
        .execute("CREATE TABLE Foo (id INTEGER);")
        .await
        .unwrap();
    glue1
        .execute("INSERT INTO Foo VALUES (1), (2), (3);")
        .await
        .unwrap();

    let data1 = glue1.execute("SELECT * FROM Foo;").await.unwrap();
    let export = glue1.storage.export().unwrap();

    let mut storage2 = SledStorage::try_from(config2).unwrap();
    storage2.import(export).unwrap();
    let mut glue2 = Glue::new(storage2);

    let data2 = glue2.execute("SELECT * FROM Foo;").await.unwrap();
    let export2 = glue2.storage.export().unwrap();
    assert_eq!(data1, data2);

    let mut storage3 = SledStorage::try_from(config3).unwrap();
    storage3.import(export2).unwrap();
    let mut glue3 = Glue::new(storage3);

    let data3 = glue3.execute("SELECT * FROM Foo;").await.unwrap();
    assert_eq!(data1, data3);
}

#[test]
fn invalid_id_offset() {
    // value in "id_offset" key must have u64 big endian format data

    let path1 = "tmp/import_error1";
    let path2 = "tmp/import_error2";
    let config1 = Config::default().path(path1).temporary(true);
    let config2 = Config::default().path(path2).temporary(true);

    let storage1 = SledStorage::try_from(config1).unwrap();
    let export = storage1.export().unwrap();

    let mut storage2 = SledStorage::try_from(config2).unwrap();
    storage2
        .tree
        .insert("id_offset", "something wrong value")
        .unwrap();

    assert_eq!(
        storage2.import(export),
        Err(Error::StorageMsg(
            "could not convert slice to array".to_owned()
        ))
    );
}
