use {gluesql_core::prelude::Glue, gluesql_csv_storage::CsvStorage};

#[tokio::test]
async fn run_test() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("DROP TABLE IF EXISTS Foo, Test, Free;")
        .await
        .unwrap();
    glue.execute("CREATE TABLE Foo (id INTEGER)").await.unwrap();
    glue.execute("INSERT INTO Foo VALUES (1), (2), (3);")
        .await
        .unwrap();

    glue.execute("SELECT * FROM Foo").await.unwrap();

    // panic!("{a:#?}");

    glue.execute(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    )
    .await
    .unwrap();
    glue.execute("INSERT INTO Test VALUES (8, 80, true);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Test (num) VALUES (10);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Test (num, id) VALUES (20, 2);")
        .await
        .unwrap();
    glue.execute("INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true);")
        .await
        .unwrap();

    glue.execute("SELECT * FROM Test;").await.unwrap();
    // panic!("{a:#?}");

    glue.execute("CREATE TABLE Free;").await.unwrap();
    glue.execute(
        r#"
        INSERT INTO Free
        VALUES
            ('{ "a": 3 }'),
            ('{ "b": 100 }'),
            ('{ "name": "reduuuce", "a": null }');
    "#,
    )
    .await
    .unwrap();
}
