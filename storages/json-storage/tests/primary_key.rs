use {
    gluesql_core::prelude::{
        Glue,
        {Payload, Value::*},
    },
    gluesql_json_storage::JsonStorage,
    std::fs::remove_dir_all,
    test_suite::{concat_with, row, select, stringify_label},
};

#[tokio::test]
async fn json_primary_key() {
    let path = "tmp/json_primary_key/";
    if let Err(e) = remove_dir_all(path) {
        println!("fs::remove_file {:?}", e);
    };
    let json_storage = JsonStorage::new(path).unwrap();
    let mut glue = Glue::new(json_storage);

    let cases = vec![
        (
            glue.execute(
                "CREATE TABLE SchemaWithPK (
                   id INT NOT NULL PRIMARY KEY,
                   name TEXT NULL
                 );",
            )
            .await,
            Ok(Payload::Create),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(2, 'second')")
                .await,
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(1, 'first')")
                .await,
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("SELECT * FROM SchemaWithPK").await,
            Ok(select!(
                id  | name
                I64 | Str;
                1     "first".to_owned();
                2     "second".to_owned()
            )),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
