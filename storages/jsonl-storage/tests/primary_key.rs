use {
    gluesql_core::prelude::{
        Glue,
        {Payload, Value::*},
    },
    gluesql_jsonl_storage::JsonlStorage,
    std::fs::remove_dir_all,
    test_suite::{concat_with, row, select, stringify_label, test},
};

#[test]
fn jsonl_primary_key() {
    let path = "tmp/jsonl_primary_key/";
    if let Err(e) = remove_dir_all(&path) {
        println!("fs::remove_file {:?}", e);
    };
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let cases = vec![
        (
            glue.execute(
                "CREATE TABLE SchemaWithPK (
                   id INT NOT NULL PRIMARY KEY,
                   name TEXT NULL
                 );",
            ),
            Ok(Payload::Create),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(2, 'second')"),
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(1, 'first')"),
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("SELECT * FROM SchemaWithPK"),
            Ok(select!(
                id  | name
                I64 | Str;
                1     "first".to_owned();
                2     "second".to_owned()
            )),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
