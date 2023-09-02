use {
    crate::*,
    gluesql_core::{error::TranslateError, prelude::Value::*},
};

test_case!(ditionary_index, {
    let g = get_tester!();

    g.run("CREATE TABLE Foo (id INT, name TEXT);").await;
    g.run("CREATE INDEX Foo_id ON Foo (id)").await;
    g.run("CREATE INDEX Foo_id_2 ON Foo (id + 2)").await;
    g.test(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME            | ORDER             | EXPRESSION         | UNIQUENESS;
            Str              | Str                   | Str               | Str                | Bool;
            "Foo".to_owned()   "Foo_id".to_owned()     "BOTH".to_owned()   "id".to_owned()      false;
            "Foo".to_owned()   "Foo_id_2".to_owned()   "BOTH".to_owned()   "id + 2".to_owned()  false
        ))
    ).await;

    g.run("CREATE TABLE Bar (id INT PRIMARY KEY, name TEXT);")
        .await;
    g.run("CREATE INDEX Bar_name_concat ON Bar (name + '_')")
        .await;
    g.test(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME                  | ORDER             | EXPRESSION               | UNIQUENESS;
            Str              | Str                         | Str               | Str                      | Bool;
            "Bar".to_owned()   "PRIMARY".to_owned()          "BOTH".to_owned()   "id".to_owned()            true;
            "Bar".to_owned()   "Bar_name_concat".to_owned()  "BOTH".to_owned()   "name + '_'".to_owned()  false;
            "Foo".to_owned()   "Foo_id".to_owned()           "BOTH".to_owned()   "id".to_owned()            false;
            "Foo".to_owned()   "Foo_id_2".to_owned()         "BOTH".to_owned()   "id + 2".to_owned()        false
        ))
    ).await;

    let test_cases = [
        (
            "DROP INDEX Bar.PRIMARY",
            Err(TranslateError::CannotDropPrimary.into()),
        ),
        (
            "CREATE INDEX Primary ON Foo (id)",
            Err(TranslateError::ReservedIndexName("Primary".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
