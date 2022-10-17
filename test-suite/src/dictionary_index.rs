use {crate::*, gluesql_core::prelude::Value::*};

test_case!(ditionary_index, async move {
    run!("CREATE TABLE Foo (id INT PRIMARY KEY, name TEXT);");
    run!("CREATE INDEX Foo_id_1 ON Foo (id + 1)");
    run!("CREATE INDEX Foo_name_upper ON Foo (name + '_')");

    let test_cases = [(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME                  | ORDER             | EXPRESSION               | UNIQUENESS;
            Str              | Str                         | Str               | Str                      | Bool;
            "Foo".to_owned()   "PRIMARY".to_owned()          "BOTH".to_owned()   "id".to_owned()            true;
            "Foo".to_owned()   "Foo_id_1".to_owned()         "BOTH".to_owned()   "id + 1".to_owned()        false;
            "Foo".to_owned()   "Foo_name_upper".to_owned()   "BOTH".to_owned()   "name + \"_\"".to_owned()   false
        )),
    )];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
