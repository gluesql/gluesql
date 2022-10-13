use {crate::*, gluesql_core::prelude::Value::*};

test_case!(ditionary_index, async move {
    run!("CREATE TABLE Foo (id INT);");
    run!("CREATE INDEX Foo_id ON Foo (id ASC)");
    run!("CREATE INDEX Foo_id_1 ON Foo (id + 1 DESC)");
    run!("CREATE INDEX Foo_id_2 ON Foo (id * 2)");

    let test_cases = [(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME           | ORDER             | EXPRESSION;
            Str              | Str                  | Str               | Str;
            "Foo".to_owned()   "Foo_id".to_owned()    "BOTH".to_owned()    "id".to_owned();
            "Foo".to_owned()   "Foo_id_1".to_owned()  "BOTH".to_owned()   "id + 1".to_owned();
            "Foo".to_owned()   "Foo_id_2".to_owned()  "BOTH".to_owned()   "id * 2".to_owned()
        )),
    )];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
