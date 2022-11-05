use {crate::*, gluesql_core::prelude::Value::*};

test_case!(ditionary_index, async move {
    run!("CREATE TABLE Foo (id INT, name TEXT);");
    run!("CREATE INDEX Foo_id ON Foo (id)");
    run!("CREATE INDEX Foo_id_2 ON Foo (id + 2)");
    test!(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME            | ORDER             | EXPRESSION         | UNIQUENESS;
            Str              | Str                   | Str               | Str                | Bool;
            "Foo".to_owned()   "Foo_id".to_owned()     "BOTH".to_owned()   "id".to_owned()      false;
            "Foo".to_owned()   "Foo_id_2".to_owned()   "BOTH".to_owned()   "id + 2".to_owned()  false
        ))
    );

    run!("CREATE TABLE Bar (id INT PRIMARY KEY, name TEXT);");
    run!("CREATE INDEX Bar_name_concat ON Bar (name + '_')");
    test!(
        "SELECT * FROM GLUE_INDEXES",
        Ok(select!(
            TABLE_NAME       | INDEX_NAME                  | ORDER             | EXPRESSION               | UNIQUENESS;
            Str              | Str                         | Str               | Str                      | Bool;
            "Bar".to_owned()   "PRIMARY".to_owned()          "BOTH".to_owned()   "id".to_owned()            true;
            "Bar".to_owned()   "Bar_name_concat".to_owned()  "BOTH".to_owned()   "name + '_'".to_owned()  false;
            "Foo".to_owned()   "Foo_id".to_owned()           "BOTH".to_owned()   "id".to_owned()            false;
            "Foo".to_owned()   "Foo_id_2".to_owned()         "BOTH".to_owned()   "id + 2".to_owned()        false
        ))
    );
});
