use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(entries, async move {

    test! {
        name: "test entries function works while creating a table simultaneously",
        sql: "CREATE TABLE Item (map MAP)",
        expected: Ok(Payload::Create)
    };
    test! {
        name: "test if the sample string gets inserted to table",
        sql: "INSERT INTO Item VALUES ('{\"name\":\"GlueSQL\"}')",
        expected: Ok(Payload::Insert(1))
    };
    test! {
        name: "check id the entries function works with the previously inserted string",
        sql: "SELECT ENTRIES(map) AS test FROM Item;",
        expected: Ok(select!(
            "test";
            List;
            vec![
                List(vec![Str("name".to_owned()), Str("GlueSQL".to_owned())])
            ]
        ))
    };
});
