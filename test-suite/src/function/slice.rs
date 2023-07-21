use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(slice, async move {
    test! {
        name: "test replace function works while creating a table simultaneously",
        sql: "CREATE TABLE Test (list LIST)",
        expected: Ok(Payload::Create)
    };
    test! {
        name: "test if the sample string gets inserted to table",
        sql: "INSERT INTO Test VALUES ('[1,2,3,4]')",
        expected: Ok(Payload::Insert(1))
    };
    test! {
        name: "check id the replace function works with the previously inserted string",
        sql: "SELECT SLICE(list, 1, 2) AS test FROM Item;",
        expected: Ok(select!(
            "test"
            List;
            vec![I64(2),I64(3)]
        ))
    };
});
