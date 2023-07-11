use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value},
    },
};

test_case!(replace, async move {
    test! {
        name: "test replace function works while creating a table simultaneously",
        sql: "CREATE TABLE Item (name TEXT DEFAULT REPLACE('SQL Tutorial', 'T', 'M'))",
        expected: Ok(Payload::Create)
    };
    test! {
        name: "test if the sample string gets inserted to table",
        sql: "INSERT INTO Item VALUES ('Tticky GlueTQL')",
        expected: Ok(Payload::Insert(1))
    };
    test! {
        name: "check id the replace function works with the previously inserted string",
        sql: "SELECT REPLACE(name,'T','S') AS test FROM Item;",
        expected: Ok(select!(
            "test"
            Value::Str;
            "Sticky GlueSQL".to_owned()
        ))
    };
    test! {
        name: "test when one argument was given",
        sql: "SELECT REPLACE('GlueSQL') AS test FROM Item",
        expected: Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "REPLACE".to_owned(),
            expected: 3,
            found: 1,
        }.into())

    };
    test! {
        name: "test when two arguments were given",
        sql: "SELECT REPLACE('GlueSQL','G') AS test FROM Item",
        expected: Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "REPLACE".to_owned(),
            expected: 3,
            found: 2,
        }.into())
    };
    test! {
        name: "test when integers were given as arguments instead of string values",
        sql: "SELECT REPLACE(1,1,1) AS test FROM Item",
        expected: Err(EvaluateError::FunctionRequiresStringValue("REPLACE".to_owned()).into())
    };
    test! {
        name: "test when null was given as argument",
        sql: "SELECT REPLACE(name, null,null) AS test FROM Item",
        expected: Ok(select_with_null!(test; Value::Null))
    };
    test! {
        name: "test if the table can be created will null value",
        sql: "CREATE TABLE NullTest (name TEXT null)",
        expected: Ok(Payload::Create)
    };
    test! {
        name: "test if null can be inserted",
        sql: "INSERT INTO NullTest VALUES (null)",
        expected: Ok(Payload::Insert(1))
    };
    test! {
        name: "test if replace works in null value",
        sql: "SELECT REPLACE(name, 'G','T') AS test FROM NullTest",
        expected: Ok(select_with_null!(test; Value::Null))
    };
    run!("DELETE FROM Item");
});
