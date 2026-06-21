use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value},
    },
};

test_case!(replace, {
    let g = get_tester!();

    g.named_test(
        "test replace function works while creating a table simultaneously",
        "CREATE TABLE Item (name TEXT DEFAULT REPLACE('SQL Tutorial', 'T', 'M'))",
        Ok(Payload::Create),
    );
    g.named_test(
        "test if the sample string gets inserted to table",
        "INSERT INTO Item VALUES ('Tticky GlueTQL')",
        Ok(Payload::Insert(1)),
    );
    g.named_test(
        "check id the replace function works with the previously inserted string",
        "SELECT REPLACE(name,'T','S') AS test FROM Item;",
        Ok(select!(
            "test"
            Value::Str;
            "Sticky GlueSQL".to_owned()
        )),
    );
    g.named_test(
        "test when one argument was given",
        "SELECT REPLACE('GlueSQL') AS test FROM Item",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "REPLACE".to_owned(),
            expected: 3,
            found: 1,
        }
        .into()),
    );
    g.named_test(
        "test when two arguments were given",
        "SELECT REPLACE('GlueSQL','G') AS test FROM Item",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "REPLACE".to_owned(),
            expected: 3,
            found: 2,
        }
        .into()),
    );
    g.named_test(
        "test when integers were given as arguments instead of string values",
        "SELECT REPLACE(1,1,1) AS test FROM Item",
        Err(EvaluateError::FunctionRequiresStringValue("REPLACE".to_owned()).into()),
    );
    g.named_test(
        "test when null was given as argument",
        "SELECT REPLACE(name, null,null) AS test FROM Item",
        Ok(select_with_null!(test; Value::Null)),
    );
    g.named_test(
        "test if the table can be created will null value",
        "CREATE TABLE NullTest (name TEXT null)",
        Ok(Payload::Create),
    );
    g.named_test(
        "test if null can be inserted",
        "INSERT INTO NullTest VALUES (null)",
        Ok(Payload::Insert(1)),
    );
    g.named_test(
        "test if replace works in null value",
        "SELECT REPLACE(name, 'G','T') AS test FROM NullTest",
        Ok(select_with_null!(test; Value::Null)),
    );
    g.run("DELETE FROM Item");
});
