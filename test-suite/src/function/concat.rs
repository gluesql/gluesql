use {
    crate::*,
    gluesql_core::{prelude::Value::*, translate::TranslateError},
};

test_case!(concat, async move {
    run!(
        "
        CREATE TABLE Concat (
            id INTEGER,
            rate FLOAT,
            flag BOOLEAN,
            text TEXT,
            null_value TEXT NULL,
        );
    "
    );
    run!(r#"INSERT INTO Concat VALUES (1, 2.3, TRUE, "Foo", NULL);"#);

    test!(
        r#"select concat("ab", "cd") as myc from Concat;"#,
        Ok(select!(
           myc
           Str;
           "abcd".to_owned()
        ))
    );

    test!(
        r#"select concat("ab", "cd", "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        ))
    );

    test!(
        r#"select concat("ab", "cd", NULL, "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        ))
    );
    // test with non string arguments
    test!(
        r#"select concat(123, 456, 3.14) as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "1234563.14".to_owned()
        ))
    );
    // test with zero arguments
    test!(
        r#"select concat() as myconcat from Concat;"#,
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "CONCAT".to_owned(),
            expected_minimum: 1,
            found: 0
        }
        .into())
    );
});
