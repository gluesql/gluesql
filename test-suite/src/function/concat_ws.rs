use {
    crate::*,
    gluesql_core::{prelude::Value::*, translate::TranslateError},
};

test_case!(concat_ws, async move {
    test!(
        r#"VALUES(CONCAT_WS(",", "AB", "CD", "EF"))"#,
        Ok(select!(
            column1
            Str;
            "AB,CD,EF".to_owned()
        ))
    );

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
        r#"select concat_ws("", "ab", "cd") as myc from Concat;"#,
        Ok(select!(
           myc
           Str;
           "abcd".to_owned()
        ))
    );

    test!(
        r#"select concat_ws("", "ab", "cd", "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        ))
    );

    test!(
        r#"select concat_ws(",", "ab", "cd", "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "ab,cd,ef".to_owned()
        ))
    );

    test!(
        r#"select concat_ws("/", "ab", "cd", "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "ab/cd/ef".to_owned()
        ))
    );

    test!(
        r#"select concat_ws("", "ab", "cd", NULL, "ef") as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        ))
    );

    test!(
        r#"select concat_ws("", 123, 456, 3.14) as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           Str;
           "1234563.14".to_owned()
        ))
    );

    test!(
        r#"select concat_ws() as myconcat from Concat;"#,
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "CONCAT_WS".to_owned(),
            expected_minimum: 2,
            found: 0
        }
        .into())
    );
});
