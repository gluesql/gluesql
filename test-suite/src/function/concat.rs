use crate::*;

test_case!(concat, async move {
    use gluesql_core::prelude::Value::*;
    use gluesql_core::translate::TranslateError;

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
        Ok(select!(
           myc
           Str;
           "abcd".to_owned()
        )),
        r#"select concat("ab", "cd") as myc from Concat;"#
    );

    test!(
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        )),
        r#"select concat("ab", "cd", "ef") as myconcat from Concat;"#
    );

    test!(
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        )),
        r#"select concat("ab", "cd", NULL, "ef") as myconcat from Concat;"#
    );
    // test with non string arguments
    test!(
        Ok(select!(
           myconcat
           Str;
           "1234563.14".to_owned()
        )),
        r#"select concat(123, 456, 3.14) as myconcat from Concat;"#
    );
    // test with zero arguments
    test!(
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "CONCAT".to_owned(),
            expected_minimum: 1,
            found: 0
        }
        .into()),
        r#"select concat() as myconcat from Concat;"#
    );
});
