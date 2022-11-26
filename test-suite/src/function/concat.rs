use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        prelude::Value::{self, *},
        translate::TranslateError,
    },
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
        Ok(select_with_null!(myconcat; Null))
    );

    test!(
        r#"select concat() as myconcat from Concat;"#,
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "CONCAT".to_owned(),
            expected_minimum: 1,
            found: 0
        }
        .into())
    );

    test!(
        r#"select concat(DATE "2020-06-11", DATE "2020-16-3") as myconcat from Concat;"#,
        Err(ValueError::FailedToParseDate("2020-16-3".to_owned()).into())
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

    let l = |s: &str| Value::parse_json_list(s).unwrap();

    test!(
        r#"select concat(["ab","cd"], ["de","fg"]) as myconcat from Concat;"#,
        Ok(select!(
           myconcat
           List;
           vec!["ab", "cd", "de", "fg"].into_iter().map(l).collect::<Vec<_>>()
        ))
    );
});
