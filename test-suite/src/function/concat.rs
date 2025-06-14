use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(concat, {
    let g = get_tester!();

    g.test(
        "select concat('ab', 'cd') as myc;",
        Ok(select!(
           myc
           Str;
           "abcd".to_owned()
        )),
    )
    .await;

    g.test(
        "select concat('ab', 'cd', 'ef') as myconcat;",
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        )),
    )
    .await;

    g.test(
        "select concat('ab', 'cd', NULL, 'ef') as myconcat;",
        Ok(select_with_null!(myconcat; Null)),
    )
    .await;

    g.test(
        "select concat(DATE '2020-06-11', DATE '2020-16-3') as myconcat;",
        Err(ValueError::FailedToParseDate("2020-16-3".to_owned()).into()),
    )
    .await;

    // test with non string arguments
    g.test(
        "select concat(123, 456, 3.14) as myconcat;",
        Ok(select!(
           myconcat
           Str;
           "1234563.14".to_owned()
        )),
    )
    .await;
    // test with zero arguments
    g.test(
        r#"select concat() as myconcat;"#,
        Err(ValueError::EmptyArgNotAllowedInConcat.into()),
    )
    .await;

    g.test(
        r#"SELECT CONCAT(
            CAST('[1, 2, 3]' AS LIST),
            CAST('["one", "two", "three"]' AS LIST)
        ) AS myconcat;"#,
        Ok(select!(
           myconcat
           List;
           vec![I64(1), I64(2), I64(3), Str("one".to_owned()), Str("two".to_owned()), Str("three".to_owned())]
        ))
    ).await;
});
