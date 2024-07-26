use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(concat, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Concat (
            id INTEGER,
            rate FLOAT,
            flag BOOLEAN,
            text TEXT,
            null_value TEXT NULL
        );
    ",
    )
    .await;
    g.run("INSERT INTO Concat VALUES (1, 2.3, TRUE, 'Foo', NULL);")
        .await;

    g.test(
        "select concat('ab', 'cd') as myc from Concat;",
        Ok(select!(
           myc
           Str;
           "abcd".to_owned()
        )),
    )
    .await;

    g.test(
        "select concat('ab', 'cd', 'ef') as myconcat from Concat;",
        Ok(select!(
           myconcat
           Str;
           "abcdef".to_owned()
        )),
    )
    .await;

    g.test(
        "select concat('ab', 'cd', NULL, 'ef') as myconcat from Concat;",
        Ok(select_with_null!(myconcat; Null)),
    )
    .await;

    g.test(
        "select concat(DATE '2020-06-11', DATE '2020-16-3') as myconcat from Concat;",
        Err(ValueError::FailedToParseDate("2020-16-3".to_owned()).into()),
    )
    .await;

    // test with non string arguments
    g.test(
        "select concat(123, 456, 3.14) as myconcat from Concat;",
        Ok(select!(
           myconcat
           Str;
           "1234563.14".to_owned()
        )),
    )
    .await;
    // test with zero arguments
    g.test(
        r#"select concat() as myconcat from Concat;"#,
        Err(ValueError::EmptyArgNotAllowedInConcat.into()),
    )
    .await;

    g.run(
        r#"
        CREATE TABLE ListTypeConcat (
            id INTEGER,
            items LIST,
            items2 LIST
        )"#,
    )
    .await;

    g.run(
        r#"
        INSERT INTO ListTypeConcat VALUES
            (1, '[1, 2, 3]', '["one", "two", "three"]');
        "#,
    )
    .await;

    g.test(
        r#"select concat(items, items2) as myconcat from ListTypeConcat;"#,
        Ok(select!(
           myconcat
           List;
           vec![I64(1), I64(2), I64(3), Str("one".to_owned()), Str("two".to_owned()), Str("three".to_owned())]
        ))
    ).await;
});
