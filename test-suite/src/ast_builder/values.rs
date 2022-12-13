use {
    crate::*,
    gluesql_core::{ast_builder::*, prelude::Value::*},
};

test_case!(values_test, async move {
    let glue = get_glue!();

    let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
        .execute(glue)
        .await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        1         "Glue".to_owned();
        2         "SQL".to_owned();
        3         "Rust".to_owned()

    ));
    test(actual, expected);

    let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
        .order_by("column2 desc")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        2         "SQL".to_owned();
        3         "Rust".to_owned();
        1         "Glue".to_owned()
    ));
    test(actual, expected);

    let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
        .offset(1)
        .execute(glue)
        .await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        2         "SQL".to_owned();
        3         "Rust".to_owned()
    ));
    test(actual, expected);

    let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
        .limit(2)
        .execute(glue)
        .await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        1         "Glue".to_owned();
        2         "SQL".to_owned()
    ));
    test(actual, expected);

    let actual = values(vec!["1, 'Glue'", "2, 'SQL'", "3, 'Rust'"])
        .alias_as("Sub")
        .select()
        .project("column1 AS id")
        .project("column2 AS name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Glue".to_owned();
        2     "SQL".to_owned();
        3     "Rust".to_owned()
    ));
    test(actual, expected);
});
