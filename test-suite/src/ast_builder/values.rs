use {
    crate::*,
    gluesql_core::{ast_builder::*, prelude::Value::*},
};

test_case!(values, {
    use gluesql_core::ast_builder::values;
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
    assert_eq!(actual, expected, "values - row as string");

    let actual = values(vec![
        vec!["1", "'Glue'"],
        vec!["2", "'SQL'"],
        vec!["3", "'Rust'"],
    ])
    .execute(glue)
    .await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        1         "Glue".to_owned();
        2         "SQL".to_owned();
        3         "Rust".to_owned()

    ));
    assert_eq!(actual, expected, "values - row as vec");

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
    assert_eq!(actual, expected, "values - order by");

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
    assert_eq!(actual, expected, "values - offset");

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
    assert_eq!(actual, expected, "values - limit");

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
    assert_eq!(actual, expected, "values - alias as");
});
