use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        prelude::Value::{Null, Str},
    },
};

test_case!(padding, {
    let glue = get_glue!();

    let actual = values(vec![
        vec![f::lpad("'hello'", 10, None), f::rpad("'hello'", 10, None)],
        vec![
            f::lpad("'hello'", 10, Some("'ab'".into())),
            f::rpad("'hello'", 10, Some("'ab'".into())),
        ],
        vec![f::lpad("'hello'", 3, None), f::rpad("'hello'", 3, None)],
        vec![
            f::lpad("'hello'", 3, Some("'ab'".into())),
            f::rpad("'hello'", 3, Some("'ab'".into())),
        ],
        vec![f::lpad("NULL", 5, None), f::rpad("NULL", 5, None)],
    ])
    .alias_as("Sub")
    .select()
    .project("column1 AS lpaded")
    .project("column2 AS rpaded")
    .execute(glue)
    .await;
    let expected = Ok(select_with_null!(
        lpaded                       | rpaded;
        Str("     hello".to_owned())   Str("hello     ".to_owned());
        Str("ababahello".to_owned())   Str("helloababa".to_owned());
        Str("hel".to_owned())          Str("hel".to_owned());
        Str("hel".to_owned())          Str("hel".to_owned());
        Null                           Null
    ));
    assert_eq!(
        actual, expected,
        "lpad and rpad should pad the string with given length"
    );
});
