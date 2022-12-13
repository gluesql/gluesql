use {
    crate::*,
    gluesql_core::{ast_builder::*, prelude::Value::*},
};

test_case!(values_test, async move {
    let glue = get_glue!();

    let actual = values(vec!["1, 'GLUE'", "2, 'SQL'"]).execute(glue).await;
    let expected = Ok(select!(
        column1 | column2
        I64     | Str;
        1         "GLUE".to_owned();
        2         "SQL".to_owned()
    ));
    test(actual, expected);
});
