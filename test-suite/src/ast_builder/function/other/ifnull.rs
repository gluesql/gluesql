use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(ifnull, async move {
    let glue = get_glue!();

    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    let actual = table("Foo")
        .insert()
        .columns("id, name")
        .values(vec![
            vec![num(100), text("Pickle")],
            vec![num(200), null()],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    test(actual, expected);

    let actual = table("Foo")
        .select()
        .project("id")
        .project(col("name").ifnull(text("isnull")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "IFNULL(\"name\", 'isnull')"
        I64 | Str;
        100   "Pickle".to_owned();
        200   "isnull".to_owned()
    ));
    test(actual, expected);

    
});
