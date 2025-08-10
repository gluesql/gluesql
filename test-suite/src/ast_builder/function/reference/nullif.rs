use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(nullif, {
    let glue = get_glue!();

    // create table - Foo
    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER")
        .add_column("name TEXT")
        .add_column("nickname TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

    // insert into Foo
    let actual = table("Foo")
        .insert()
        .columns("id, name, nickname")
        .values(vec![
            vec![num(100), text("hello"), text("bye")],
            vec![num(200), text("world"), text("world")],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert into Foo");

    // Return first argument when other column text different
    let actual = table("Foo")
        .select()
        .project("id")
        .project(f::nullif(col("name"), col("nickname")))
        .execute(glue)
        .await;
    let expected = Ok(select_with_null!(
        id | "NULLIF(\"name\", \"nickname\")";
        I64(100) Str("hello".to_owned());
        I64(200) Null
    ));
    assert_eq!(
        actual, expected,
        "return first argument with other column using nullif"
    );

    // nullif without table
    let actual = values(vec![
        vec![f::nullif(text("HELLO"), text("WORLD"))],
        vec![f::nullif(text("WORLD"), text("WORLD"))],
    ])
    .execute(glue)
    .await;
    let expected = Ok(select_with_null!(
        "column1";
        Str("HELLO".to_owned());
        Null
    ));
    assert_eq!(actual, expected, "nullif without table");
});
