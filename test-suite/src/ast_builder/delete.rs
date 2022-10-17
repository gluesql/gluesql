use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(delete, async move {
    let glue = get_glue!();

    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("score INTEGER")
        .add_column("flag BOOLEAN")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    let actual = table("Foo")
        .insert()
        .values(vec![
            vec![num(1), num(100), true.into()],
            vec![num(2), num(300), false.into()],
            vec![num(3), num(700), true.into()],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    test(actual, expected);

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     100     true;
        2     300     false;
        3     700     true
    ));
    test(actual, expected);

    // delete using filter
    let actual = table("Foo")
        .delete()
        .filter(col("flag").eq(false))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    test(actual, expected);

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     100     true;
        3     700     true
    ));
    test(actual, expected);

    // delete all
    let actual = table("Foo").delete().execute(glue).await;
    let expected = Ok(Payload::Delete(2));
    test(actual, expected);

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(Payload::Select {
        labels: vec!["id".to_owned(), "score".to_owned(), "flag".to_owned()],
        rows: vec![],
    });
    test(actual, expected);
});
