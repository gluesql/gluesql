use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(delete, {
    let glue = get_glue!();

    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("score INTEGER")
        .add_column("flag BOOLEAN")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

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
    assert_eq!(actual, expected, "insert into Foo");

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     100     true;
        2     300     false;
        3     700     true
    ));
    assert_eq!(actual, expected, "select * from Foo");

    // delete using filter
    let actual = table("Foo")
        .delete()
        .filter(col("flag").eq(false))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    assert_eq!(actual, expected, "delete using filter");

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     100     true;
        3     700     true
    ));
    assert_eq!(actual, expected, "select * from Foo");

    // delete all
    let actual = table("Foo").delete().execute(glue).await;
    let expected = Ok(Payload::Delete(2));
    assert_eq!(actual, expected, "delete all");

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(Payload::Select {
        labels: vec!["id".to_owned(), "score".to_owned(), "flag".to_owned()],
        rows: vec![],
    });
    assert_eq!(actual, expected, "select * from Foo");
});
