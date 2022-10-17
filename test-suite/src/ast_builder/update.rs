use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(update, async move {
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

    // update all
    let actual = table("Foo")
        .update()
        .set("score", col("score").div(10))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Update(3));
    test(actual, expected);

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     10      true;
        2     30      false;
        3     70      true
    ));
    test(actual, expected);

    // update set multiple and use filter
    let actual = table("Foo")
        .update()
        .set("score", "score * 2 + 5")
        .set("flag", col("flag").negate())
        .filter(col("score").lte(30))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Update(2));
    test(actual, expected);

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | score | flag
        I64 | I64   | Bool;
        1     25      false;
        2     65      true;
        3     70      true
    ));
    test(actual, expected);
});
