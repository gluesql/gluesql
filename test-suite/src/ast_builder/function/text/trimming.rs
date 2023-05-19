use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(trimming, async move {
    let glue = get_glue!();

    // rtrim test
    let test_text = text("chicken   ").rtrim(Some(text(" ")));

    let actual = table("Food")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue)
        .await;

    let expected = Ok(Payload::Create);
    test(actual, expected);

    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(1), test_text]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    let actual = table("Food").select().execute(glue).await;

    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned()
    ));

    test(actual, expected);

    // ltrim test
    let test_text = ltrim(text("   chicken"), Some(text(" ")));

    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(2), test_text]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    let actual = table("Food").select().execute(glue).await;

    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned()
    ));

    test(actual, expected);

    // ltrim and rtrim test
    let test_text = text("chicken").ltrim(Some(text("ch"))).rtrim(None);

    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(3), test_text]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    let actual = table("Food").select().execute(glue).await;

    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned();
        3     "icken".to_owned()
    ));
    test(actual, expected);

    // rtrim and ltrim test
    let test_text = text("chicken").rtrim(Some(text("en"))).ltrim(None);

    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(4), test_text]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    let actual = table("Food").select().execute(glue).await;

    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned();
        3     "icken".to_owned();
        4     "chick".to_owned()
    ));

    test(actual, expected);
});
