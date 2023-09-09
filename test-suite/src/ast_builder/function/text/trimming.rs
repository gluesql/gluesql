use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(trimming, {
    let glue = get_glue!();

    // rtrim test
    let actual = table("Food")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Food");

    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![
            num(1),
            text("chicken   ").rtrim(Some(text(" "))),
        ]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - rtrim");

    let actual = table("Food").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned()
    ));
    assert_eq!(actual, expected, "select from Food");

    // ltrim test
    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![
            num(2),
            f::ltrim(text("   chicken"), Some(text(" "))),
        ]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - ltirm");

    let actual = table("Food").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned()
    ));
    assert_eq!(actual, expected, "select from Food");

    // ltrim and rtrim test
    let test_text = text("chicken").ltrim(Some(text("ch"))).rtrim(None);
    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(3), test_text]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - ltrim and rtrim");

    let actual = table("Food").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned();
        3     "icken".to_owned()
    ));
    assert_eq!(actual, expected, "select from Food");

    // rtrim and ltrim test
    let test_text = text("chicken").rtrim(Some(text("en"))).ltrim(None);
    let actual = table("Food")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(4), test_text]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - rtrim and ltrim");

    let actual = table("Food").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "chicken".to_owned();
        2     "chicken".to_owned();
        3     "icken".to_owned();
        4     "chick".to_owned()
    ));
    assert_eq!(actual, expected, "select from Food");
});
