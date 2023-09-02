use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(position_and_indexing, {
    // test - find_idx

    let glue = get_glue!();

    // create table - Item
    let actual = table("Item")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("index INTEGER")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Item");

    // insert table - Item
    let test_num = f::find_idx(text("strawberry"), text("berry"), None);
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(1), test_num]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - find_idx");

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;
    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6
    ));
    assert_eq!(actual, expected, "select from Item");

    let test_num = f::find_idx(
        text("Oracle Database 12c Release"),
        text("as"),
        Some(num(15)),
    );
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(2), test_num]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - find_idx");

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;
    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6;
        2     25
    ));
    assert_eq!(actual, expected, "select from Item");

    let test_num = text("Oracle Database 12c Release").find_idx(text("as"), Some(num(15)));
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(3), test_num]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - find_idx");

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;
    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6;
        2     25;
        3     25
    ));
    assert_eq!(actual, expected, "select from Item");

    // test - position
    let test_num = f::position(text("cake"), text("ke"));

    // insert table - Item
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(4), test_num]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - position");

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;
    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6;
        2     25;
        3     25;
        4     3
    ));
    assert_eq!(actual, expected, "select from Item");

    // test - left
    let actual = table("LeftRight")
        .create_table()
        .add_column("value TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - LeftRight");

    let test_str = f::left(text("Hello, World"), num(7));

    // insert table - Item
    let actual = table("LeftRight")
        .insert()
        .values(vec![vec![test_str]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - left");

    // select - table - Item
    let actual = table("LeftRight").select().execute(glue).await;
    let expected = Ok(select!(
        value
        Str;
        "Hello, ".to_owned()
    ));
    assert_eq!(actual, expected, "select from LeftRight");

    // test - right
    let test_str = f::right(text("Hello, World"), num(7));
    // insert table - Item
    let actual = table("LeftRight")
        .insert()
        .values(vec![vec![test_str]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - right");

    // select - table - Item
    let actual = table("LeftRight").select().execute(glue).await;
    let expected = Ok(select!(
        value
        Str;
        "Hello, ".to_owned();
        ", World".to_owned()
    ));
    assert_eq!(actual, expected, "select from LeftRight");
});
