use gluesql_core::ast_builder::left;
use {
    crate::*,
    gluesql_core::{
        ast_builder::{find_idx, num, position, right, table, text, Execute},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(position_and_indexing, async move {
    // test - find_idx

    let glue = get_glue!();
    let test_num = find_idx(text("strawberry"), text("berry"), None);

    // create table - Item
    let actual = table("Item")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("index INTEGER")
        .execute(glue)
        .await;

    let expected = Ok(Payload::Create);

    test(actual, expected);

    // insert table - Item
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(1), test_num]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;

    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6
    ));

    test(actual, expected);

    let test_num = find_idx(
        text("Oracle Database 12c Release"),
        text("as"),
        Some(num(15)),
    );

    // insert table - Item
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(2), test_num]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;

    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6;
        2     25
    ));

    test(actual, expected);

    let test_num = text("Oracle Database 12c Release").find_idx(text("as"), Some(num(15)));

    // insert table - Item
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(3), test_num]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    // select - table - Item
    let actual = table("Item").select().execute(glue).await;

    let expected = Ok(select!(
        id  | index
        I64 | I64;
        1     6;
        2     25;
        3     25
    ));

    test(actual, expected);

    // test - position
    let test_num = position(text("cake"), text("ke"));

    // insert table - Item
    let actual = table("Item")
        .insert()
        .columns("id, index")
        .values(vec![vec![num(4), test_num]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

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

    test(actual, expected);

    // test - left
    let actual = table("LeftRight")
        .create_table()
        .add_column("value TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    let test_str = left(text("Hello, World"), num(7));

    // insert table - Item
    let actual = table("LeftRight")
        .insert()
        .values(vec![vec![test_str]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    // select - table - Item
    let actual = table("LeftRight").select().execute(glue).await;

    let expected = Ok(select!(
        value
        Str;
        "Hello, ".to_owned()
    ));

    test(actual, expected);

    // test - right
    let test_str = right(text("Hello, World"), num(7));

    // insert table - Item
    let actual = table("LeftRight")
        .insert()
        .values(vec![vec![test_str]])
        .execute(glue)
        .await;

    let expected = Ok(Payload::Insert(1));
    test(actual, expected);

    // select - table - Item
    let actual = table("LeftRight").select().execute(glue).await;

    let expected = Ok(select!(
        value
        Str;
        "Hello, ".to_owned();
        ", World".to_owned()
    ));

    test(actual, expected);
});
