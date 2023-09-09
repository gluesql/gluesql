use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(insert, {
    let glue = get_glue!();

    // create table - Foo
    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .add_column("rate FLOAT DEFAULT 0.0")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

    // create table - Bar
    let actual = table("Bar")
        .create_table()
        .add_column("id INTEGER UNIQUE NOT NULL")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Bar");

    // insert - basic
    let actual = table("Foo")
        .insert()
        .values(vec!["1, 'Fruit', 0.1", "2, 'Meat', 0.8"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert - basic");

    // insert - specifying columns
    let actual = table("Foo")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(3), text("Drink")]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - specifying columns");

    // insert - from source
    let actual = table("Bar")
        .insert()
        .as_select(table("Foo").select().project("id, name"))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    assert_eq!(actual, expected, "insert - from source");

    // select from Foo
    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name               | rate
        I64 | Str                | F64;
        1     "Fruit".to_owned()   0.1;
        2     "Meat".to_owned()    0.8;
        3     "Drink".to_owned()   0.0
    ));
    assert_eq!(actual, expected, "select from Foo");

    // select from Bar
    let actual = table("Bar").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Fruit".to_owned();
        2     "Meat".to_owned();
        3     "Drink".to_owned()
    ));
    assert_eq!(actual, expected, "select from Bar");
});
