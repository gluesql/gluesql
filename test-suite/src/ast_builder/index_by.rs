use {
    crate::*,
    gluesql_core::{
        ast_builder::*,
        prelude::{Payload, Value::*},
    },
};

test_case!(index_by, {
    let glue = get_glue!();

    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

    let actual = table("Foo")
        .insert()
        .columns("id, name")
        .values(vec![vec![num(1), text("Drink")]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert - specifying columns");

    let actual = table("Foo")
        .index_by(primary_key().eq("1"))
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Drink".to_owned()
    ));
    assert_eq!(actual, expected, "basic select with index by");
});
