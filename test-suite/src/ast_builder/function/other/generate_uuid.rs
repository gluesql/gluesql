use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
    },
};

test_case!(generate_uuid, {
    let glue = get_glue!();

    // create table - Foo
    let actual = table("Foo")
        .create_table()
        .add_column("id UUID PRIMARY KEY")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

    // insert into Foo
    let actual = table("Foo")
        .insert()
        .columns("id")
        .values(vec![vec![f::generate_uuid()]])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(1));
    assert_eq!(actual, expected, "insert into Foo");
});
