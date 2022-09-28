use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(basic, async move {
    let glue = get_glue!();

    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table");

    let actual = table("Foo")
        .insert()
        .columns("id, name")
        .values(vec![
            vec![num(100), text("Pickle")],
            vec![num(200), text("Lemon")],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert");

    let actual = table("Foo")
        .select()
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        100   "Pickle".to_owned();
        200   "Lemon".to_owned()
    ));
    assert_eq!(actual, expected, "select");

    let actual = table("Foo")
        .update()
        .set("id", col("id").mul(2))
        .filter(col("id").eq(200))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Update(1));
    assert_eq!(actual, expected, "update");

    let actual = table("Foo")
        .select()
        .filter("name = 'Lemon'")
        .project("id, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        400   "Lemon".to_owned()
    ));
    assert_eq!(actual, expected, "select after update");

    let actual = table("Foo")
        .delete()
        .filter(col("id").gt(200))
        .execute(glue)
        .await;
    let expected = Ok(Payload::Delete(1));
    assert_eq!(actual, expected, "delete");

    let actual = table("Foo").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        100   "Pickle".to_owned()
    ));
    assert_eq!(actual, expected, "select after delete");

    let actual = table("Foo").drop_table().execute(glue).await;
    let expected = Ok(Payload::DropTable);
    assert_eq!(actual, expected, "drop table");
});
