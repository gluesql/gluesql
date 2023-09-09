use {
    crate::*,
    chrono::{NaiveDate, NaiveDateTime},
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(coalesce, {
    let glue = get_glue!();

    // create table - Foo
    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("first TEXT")
        .add_column("second INTEGER")
        .add_column("third TIMESTAMP")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Foo");

    // insert into Foo
    let actual = table("Foo")
        .insert()
        .columns("id, first, second, third")
        .values(vec![
            vec![num(100), text("visible"), null(), null()],
            vec![num(200), null(), num(42), null()],
            vec![num(300), null(), null(), timestamp("2023-06-01 12:00:00")],
            vec![num(400), null(), null(), null()],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into Foo");

    let actual = table("Foo")
        .select()
        .project("id")
        .project(f::coalesce(vec![
            null(),
            col("first"),
            col("second"),
            col("third"),
        ]))
        .order_by("id")
        .execute(glue)
        .await;
    let expected = Ok(select_with_null!(
        id        | r#"COALESCE(NULL, "first", "second", "third")"#;
        I64(100)    Str("visible".to_owned());
        I64(200)    I64(42);
        I64(300)    Timestamp("2023-06-01T12:00:00".parse::<NaiveDateTime>().unwrap());
        I64(400)    Null
    ));
    assert_eq!(actual, expected, "coalesce with table columns");

    let actual = values(vec![
        vec![f::coalesce(vec![text("뀨")])],
        vec![f::coalesce(vec![null(), num(1)])],
        vec![f::coalesce(vec![null(), null(), date("2000-01-01")])],
    ])
    .execute(glue)
    .await;
    let expected = Ok(select_with_null!(
        column1;
        Str("뀨".to_owned());
        I64(1);
        Date("2000-01-01".parse::<NaiveDate>().unwrap())
    ));
    assert_eq!(actual, expected, "coalesce without table");
});
