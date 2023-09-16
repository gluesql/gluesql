use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(data_aggregation, {
    let glue = get_glue!();

    let actual = table("User")
        .create_table()
        .add_column("id INT")
        .add_column("name TEXT")
        .add_column("age INT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table");

    let actual = table("User")
        .insert()
        .columns("id, name, age")
        .values(vec![
            vec![num(1), text("Alice"), num(20)],
            vec![num(2), text("Bob"), num(30)],
            vec![num(3), text("Carol"), num(30)],
            vec![num(4), text("Dave"), num(50)],
            vec![num(5), text("Eve"), num(50)],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(5));
    assert_eq!(actual, expected, "insert");

    let actual = table("User")
        .select()
        .group_by("age")
        .project("age, count(*)")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        age | r#"count(*)"#;
        I64 | I64;
        20    1;
        30    2;
        50    2
    ));
    assert_eq!(actual, expected, "group by");

    let actual = table("User")
        .select()
        .group_by("age")
        .having("count(*) > 1")
        .project("age, count(*)")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        age | r#"count(*)"#;
        I64 | I64;
        30    2;
        50    2
    ));
    assert_eq!(actual, expected, "having");
});
