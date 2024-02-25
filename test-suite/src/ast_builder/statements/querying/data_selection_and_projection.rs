use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(data_selection_and_projection, {
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
        .filter(col("age").gt(30))
        .project("id, age, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | age | name;
        I64 | I64 | Str;
        4     50    "Dave".to_owned();
        5     50    "Eve".to_owned()
    ));
    assert_eq!(actual, expected, "filter");

    let actual = table("User")
        .select()
        .project("id, age, name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | age | name;
        I64 | I64 | Str;
        1     20    "Alice".to_owned();
        2     30    "Bob".to_owned();
        3     30    "Carol".to_owned();
        4     50    "Dave".to_owned();
        5     50    "Eve".to_owned()
    ));
    assert_eq!(actual, expected, "project");
});
