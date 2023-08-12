use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(pattern_matching, {
    let glue = get_glue!();

    // create table - Category
    let actual = table("Category")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Category");

    // insert into Category
    let actual = table("Category")
        .insert()
        .values(vec!["1, 'Meat'", "2, 'meat'", "3, 'Drink'", "4, 'drink'"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into - Category");

    // like
    let actual = table("Category")
        .select()
        .filter(
            col("name")
                .like(text("D%"))
                .or(col("name").like(text("M___"))),
        )
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Meat".to_owned();
        3     "Drink".to_owned()
    ));
    assert_eq!(actual, expected, "like");

    // ilike
    let actual = table("Category")
        .select()
        .filter(
            col("name")
                .ilike(text("D%"))
                .or(col("name").ilike(text("M___"))),
        )
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Meat".to_owned();
        2     "meat".to_owned();
        3     "Drink".to_owned();
        4     "drink".to_owned()
    ));
    assert_eq!(actual, expected, "ilike");

    // not_like
    let actual = table("Category")
        .select()
        .filter(
            col("name")
                .not_like(text("D%"))
                .and(col("name").not_like(text("M___"))),
        )
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        2     "meat".to_owned();
        4     "drink".to_owned()
    ));
    assert_eq!(actual, expected, "not_like");

    // not_ilike
    let actual = table("Category")
        .select()
        .filter(
            col("name")
                .not_ilike(text("D%"))
                .and(col("name").not_ilike(text("M___"))),
        )
        .execute(glue)
        .await;
    let expected = Ok(Payload::Select {
        labels: vec!["id".to_owned(), "name".to_owned()],
        rows: vec![],
    });
    assert_eq!(actual, expected, "not_ilike");
});
