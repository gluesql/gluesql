use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(pattern_matching, async move {
    let glue = get_glue!();

    // create table - Category
    let actual = table("Category")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);


    // insert into Category
    let actual = table("Category")
        .insert()
        .values(vec!["1, 'Fruit'", "2, 'Meat'", "3, 'Drink'", "4, 'drink'"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    test(actual, expected);

    // like
    let actual = table("Category")
                  .select()
                  .filter(col("name").like(text("D%")))
                  .execute(glue)
                  .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        3     "Drink".to_owned()
    ));
    test(actual, expected);

    // ilike
    let actual = table("Category")
                  .select()
                  .filter(col("name").ilike(text("D%")))
                  .execute(glue)
                  .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        3     "Drink".to_owned();
        4     "drink".to_owned()
    ));
    test(actual, expected);

    // not_like
    let actual = table("Category")
                  .select()
                  .filter(col("name").not_like(text("D%")))
                  .execute(glue)
                  .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Fruit".to_owned();
        2     "Meat".to_owned();
        4     "drink".to_owned()
    ));
    test(actual, expected);


    // not_ilike
    let actual = table("Category")
                  .select()
                  .filter(col("name").not_ilike(text("D%")))
                  .execute(glue)
                  .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Fruit".to_owned();
        2     "Meat".to_owned()
    ));
    test(actual, expected);
});
