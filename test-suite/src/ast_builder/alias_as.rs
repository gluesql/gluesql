use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(alias_as, async move {
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

    // create table - Item
    let actual = table("Item")
        .create_table()
        .add_column("id INTEGER")
        .add_column("category_id INTEGER")
        .add_column("name TEXT")
        .add_column("price INTEGER")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    // insert into Category
    let actual = table("Category")
        .insert()
        .values(vec!["1, 'Fruit'", "2, 'Meat'", "3, 'Drink'"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    test(actual, expected);

    // insert into Item
    let actual = table("Item")
        .insert()
        .values(vec![
            "100, 1, 'Pineapple', 40",
            "200, 2, 'Pork belly', 90",
            "300, 1, 'Strawberry', 30",
            "400, 3, 'Coffee', 25",
            "500, 3, 'Orange juice', 60",
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(5));
    test(actual, expected);

    // select -> derived subquery
    let actual = table("Item")
        .select()
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | category_id | name                      | price;
        I64 | I64         | Str                       | I64;
        100   1             "Pineapple".to_owned()      40;
        200   2             "Pork belly".to_owned()     90;
        300   1             "Strawberry".to_owned()     30;
        400   3             "Coffee".to_owned()         25;
        500   3             "Orange juice".to_owned()   60
    ));
    test(actual, expected);

    // select -> filter -> derived subquery
    let actual = table("Item")
        .select()
        .filter("id = 300")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | category_id | name                      | price;
        I64 | I64         | Str                       | I64;
        300   1             "Strawberry".to_owned()     30
    ));
    test(actual, expected);

    // select -> order_by -> derived subquery
    let actual = table("Item")
        .select()
        .order_by("price DESC")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | category_id | name                      | price;
        I64 | I64         | Str                       | I64;
        200   2             "Pork belly".to_owned()     90;
        500   3             "Orange juice".to_owned()   60;
        100   1             "Pineapple".to_owned()      40;
        300   1             "Strawberry".to_owned()     30;
        400   3             "Coffee".to_owned()         25
    ));
    test(actual, expected);

    // select -> project -> derived subquery
    let actual = table("Item")
        .select()
        .project("id")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id;
        I64;
        100;
        200;
        300;
        400;
        500
    ));
    test(actual, expected);

    // select -> join(cartesian) -> derived subquery
    let actual = table("Item")
        .alias_as("i")
        .select()
        .project("i.id AS item_id")
        .project("i.name AS item_name")
        .alias_as("Sub1")
        .select()
        .join_as("Category", "c")
        .alias_as("Sub2")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id | item_name                 | id  | name;
        I64     | Str                       | I64 | Str;
        100       "Pineapple".to_owned()      1     "Fruit".to_owned();
        100       "Pineapple".to_owned()      2     "Meat".to_owned();
        100       "Pineapple".to_owned()      3     "Drink".to_owned();
        200       "Pork belly".to_owned()     1     "Fruit".to_owned();
        200       "Pork belly".to_owned()     2     "Meat".to_owned();
        200       "Pork belly".to_owned()     3     "Drink".to_owned();
        300       "Strawberry".to_owned()     1     "Fruit".to_owned();
        300       "Strawberry".to_owned()     2     "Meat".to_owned();
        300       "Strawberry".to_owned()     3     "Drink".to_owned();
        400       "Coffee".to_owned()         1     "Fruit".to_owned();
        400       "Coffee".to_owned()         2     "Meat".to_owned();
        400       "Coffee".to_owned()         3     "Drink".to_owned();
        500       "Orange juice".to_owned()   1     "Fruit".to_owned();
        500       "Orange juice".to_owned()   2     "Meat".to_owned();
        500       "Orange juice".to_owned()   3     "Drink".to_owned()
    ));
    test(actual, expected);

    // select -> join -> on -> derived subquery
    let actual = table("Item")
        .alias_as("i")
        .select()
        .project("i.name AS item_name")
        .project("category_id")
        .alias_as("Sub1")
        .select()
        .join_as("Category", "c")
        .on("c.id = Sub1.category_id")
        .alias_as("Sub2")
        .select()
        .project("item_name")
        .project("name as category_name")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_name                 | category_name;
        Str                       | Str;
        "Pineapple".to_owned()      "Fruit".to_owned();
        "Pork belly".to_owned()     "Meat".to_owned();
        "Strawberry".to_owned()     "Fruit".to_owned();
        "Coffee".to_owned()         "Drink".to_owned();
        "Orange juice".to_owned()   "Drink".to_owned()
    ));
    test(actual, expected);
});
