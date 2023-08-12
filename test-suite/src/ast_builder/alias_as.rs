use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(alias_as, {
    let glue = get_glue!();

    // create table - Category
    let actual = table("Category")
        .create_table()
        .add_column("category_id INTEGER PRIMARY KEY")
        .add_column("category_name TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Category");

    // create table - Item
    let actual = table("Item")
        .create_table()
        .add_column("item_id INTEGER")
        .add_column("category_id INTEGER")
        .add_column("item_name TEXT")
        .add_column("price INTEGER")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Item");

    // insert into Category
    let actual = table("Category")
        .insert()
        .values(vec!["1, 'Fruit'", "2, 'Meat'", "3, 'Drink'"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(3));
    assert_eq!(actual, expected, "insert into Category");

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
    assert_eq!(actual, expected, "insert into Item");

    // select -> derived subquery
    let actual = table("Item")
        .select()
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id  | category_id | item_name                 | price;
        I64      | I64         | Str                       | I64;
        100        1             "Pineapple".to_owned()      40;
        200        2             "Pork belly".to_owned()     90;
        300        1             "Strawberry".to_owned()     30;
        400        3             "Coffee".to_owned()         25;
        500        3             "Orange juice".to_owned()   60
    ));
    assert_eq!(actual, expected, "select -> derived subquery");

    // select -> filter -> derived subquery
    let actual = table("Item")
        .select()
        .filter("item_id = 300")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id  | category_id | item_name               | price;
        I64      | I64         | Str                     | I64;
        300        1             "Strawberry".to_owned()   30
    ));
    assert_eq!(actual, expected, "select -> filter -> derived subquery");

    // select -> project -> derived subquery
    let actual = table("Item")
        .select()
        .project("item_id")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id;
        I64;
        100;
        200;
        300;
        400;
        500
    ));
    assert_eq!(actual, expected, "select -> project -> derived subquery");

    // select -> join(cartesian) -> derived subquery
    let actual = table("Item")
        .alias_as("i")
        .select()
        .join_as("Category", "c")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id | category_id | item_name                 | price | category_id | category_name;
        I64     | I64         | Str                       | I64   | I64         | Str;
        100       1             "Pineapple".to_owned()      40      1             "Fruit".to_owned();
        100       1             "Pineapple".to_owned()      40      2             "Meat".to_owned();
        100       1             "Pineapple".to_owned()      40      3             "Drink".to_owned();
        200       2             "Pork belly".to_owned()     90      1             "Fruit".to_owned();
        200       2             "Pork belly".to_owned()     90      2             "Meat".to_owned();
        200       2             "Pork belly".to_owned()     90      3             "Drink".to_owned();
        300       1             "Strawberry".to_owned()     30      1             "Fruit".to_owned();
        300       1             "Strawberry".to_owned()     30      2             "Meat".to_owned();
        300       1             "Strawberry".to_owned()     30      3             "Drink".to_owned();
        400       3             "Coffee".to_owned()         25      1             "Fruit".to_owned();
        400       3             "Coffee".to_owned()         25      2             "Meat".to_owned();
        400       3             "Coffee".to_owned()         25      3             "Drink".to_owned();
        500       3             "Orange juice".to_owned()   60      1             "Fruit".to_owned();
        500       3             "Orange juice".to_owned()   60      2             "Meat".to_owned();
        500       3             "Orange juice".to_owned()   60      3             "Drink".to_owned()
    ));
    assert_eq!(
        actual, expected,
        "select -> join(cartesian) -> derived subquery"
    );

    // select -> join -> on -> derived subquery
    let actual = table("Item")
        .alias_as("i")
        .select()
        .join_as("Category", "c")
        .on("c.category_id = i.category_id")
        .alias_as("Sub")
        .select()
        .project("item_name")
        .project("category_name")
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
    assert_eq!(actual, expected, "select -> join -> on -> derived subquery");

    // select -> join -> hash -> derived subquery
    let actual = table("Item")
        .select()
        .join("Category")
        .hash_executor("Category.category_id", "Item.category_id")
        .alias_as("Sub")
        .select()
        .project("item_name")
        .project("category_name")
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
    assert_eq!(
        actual, expected,
        "select -> join -> hash -> derived subquery"
    );

    // select -> project -> derived subquery -> select -> group_by -> derived subquery
    let actual = table("Category")
        .select()
        .project("category_name")
        .alias_as("Sub1")
        .select()
        .group_by("category_name")
        .alias_as("Sub2")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        category_name;
        Str;
        "Fruit".to_owned();
        "Meat".to_owned();
        "Drink".to_owned()
    ));
    assert_eq!(
        actual, expected,
        "select -> project -> derived subquery -> select -> group_by -> derived subquery"
    );

    // select -> project -> derived subquery -> select -> group_by -> having -> derived subquery
    let actual = table("Category")
        .select()
        .project("category_name")
        .alias_as("Sub1")
        .select()
        .group_by("category_name")
        .having("category_name = 'Meat'")
        .alias_as("Sub2")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        category_name;
        Str;
        "Meat".to_owned()
    ));
    assert_eq!(
        actual, expected,
        "select -> project -> derived subquery -> select -> group_by -> having -> derived subquery"
    );

    // select -> order_by -> derived subquery
    let actual = table("Item")
        .select()
        .order_by("price DESC")
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id  | category_id | item_name                 | price;
        I64      | I64         | Str                       | I64;
        200        2             "Pork belly".to_owned()     90;
        500        3             "Orange juice".to_owned()   60;
        100        1             "Pineapple".to_owned()      40;
        300        1             "Strawberry".to_owned()     30;
        400        3             "Coffee".to_owned()         25
    ));
    assert_eq!(actual, expected, "select -> order_by -> derived subquery");

    // select -> offset -> derived subquery
    let actual = table("Item")
        .select()
        .offset(4)
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id  | category_id | item_name                 | price;
        I64      | I64         | Str                       | I64;
        500        3             "Orange juice".to_owned()   60
    ));
    assert_eq!(actual, expected, "select -> offset -> derived subquery");

    // select -> limit -> derived subquery
    let actual = table("Item")
        .select()
        .limit(1)
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item_id  | category_id | item_name                 | price;
        I64      | I64         | Str                       | I64;
        100        1             "Pineapple".to_owned()      40
    ));
    assert_eq!(actual, expected, "select -> limit -> derived subquery");

    // select -> offset -> limit -> derived subquery
    let actual = table("Item")
        .select()
        .offset(3)
        .limit(1)
        .alias_as("Sub")
        .select()
        .execute(glue)
        .await;
    let expected = Ok(select!(
         item_id  | category_id | item_name                 | price;
         I64      | I64         | Str                       | I64;
         400        3             "Coffee".to_owned()         25
    ));
    assert_eq!(
        actual, expected,
        "select -> offset -> limit -> derived subquery"
    );
});
