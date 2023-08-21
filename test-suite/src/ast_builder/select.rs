use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(select, {
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

    // basic select
    let actual = table("Category").select().execute(glue).await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        1     "Fruit".to_owned();
        2     "Meat".to_owned();
        3     "Drink".to_owned()
    ));
    assert_eq!(actual, expected, "basic select");

    // filter (WHERE name = "Meat")
    let actual = table("Category")
        .select()
        .filter("name = 'Meat'")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | name
        I64 | Str;
        2     "Meat".to_owned()
    ));
    assert_eq!(actual, expected, "filter (WHERE name = 'Meat')");

    // inner join
    let actual = table("Item")
        .alias_as("i")
        .select()
        .join_as("Category", "c")
        .on("c.id = i.category_id")
        .filter("c.name = 'Fruit' OR c.name = 'Meat'")
        .project("i.name AS item")
        .project("c.name AS category")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        item                    | category
        Str                     | Str;
        "Pineapple".to_owned()    "Fruit".to_owned();
        "Pork belly".to_owned()   "Meat".to_owned();
        "Strawberry".to_owned()   "Fruit".to_owned()
    ));
    assert_eq!(actual, expected, "inner join");

    // left outer join
    let actual = table("Category")
        .select()
        .left_join("Item")
        .on(col("Category.id")
            .eq(col("Item.category_id"))
            .and(col("price").gt(50)))
        .project(vec![
            "Category.name AS category",
            "Item.name AS item",
            "price",
        ])
        .execute(glue)
        .await;
    let expected = Ok(select_with_null!(
        category                | item                           | price;
        Str("Fruit".to_owned())   Null                             Null;
        Str("Meat".to_owned())    Str("Pork belly".to_owned())     I64(90);
        Str("Drink".to_owned())   Str("Orange juice".to_owned())   I64(60)
    ));
    assert_eq!(actual, expected, "left outer join");

    // group by - having
    let actual = table("Item")
        .select()
        .join("Category")
        .on(col("Category.id").eq("Item.category_id"))
        .group_by("Item.category_id")
        .having("SUM(Item.price) > 80")
        .project("Category.name AS category")
        .project("SUM(Item.price) AS sum_price")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        category           | sum_price
        Str                | I64;
        "Meat".to_owned()    90;
        "Drink".to_owned()   85
    ));
    assert_eq!(actual, expected, "group by - having");

    // order by
    let actual = table("Item")
        .select()
        .project("name, price")
        .order_by("price DESC")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                      | price
        Str                       | I64;
        "Pork belly".to_owned()     90;
        "Orange juice".to_owned()   60;
        "Pineapple".to_owned()      40;
        "Strawberry".to_owned()     30;
        "Coffee".to_owned()         25
    ));
    assert_eq!(actual, expected, "order by");

    // offset, limit
    let actual = table("Item")
        .select()
        .project("name, price")
        .order_by("price DESC")
        .offset(1)
        .limit(2)
        .execute(glue)
        .await;
    let expected = Ok(select!(
        name                      | price
        Str                       | I64;
        "Orange juice".to_owned()   60;
        "Pineapple".to_owned()      40
    ));
    assert_eq!(actual, expected, "offset, limit");
});
