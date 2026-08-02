use {
    crate::*,
    gluesql_core::{
        error::{Error, EvaluateError},
        executor::Payload,
        prelude::Value::*,
        query_builder::*,
    },
};

test_case!(select, {
    let glue = get_glue!();

    // create table - Category
    let actual = table("Category")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("name TEXT")
        .execute(glue);
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Category");

    // create table - Item
    let actual = table("Item")
        .create_table()
        .add_column("id INTEGER")
        .add_column("category_id INTEGER")
        .add_column("name TEXT")
        .add_column("price INTEGER")
        .execute(glue);
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Item");

    // insert into Category
    let actual = table("Category")
        .insert()
        .values(vec!["1, 'Fruit'", "2, 'Meat'", "3, 'Drink'"])
        .execute(glue);
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
        .execute(glue);
    let expected = Ok(Payload::Insert(5));
    assert_eq!(actual, expected, "insert into Item");

    // basic select
    let actual = table("Category").select().execute(glue);
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
        .execute(glue);
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
        .execute(glue);
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
        .execute(glue);
    let expected = Ok(select_with_null!(
        category                | item                           | price;
        Str("Fruit".to_owned())   Null                             Null;
        Str("Meat".to_owned())    Str("Pork belly".to_owned())     I64(90);
        Str("Drink".to_owned())   Str("Orange juice".to_owned())   I64(60)
    ));
    assert_eq!(actual, expected, "left outer join");

    // explicit hash join skips NULL right keys
    let actual = table("Item")
        .select()
        .left_join("Category")
        .hash_executor("NULL", "Item.category_id")
        .filter("Item.id = 100")
        .project("Item.name AS item")
        .project("Category.name AS category")
        .execute(glue);
    let expected = Ok(select_with_null!(
        item                        | category;
        Str("Pineapple".to_owned())   Null
    ));
    assert_eq!(actual, expected, "explicit hash join skips NULL right keys");

    // explicit hash join propagates right key errors during preparation
    let actual = table("Item")
        .select()
        .join("Category")
        .hash_executor("1 / 0", "Item.category_id")
        .execute(glue);
    let expected = Err(Error::Evaluate(EvaluateError::DivisorShouldNotBeZero));
    assert_eq!(
        actual, expected,
        "explicit hash join propagates right key errors during preparation"
    );

    // group by - having
    let actual = table("Item")
        .select()
        .join("Category")
        .on(col("Category.id").eq("Item.category_id"))
        .group_by("Item.category_id")
        .having("SUM(Item.price) > 80")
        .project("Category.name AS category")
        .project("SUM(Item.price) AS sum_price")
        .execute(glue);
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
        .execute(glue);
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
        .execute(glue);
    let expected = Ok(select!(
        name                      | price
        Str                       | I64;
        "Orange juice".to_owned()   60;
        "Pineapple".to_owned()      40
    ));
    assert_eq!(actual, expected, "offset, limit");

    // distinct
    let actual = table("Item")
        .select()
        .project("category_id")
        .order_by("category_id")
        .distinct()
        .execute(glue);
    let expected = Ok(select!(
        category_id
        I64;
        1;
        2;
        3
    ));
    assert_eq!(actual, expected, "distinct");

    // distinct with multiple columns
    let actual = table("Item")
        .select()
        .project("category_id, price")
        .order_by("category_id, price")
        .distinct()
        .execute(glue);
    let expected = Ok(select!(
        category_id | price
        I64         | I64;
        1             30;
        1             40;
        2             90;
        3             25;
        3             60
    ));
    assert_eq!(actual, expected, "distinct with multiple columns");

    // distinct * (all columns)
    let actual = table("Item").select().project("*").distinct().execute(glue);
    let expected = Ok(select!(
        id | category_id | name | price
        I64 | I64 | Str | I64;
        100 1 "Pineapple".to_owned() 40;
        200 2 "Pork belly".to_owned() 90;
        300 1 "Strawberry".to_owned() 30;
        400 3 "Coffee".to_owned() 25;
        500 3 "Orange juice".to_owned() 60
    ));
    assert_eq!(actual, expected, "distinct * (all columns)");
});
