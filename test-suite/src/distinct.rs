use {
    crate::*,
    gluesql_core::{data::Value, prelude::Value::*},
    serde_json::json,
};

test_case!(distinct, {
    let g = get_tester!();

    g.run("CREATE TABLE Item (id INTEGER, name TEXT, price INTEGER)")
        .await;
    g.run("INSERT INTO Item VALUES (1, 'Apple', 100), (2, 'Banana', NULL), (1, 'Apple', 100), (3, NULL, 200)").await;

    g.named_test(
        "DISTINCT single column",
        "SELECT DISTINCT name FROM Item WHERE name IS NOT NULL ORDER BY name",
        Ok(select!(name; Str; "Apple".to_owned(); "Banana".to_owned())),
    )
    .await;

    g.named_test(
        "DISTINCT multiple columns",
        "SELECT DISTINCT id, name FROM Item ORDER BY id",
        Ok(select_with_null!(
            id | name;
            I64(1) Str("Apple".to_owned());
            I64(2) Str("Banana".to_owned());
            I64(3) Null
        )),
    )
    .await;

    g.run("CREATE TABLE Restaurant (id INTEGER, menu MAP)")
        .await;
    g.run(
        r#"
        INSERT INTO Restaurant VALUES
        (1, '{"dish": "pizza", "price": 12000}'),
        (2, '{"dish": "pizza", "price": 12000}'),
        (3, '{"dish": "pasta", "price": 15000}')
    "#,
    )
    .await;

    g.named_test(
        "DISTINCT with Map menu data",
        "SELECT DISTINCT menu FROM Restaurant ORDER BY UNWRAP(menu, 'price')",
        Ok(select_with_null!(
            menu;
            Value::parse_json_map(r#"{"dish": "pizza", "price": 12000}"#).unwrap();
            Value::parse_json_map(r#"{"dish": "pasta", "price": 15000}"#).unwrap()
        )),
    )
    .await;

    g.run("CREATE TABLE FoodOrders").await;
    g.run(
        r#"
        INSERT INTO FoodOrders VALUES
        ('{"food": "burger", "quantity": 2}'),
        ('{"food": "burger", "quantity": 2}'),
        ('{"food": "chicken", "quantity": 1}')
    "#,
    )
    .await;

    g.named_test(
        "DISTINCT with schemaless food orders (Row::Map case)",
        "SELECT DISTINCT * FROM FoodOrders",
        Ok(select_map!(
            json!({"food": "burger", "quantity": 2}),
            json!({"food": "chicken", "quantity": 1})
        )),
    )
    .await;
});
