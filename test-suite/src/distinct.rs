use {
    crate::*,
    gluesql_core::{data::Value, error::SelectError, prelude::Value::*},
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

    // --- SQL standard: DISTINCT + ORDER BY ---
    // PostgreSQL docs (SELECT, steps 5-8):
    //   5. projection, 6. DISTINCT, 8. ORDER BY
    // After DISTINCT only projected columns remain; ORDER BY must use them.

    // ORDER BY on a projected column — valid.
    g.named_test(
        "DISTINCT with ORDER BY on projected column is valid",
        "SELECT DISTINCT id FROM Item ORDER BY id",
        Ok(select!(id; I64; 1; 2; 3)),
    )
    .await;

    // ORDER BY positional index is always valid regardless of DISTINCT.
    g.named_test(
        "DISTINCT with ORDER BY positional index is valid",
        "SELECT DISTINCT id FROM Item ORDER BY 1",
        Ok(select!(id; I64; 1; 2; 3)),
    )
    .await;

    // ORDER BY on a column that is NOT in the select list must be rejected.
    // (PostgreSQL: "for SELECT DISTINCT, ORDER BY expressions must appear in
    //  select list")
    g.named_test(
        "DISTINCT with ORDER BY non-projected column is an error",
        "SELECT DISTINCT name FROM Item ORDER BY id",
        Err(SelectError::DistinctOrderByNotInSelectList("id".to_owned()).into()),
    )
    .await;

    // Non-DISTINCT SELECT can still ORDER BY a non-projected column (standard).
    g.named_test(
        "non-DISTINCT SELECT can ORDER BY a non-projected column",
        "SELECT name FROM Item WHERE name IS NOT NULL ORDER BY id",
        Ok(select!(
            name;
            Str;
            "Apple".to_owned();
            "Apple".to_owned();
            "Banana".to_owned()
        )),
    )
    .await;
});
