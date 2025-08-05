use {crate::*, gluesql_core::prelude::Value::*};

test_case!(distinct, {
    let g = get_tester!();

    g.run("CREATE TABLE Item (id INTEGER, name TEXT, price INTEGER)")
        .await;
    g.run("INSERT INTO Item VALUES (1, 'Apple', 100), (2, 'Banana', NULL), (1, 'Apple', 100), (3, NULL, 200)").await;

    g.named_test(
        "DISTINCT single column",
        "SELECT DISTINCT name FROM Item ORDER BY name",
        Ok(select!(name; Str; "Apple".to_owned(); "Banana".to_owned())),
    )
    .await;

    g.named_test(
        "DISTINCT multiple columns",
        "SELECT DISTINCT id, name FROM Item ORDER BY id",
        Ok(select!(
            id | name;
            I64 | Str;
            1 "Apple".to_owned();
            2 "Banana".to_owned();
            3 "".to_owned()
        )),
    )
    .await;

    g.named_test(
        "DISTINCT with NULL values",
        "SELECT DISTINCT price FROM Item ORDER BY price",
        Ok(select!(price; I64; 100; 200)),
    )
    .await;
});
