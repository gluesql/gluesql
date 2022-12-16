use {crate::*, gluesql_core::prelude::Value::*};

test_case!(basic, async move {
    run!("CREATE TABLE Item");
    run!(
        r#"
        INSERT INTO Item VALUES ('
            {
                "id": 100,
                "name": "Test 001",
                "dex": 324,
                "rare": false,
                "obj": {
                    "cost": 3000
                }
            }
        ');
    "#
    );

    test!(
        "SELECT name, dex, rare FROM Item",
        Ok(select!(
            name                  | dex | rare
            Str                   | I64 | Bool;
            "Test 001".to_owned()   324   false
        ))
    );

    run!(
        "
        UPDATE Item
        SET
            id = id + 1,
            rare = NOT rare
    "
    );
    test!(
        "SELECT id, name, dex, rare FROM Item",
        Ok(select!(
            id  | name                  | dex | rare
            I64 | Str                   | I64 | Bool;
            101   "Test 001".to_owned()   324   true
        ))
    );

    // add new field to existing row
    run!("UPDATE Item SET new_field = 'Hello'");
    test!(
        r#"SELECT new_field, obj['cost'] AS cost FROM Item"#,
        Ok(select!(
            new_field          | cost
            Str                | I64;
            "Hello".to_owned()   3000
        ))
    );
});
