use {
    crate::*,
    gluesql_core::prelude::Value::{self, *},
    serde_json::json,
};

test_case!(basic, {
    let g = get_tester!();

    g.run("CREATE TABLE Player");
    g.run(
        format!(
            "INSERT INTO Player VALUES ('{}'), ('{}');",
            json!({ "id": 1001, "name": "Beam", "flag": 1 }),
            json!({ "id": 1002, "name": "Seo" }),
        )
        .as_str(),
    );

    g.run("CREATE TABLE Item");
    g.run(
        format!(
            "INSERT INTO Item VALUES ('{}'), ('{}');",
            json!({
                "id": 100,
                "name": "Test 001",
                "dex": 324,
                "rare": false,
                "obj": {
                    "cost": 3000
                }
            }),
            json!({
                "id": 200
            })
        )
        .as_str(),
    );

    g.test(
        "SELECT name, dex, rare FROM Item WHERE id = 100",
        Ok(select!(
            name                  | dex | rare
            Str                   | I64 | Bool;
            "Test 001".to_owned()   324   false
        )),
    );

    g.test(
        "SELECT name, dex, rare FROM Item",
        Ok(select_with_null!(
            name                       | dex      | rare;
            Str("Test 001".to_owned())   I64(324)   Bool(false);
            Null                         Null       Null
        )),
    );

    g.test(
        "SELECT * FROM Item",
        Ok(select_map![
            json!({
                "id": 100,
                "name": "Test 001",
                "dex": 324,
                "rare": false,
                "obj": {
                    "cost": 3000
                }
            }),
            json!({
                "id": 200
            })
        ]),
    );

    g.run("DELETE FROM Item WHERE id > 100");
    g.run(
        "
        UPDATE Item
        SET
            id = id + 1,
            rare = NOT rare
    ",
    );
    g.test(
        "SELECT id, name, dex, rare FROM Item",
        Ok(select!(
            id  | name                  | dex | rare
            I64 | Str                   | I64 | Bool;
            101   "Test 001".to_owned()   324   true
        )),
    );

    // add new field to existing row
    g.run("UPDATE Item SET new_field = 'Hello'");
    g.test(
        r"SELECT new_field, obj['cost'] AS cost FROM Item",
        Ok(select!(
            new_field          | cost
            Str                | I64;
            "Hello".to_owned()   3000
        )),
    );

    // join
    g.test(
        "SELECT
            Player.id AS player_id,
            Player.name AS player_name,
            Item.obj['cost'] AS item_cost
        FROM Item
        JOIN Player
        WHERE flag IS NOT NULL;
        ",
        Ok(select!(
            player_id | player_name       | item_cost
            I64       | Str               | I64;
            1001        "Beam".to_owned()   3000
        )),
    );
});
