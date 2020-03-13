mod helper;

use helper::{Helper, SledHelper};

#[test]
fn join() {
    let helper = SledHelper::new("data.db");

    let create_sqls: [&str; 2] = [
        "
        CREATE TABLE Player (
            id INTEGER,
            name TEXT
        );
    ",
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            player_id INTEGER,
        );
    ",
    ];

    create_sqls.iter().for_each(|sql| helper.run_and_print(sql));

    let delete_sqls = ["DELETE FROM Player", "DELETE FROM Item"];

    delete_sqls.iter().for_each(|sql| helper.run_and_print(sql));

    let insert_sqls = [
        "INSERT INTO Player (id, name) VALUES (1, \"Taehoon\")",
        "INSERT INTO Player (id, name) VALUES (2, \"Mike\")",
        "INSERT INTO Player (id, name) VALUES (3, \"Jorno\")",
        "INSERT INTO Player (id, name) VALUES (4, \"Berry\")",
        "INSERT INTO Player (id, name) VALUES (5, \"Hwan\")",
        "INSERT INTO Item (id, quantity, player_id) VALUES (101, 1, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (102, 4, 2);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (103, 9, 3);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (104, 2, 3);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (105, 1, 3);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (106, 5, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (107, 2, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (108, 1, 5);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (109, 1, 5);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (110, 3, 3);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (111, 4, 2);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (112, 8, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (113, 7, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (114, 1, 1);",
        "INSERT INTO Item (id, quantity, player_id) VALUES (115, 2, 1);",
    ];

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    let select_sqls = [
        (15, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id;"),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;"),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1;"),
        (5, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1;"),
        (15, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1;"),
        (4, "SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id;"),
        (7, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (7, "SELECT * FROM Item i INNER JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (5, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND Item.quantity = 1;"),
    ];

    select_sqls
        .iter()
        .for_each(|(num, sql)| helper.test_rows(sql, *num));

    delete_sqls.iter().for_each(|sql| helper.run_and_print(sql));
}
