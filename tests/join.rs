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
        (
            15,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id;",
        ),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;"),
        (7, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;"),
        (7, "SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;"),
        /*
        (7, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            LEFT JOIN Player p4 ON p4.id = Item.player_id
            LEFT JOIN Player p5 ON p5.id = Item.player_id
            LEFT JOIN Player p6 ON p6.id = Item.player_id
            LEFT JOIN Player p7 ON p7.id = Item.player_id
            LEFT JOIN Player p8 ON p8.id = Item.player_id
            LEFT JOIN Player p9 ON p9.id = Item.player_id
            WHERE Player.id = 1;"),
        (6, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            LEFT JOIN Player p1 ON p1.id = Item.player_id
            LEFT JOIN Player p2 ON p2.id = Item.player_id
            LEFT JOIN Player p3 ON p3.id = Item.player_id
            LEFT JOIN Player p4 ON p4.id = Item.player_id
            LEFT JOIN Player p5 ON p5.id = Item.player_id
            LEFT JOIN Player p6 ON p6.id = Item.player_id
            LEFT JOIN Player p7 ON p7.id = Item.player_id
            LEFT JOIN Player p8 ON p8.id = Item.player_id
            INNER JOIN Player p9 ON p9.id = Item.player_id AND Item.id > 101
            WHERE Player.id = 1;"),
        (5, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1;"),
        (5, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (15, "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1;"),
        (15, "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1;"),
        (7, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (7, "SELECT * FROM Item i INNER JOIN Player p ON p.id = i.player_id AND p.id = 1;"),
        (5, "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND Item.quantity = 1;"),
        (0, "SELECT * FROM Player
            INNER JOIN Item ON 1 = 2
            INNER JOIN Item i2 ON 1 = 2
        "),
        (7, "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            WHERE Player.id = (SELECT id FROM Player LIMIT 1 OFFSET 0);"),
        (0, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id = (SELECT id FROM Item i2 WHERE i2.id = i1.id)"),
        (0, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id =
                (SELECT i2.id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE
                     i2.id = i1.id AND
                     i3.id = i2.id AND
                     i1.id = i3.id);"),
        (4, "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id IN
                (SELECT i2.player_id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE Player.name = \"Jorno\");"),
        // cartesian product tests
        (15, "SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id;"),
        (25, "SELECT * FROM Player p1 LEFT JOIN Player p2 ON 1 = 1"),
        (30, "SELECT * FROM Item INNER JOIN Item i2 ON i2.id IN (101, 103);"),
        */
    ];

    select_sqls
        .iter()
        .for_each(|(num, sql)| helper.test_rows(sql, *num));

    delete_sqls.iter().for_each(|sql| helper.run_and_print(sql));
}
