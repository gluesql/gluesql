use crate::*;

pub fn nested_select(mut tester: impl tests::Tester) {
    let create_sqls: [&str; 2] = [
        "
        CREATE TABLE User (
            id INTEGER,
            name TEXT
        );
    ",
        "
        CREATE TABLE Request (
            id INTEGER,
            quantity INTEGER,
            user_id INTEGER,
        );
    ",
    ];

    create_sqls.iter().for_each(|sql| tester.run_and_print(sql));

    let insert_sqls = [
        "INSERT INTO User (id, name) VALUES (1, \"Taehoon\")",
        "INSERT INTO User (id, name) VALUES (2, \"Mike\")",
        "INSERT INTO User (id, name) VALUES (3, \"Jorno\")",
        "INSERT INTO User (id, name) VALUES (4, \"Berry\")",
        "INSERT INTO User (id, name) VALUES (5, \"Hwan\")",
        "INSERT INTO Request (id, quantity, user_id) VALUES (101, 1, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (102, 4, 2);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (103, 9, 3);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (104, 2, 3);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (105, 1, 3);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (106, 5, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (107, 2, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (108, 1, 5);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (109, 1, 5);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (110, 3, 3);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (111, 4, 2);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (112, 8, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (113, 7, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (114, 1, 1);",
        "INSERT INTO Request (id, quantity, user_id) VALUES (115, 2, 1);",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }

    let select_sqls = [
        (6, "SELECT * FROM Request WHERE quantity IN (5, 1);"),
        (9, "SELECT * FROM Request WHERE quantity NOT IN (5, 1);"),
        (
            4,
            "SELECT * FROM Request WHERE user_id IN (SELECT id FROM User WHERE id = 3);",
        ),
        (
            4,
            "SELECT * FROM User WHERE id IN (SELECT user_id FROM Request);",
        ),
        (
            4,
            "SELECT * FROM User WHERE id IN (SELECT user_id FROM Request WHERE user_id = User.id);",
        ),
        (4, "SELECT * FROM User WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (User.id));"),
        (2, "SELECT * FROM User WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9));"),
        (9, "SELECT * FROM Request WHERE user_id IN (SELECT id FROM User WHERE name IN (\"Taehoon\", \"Hwan\"));"),
    ];

    select_sqls
        .iter()
        .for_each(|(num, sql)| tester.test_rows(sql, *num));
}
