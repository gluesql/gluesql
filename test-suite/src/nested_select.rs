use {
    crate::*,
    gluesql_core::{executor::Payload, prelude::Value},
};

test_case!(nested_select, async move {
    let create_sqls: [&str; 2] = [
        "
        CREATE TABLE Player (
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

    for sql in create_sqls {
        run!(sql);
    }

    let insert_sqls = [
        "
        INSERT INTO Player (id, name) VALUES
            (1, \"Taehoon\"),
            (2,    \"Mike\"),
            (3,   \"Jorno\"),
            (4,   \"Berry\"),
            (5,    \"Hwan\");
        ",
        "
        INSERT INTO Request (id, quantity, user_id) VALUES
            (101, 1, 1),
            (102, 4, 2),
            (103, 9, 3),
            (104, 2, 3),
            (105, 1, 3),
            (106, 5, 1),
            (107, 2, 1),
            (108, 1, 5),
            (109, 1, 5),
            (110, 3, 3),
            (111, 4, 2),
            (112, 8, 1),
            (113, 7, 1),
            (114, 1, 1),
            (115, 2, 1);
        ",
    ];

    for insert_sql in insert_sqls {
        run!(insert_sql);
    }

    let select_sqls = [
        (6, "SELECT * FROM Request WHERE quantity IN (5, 1);"),
        (9, "SELECT * FROM Request WHERE quantity NOT IN (5, 1);"),
        (
            4,
            "SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE id = 3);",
        ),
        (
            4,
            "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request);",
        ),
        (
            4,
            "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id = Player.id);",
        ),
        (4, "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (Player.id));"),
        (2, "SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9));"),
        (9, "SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE name IN (\"Taehoon\", \"Hwan\"));"),
    ];
    for (num, sql) in select_sqls {
        count!(num, sql);
    }

    let test_cases = [
        (
            "SELECT * FROM Player WHERE id = (SELECT id FROM Player WHERE id = 9)",
            Ok(Payload::Select {
                labels: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![],
            }),
        ),
        (
            "SELECT (SELECT N FROM SERIES(3) WHERE N = 4) N",
            Ok(select_with_null!(
                N;
                Value::Null
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
