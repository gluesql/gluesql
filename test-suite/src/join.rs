use {
    crate::*,
    Value::*,
    gluesql_core::{
        error::{AlterError, PlanError, TranslateError},
        prelude::*,
    },
};

test_case!(join, {
    let g = get_tester!();

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
            player_id INTEGER
        );
    ",
    ];

    for sql in create_sqls {
        g.run(sql);
    }

    let delete_sqls = ["DELETE FROM Player", "DELETE FROM Item"];

    for sql in delete_sqls {
        g.run(sql);
    }

    let insert_sqls = [
        "
        INSERT INTO Player (id, name) VALUES
            (1, 'Taehoon'),
            (2,    'Mike'),
            (3,   'Jorno'),
            (4,   'Berry'),
            (5,    'Hwan');
        ",
        "
        INSERT INTO Item (id, quantity, player_id) VALUES
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
        g.run(insert_sql);
    }

    let select_sqls = [
        (75, "SELECT * FROM Item JOIN Player"),
        (
            15,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id;",
        ),
        (
            5,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;",
        ),
        (
            7,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;",
        ),
        (
            7,
            "SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;",
        ),
        (
            7,
            "SELECT * FROM Item
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
            WHERE Player.id = 1;",
        ),
        (
            6,
            "SELECT * FROM Item
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
            WHERE Player.id = 1;",
        ),
        (
            5,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1;",
        ),
        (
            5,
            "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1;",
        ),
        (
            15,
            "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1;",
        ),
        (
            15,
            "SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1;",
        ),
        (
            15,
            "SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1;",
        ),
        (
            7,
            "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1;",
        ),
        (
            7,
            "SELECT * FROM Item i INNER JOIN Player p ON p.id = i.player_id AND p.id = 1;",
        ),
        (
            5,
            "SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND i.quantity = 1;",
        ),
        (
            0,
            "SELECT * FROM Player
            INNER JOIN Item ON 1 = 2
            INNER JOIN Item i2 ON 1 = 2
        ",
        ),
        (
            7,
            "SELECT * FROM Item
            LEFT JOIN Player ON Player.id = Item.player_id
            WHERE Player.id = (SELECT id FROM Player LIMIT 1 OFFSET 0);",
        ),
        (
            0,
            "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id = (SELECT id FROM Item i2 WHERE i2.id = i1.id)",
        ),
        (
            0,
            "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id =
                (SELECT i2.id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE
                     i2.id = i1.id AND
                     i3.id = i2.id AND
                     i1.id = i3.id);",
        ),
        (
            4,
            "SELECT * FROM Item i1
            LEFT JOIN Player ON Player.id = i1.player_id
            WHERE Player.id IN
                (SELECT i2.player_id FROM Item i2
                 JOIN Item i3 ON i3.id = i2.id
                 WHERE Player.name = 'Jorno');",
        ),
        // cartesian product tests
        (
            15,
            "SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id;",
        ),
        (25, "SELECT * FROM Player p1 LEFT JOIN Player p2 ON 1 = 1"),
        (
            30,
            "SELECT * FROM Item INNER JOIN Item i2 ON i2.id IN (101, 103);",
        ),
    ];

    for (num, sql) in select_sqls {
        g.count(sql, num);
    }

    for sql in delete_sqls {
        g.run(sql);
    }
});

test_case!(join_primary_key_predicate_on_joined_relation, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        ",
    );
    g.run(
        "
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            project_id INTEGER,
            done BOOLEAN NOT NULL
        );
        ",
    );
    g.run(
        "
        INSERT INTO projects (id, name) VALUES
            (1, 'P1'),
            (2, 'P2');
        ",
    );
    g.run(
        "
        INSERT INTO tasks VALUES
            (1, 1, FALSE),
            (2, 1, FALSE),
            (3, 2, FALSE),
            (4, 2, FALSE);
        ",
    );

    g.test(
        "
        SELECT t.id
        FROM tasks t
        JOIN projects p ON p.id = t.project_id
        WHERE p.id = 1
          AND t.done = FALSE
        ORDER BY t.id;
        ",
        Ok(select!(id I64; 1; 2)),
    );

    g.run(
        "
        CREATE TABLE tasks_with_different_pk (
            task_id INTEGER PRIMARY KEY,
            project_id INTEGER,
            done BOOLEAN NOT NULL
        );
        ",
    );
    g.run(
        "
        INSERT INTO tasks_with_different_pk VALUES
            (101, 1, FALSE),
            (102, 1, FALSE),
            (103, 2, FALSE),
            (104, 2, FALSE);
        ",
    );

    g.test(
        "
        SELECT t.task_id
        FROM tasks_with_different_pk t
        JOIN projects p ON p.id = t.project_id
        WHERE p.id = 1
          AND t.done = FALSE
        ORDER BY t.task_id;
        ",
        Ok(select!(task_id I64; 101; 102)),
    );

    g.run(
        "
        CREATE TABLE tasks_without_pk (
            task_id INTEGER,
            project_id INTEGER,
            done BOOLEAN NOT NULL
        );
        ",
    );
    g.run(
        "
        INSERT INTO tasks_without_pk VALUES
            (201, 1, FALSE),
            (202, 1, FALSE),
            (203, 2, FALSE),
            (204, 2, FALSE);
        ",
    );

    g.test(
        "
        SELECT t.task_id
        FROM tasks_without_pk t
        JOIN projects p ON p.id = t.project_id
        WHERE p.id = 1
          AND t.done = FALSE
        ORDER BY t.task_id;
        ",
        Ok(select!(task_id I64; 201; 202)),
    );

    g.test(
        "
        SELECT t.task_id
        FROM tasks_with_different_pk t
        JOIN projects p ON p.id = t.project_id
        WHERE id = 1
          AND t.done = FALSE
        ORDER BY t.task_id;
        ",
        Ok(select!(task_id I64; 101; 102)),
    );
});

test_case!(join_primary_key_predicate_with_multiple_joins, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE tasks_for_tags (
            task_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL
        );
        ",
    );
    g.run(
        "
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        ",
    );
    g.run(
        "
        CREATE TABLE task_tags (
            task_id INTEGER,
            tag_id INTEGER
        );
        ",
    );
    g.run(
        "
        INSERT INTO tasks_for_tags VALUES
            (101, 'Task 1'),
            (102, 'Task 2'),
            (103, 'Task 3');
        ",
    );
    g.run(
        "
        INSERT INTO tags VALUES
            (1, 'bug'),
            (2, 'feature');
        ",
    );
    g.run(
        "
        INSERT INTO task_tags VALUES
            (101, 1),
            (102, 1),
            (103, 2);
        ",
    );

    g.test(
        "
        SELECT t.task_id
        FROM tasks_for_tags t
        JOIN task_tags tt ON tt.task_id = t.task_id
        JOIN tags tag ON tag.id = tt.tag_id
        WHERE tag.id = 1
        ORDER BY t.task_id;
        ",
        Ok(select!(task_id I64; 101; 102)),
    );

    g.test(
        "
        SELECT t.task_id
        FROM tasks_for_tags t
        JOIN task_tags tt ON tt.task_id = t.task_id
        JOIN tags tag ON tag.id = tt.tag_id
        WHERE id = 1
        ORDER BY t.task_id;
        ",
        Ok(select!(task_id I64; 101; 102)),
    );
});

test_case!(project, {
    let g = get_tester!();

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
            player_id INTEGER
        );
    ",
    ];

    for sql in create_sqls {
        g.run(sql);
    }

    let insert_sqls = [
        "
        INSERT INTO Player (id, name) VALUES
            (1, 'Taehoon'),
            (2,    'Mike'),
            (3,   'Jorno'),
            (4,   'Berry'),
            (5,    'Hwan');
        ",
        "
        INSERT INTO Item (id, quantity, player_id) VALUES
            (101, 1, 1),
            (102, 4, 2),
            (103, 9, 4);
        ",
    ];

    for insert_sql in insert_sqls {
        g.run(insert_sql);
    }

    let sql = "
        SELECT p.id, i.id
        FROM Player p
        LEFT JOIN Item i
        ON p.id = i.player_id
    ";
    let expected = select_with_null!(
        id     | id;
        I64(1)   I64(101);
        I64(2)   I64(102);
        I64(3)   Null;
        I64(4)   I64(103);
        I64(5)   Null
    );
    g.test(sql, Ok(expected));

    let sql = "
        SELECT p.id, player_id
        FROM Player p
        LEFT JOIN Item
        ON p.id = player_id
    ";
    let expected = select_with_null!(
        id     | player_id;
        I64(1)   I64(1);
        I64(2)   I64(2);
        I64(3)   Null;
        I64(4)   I64(4);
        I64(5)   Null
    );
    g.test(sql, Ok(expected));

    let sql = "
        SELECT Item.*
        FROM Player p
        LEFT JOIN Item
        ON p.id = player_id
    ";
    let expected = select_with_null!(
        id       | quantity | player_id;
        I64(101)   I64(1)     I64(1);
        I64(102)   I64(4)     I64(2);
        Null       Null       Null;
        I64(103)   I64(9)     I64(4);
        Null       Null       Null
    );
    g.test(sql, Ok(expected));

    let sql = "
        SELECT *
        FROM Player p
        LEFT JOIN Item
        ON p.id = player_id
    ";
    let expected = select_with_null!(
        id     | name                      | id       | quantity | player_id;
        I64(1)   Str("Taehoon".to_owned())   I64(101)   I64(1)     I64(1);
        I64(2)   Str("Mike".to_owned())      I64(102)   I64(4)     I64(2);
        I64(3)   Str("Jorno".to_owned())     Null       Null       Null;
        I64(4)   Str("Berry".to_owned())     I64(103)   I64(9)     I64(4);
        I64(5)   Str("Hwan".to_owned())      Null       Null       Null
    );
    g.test(sql, Ok(expected));

    // To test `PlanError` while using `JOIN`
    g.run("CREATE TABLE Users (id INTEGER, name TEXT);");
    g.run("INSERT INTO Users (id, name) VALUES (1, 'Harry');");
    g.run("CREATE TABLE Testers (id INTEGER, nickname TEXT);");
    g.run("INSERT INTO Testers (id, nickname) VALUES (1, 'Ron');");

    let error_cases = [
        (
            "SELECT * FROM TableA JOIN TableA USING (id);",
            TranslateError::UnsupportedJoinConstraint("USING".to_owned()).into(),
        ),
        (
            "SELECT * FROM TableA CROSS JOIN TableA as A;",
            TranslateError::UnsupportedJoinOperator("CrossJoin".to_owned()).into(),
        ),
        (
            "SELECT id FROM Users JOIN Testers ON Users.id = Testers.id;",
            PlanError::ColumnReferenceAmbiguous("id".to_owned()).into(),
        ),
        (
            // Ambiguous column should return error even with identical table join
            "SELECT id FROM Users A JOIN Users B on A.id = B.id",
            PlanError::ColumnReferenceAmbiguous("id".to_owned()).into(),
        ),
        (
            "INSERT INTO Users SELECT id FROM Users A JOIN Users B on A.id = B.id",
            PlanError::ColumnReferenceAmbiguous("id".to_owned()).into(),
        ),
        (
            "CREATE TABLE Ids AS SELECT id FROM Users A JOIN Users B on A.id = B.id",
            PlanError::ColumnReferenceAmbiguous("id".to_owned()).into(),
        ),
        (
            "CREATE TABLE JoinedUsers AS SELECT * FROM Users JOIN Testers ON Users.id = Testers.id",
            AlterError::DuplicateColumnName("id".to_owned()).into(),
        ),
        (
            "SELECT * FROM ProjectUser, ProjectItem",
            TranslateError::TooManyTables.into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error));
    }
});
