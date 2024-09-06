use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, FetchError},
        prelude::Value::*,
    },
};

test_case!(project, {
    let g = get_tester!();

    let create_sqls: [&str; 2] = [
        "
        CREATE TABLE ProjectUser (
            id INTEGER,
            name TEXT
        );
    ",
        "
        CREATE TABLE ProjectItem (
            id INTEGER,
            player_id INTEGER,
            quantity INTEGER
        );
    ",
    ];

    for sql in create_sqls {
        g.run(sql).await;
    }

    let delete_sqls = ["DELETE FROM ProjectUser", "DELETE FROM ProjectItem"];

    for sql in delete_sqls {
        g.run(sql).await;
    }

    let insert_sqls = [
        "
        INSERT INTO ProjectUser (id, name) VALUES
            (1, 'Taehoon'),
            (2,    'Mike'),
            (3,   'Jorno');
        ",
        "
        INSERT INTO ProjectItem (id, player_id, quantity) VALUES
            (101, 1, 1),
            (102, 2, 4),
            (103, 2, 9),
            (104, 3, 2),
            (105, 3, 1);
        ",
    ];

    for insert_sql in insert_sqls {
        g.run(insert_sql).await;
    }

    let test_cases = [
        ("SELECT 1 FROM ProjectUser", select!("1"; I64; 1; 1; 1)),
        (
            "SELECT id, name FROM ProjectUser",
            select!(
                id  | name
                I64 | Str;
                1     "Taehoon".to_owned();
                2     "Mike".to_owned();
                3     "Jorno".to_owned()
            ),
        ),
        (
            "SELECT player_id, quantity FROM ProjectItem",
            select!(player_id | quantity; I64 | I64; 1 1; 2 4; 2 9; 3 2; 3 1),
        ),
        (
            "SELECT player_id, player_id FROM ProjectItem",
            select!(player_id | player_id; I64 | I64; 1 1; 2 2; 2 2; 3 3; 3 3),
        ),
        (
            "
            SELECT u.id, i.id, player_id
            FROM ProjectUser u
            JOIN ProjectItem i ON u.id = 1 AND u.id = i.player_id
            ",
            select!(id | id | player_id; I64 | I64 | I64; 1 101 1),
        ),
        (
            "
            SELECT i.*, u.name
            FROM ProjectUser u
            JOIN ProjectItem i ON u.id = 2 AND u.id = i.player_id
            ",
            select!(
                id  | player_id | quantity | name
                I64 | I64       | I64      | Str;
                102   2           4          "Mike".to_owned();
                103   2           9          "Mike".to_owned()
            ),
        ),
        (
            "
            SELECT u.*, i.*
            FROM ProjectUser u
            JOIN ProjectItem i ON u.id = i.player_id
            ",
            select!(
                id  | name                 | id  | player_id | quantity
                I64 | Str                  | I64 | I64       | I64;
                1     "Taehoon".to_owned()   101   1           1;
                2     "Mike".to_owned()      102   2           4;
                2     "Mike".to_owned()      103   2           9;
                3     "Jorno".to_owned()     104   3           2;
                3     "Jorno".to_owned()     105   3           1
            ),
        ),
        (
            "SELECT id as Ident, name FROM ProjectUser",
            select!(
                Ident | name
                I64   | Str;
                1       "Taehoon".to_owned();
                2       "Mike".to_owned();
                3       "Jorno".to_owned()
            ),
        ),
        (
            "SELECT (1 + 2) as foo, 2+id+2*100-1 as Ident, name FROM ProjectUser",
            select!(
                foo | Ident | name
                I64 | I64   | Str;
                3     202     "Taehoon".to_owned();
                3     203     "Mike".to_owned();
                3     204     "Jorno".to_owned()
            ),
        ),
        (
            "
            SELECT id FROM ProjectUser
            WHERE id IN (
                SELECT ProjectUser.id FROM ProjectItem
                WHERE quantity > 5 AND ProjectUser.id = player_id
            );",
            select!(id; I64; 2),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }

    let error_cases = [
        (
            "SELECT Whatever.* FROM ProjectUser",
            FetchError::TableAliasNotFound("Whatever".to_owned()).into(),
        ),
        (
            "SELECT noname FROM ProjectUser",
            EvaluateError::IdentifierNotFound("noname".to_owned()).into(),
        ),
        (
            "SELECT (SELECT id FROM ProjectItem) as id FROM ProjectItem",
            EvaluateError::MoreThanOneRowReturned.into(),
        ),
        (
            "SELECT (SELECT 1,2)",
            EvaluateError::MoreThanOneColumnReturned.into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error)).await;
    }
});
