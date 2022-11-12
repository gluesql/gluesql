use {
    crate::*,
    gluesql_core::{
        executor::{EvaluateError, SelectError},
        prelude::Value::*,
    },
};

test_case!(blend, async move {
    let create_sqls: [&str; 2] = [
        "
        CREATE TABLE BlendUser (
            id INTEGER,
            name TEXT
        );
    ",
        "
        CREATE TABLE BlendItem (
            id INTEGER,
            player_id INTEGER,
            quantity INTEGER,
        );
    ",
    ];

    for sql in create_sqls {
        run!(sql);
    }

    let delete_sqls = ["DELETE FROM BlendUser", "DELETE FROM BlendItem"];

    for sql in delete_sqls {
        run!(sql);
    }

    let insert_sqls = [
        "
        INSERT INTO BlendUser (id, name) VALUES
            (1, 'Taehoon'),
            (2,    'Mike'),
            (3,   'Jorno');
        ",
        "
        INSERT INTO BlendItem (id, player_id, quantity) VALUES
            (101, 1, 1),
            (102, 2, 4),
            (103, 2, 9),
            (104, 3, 2),
            (105, 3, 1);
        ",
    ];

    for insert_sql in insert_sqls {
        run!(insert_sql);
    }

    let test_cases = [
        ("SELECT 1 FROM BlendUser", select!("1"; I64; 1; 1; 1)),
        (
            "SELECT id, name FROM BlendUser",
            select!(
                id  | name
                I64 | Str;
                1     "Taehoon".to_owned();
                2     "Mike".to_owned();
                3     "Jorno".to_owned()
            ),
        ),
        (
            "SELECT player_id, quantity FROM BlendItem",
            select!(player_id | quantity; I64 | I64; 1 1; 2 4; 2 9; 3 2; 3 1),
        ),
        (
            "SELECT player_id, player_id FROM BlendItem",
            select!(player_id | player_id; I64 | I64; 1 1; 2 2; 2 2; 3 3; 3 3),
        ),
        (
            "
            SELECT u.id, i.id, player_id
            FROM BlendUser u
            JOIN BlendItem i ON u.id = 1 AND u.id = i.player_id
            ",
            select!(id | id | player_id; I64 | I64 | I64; 1 101 1),
        ),
        (
            "
            SELECT i.*, u.name
            FROM BlendUser u
            JOIN BlendItem i ON u.id = 2 AND u.id = i.player_id
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
            FROM BlendUser u
            JOIN BlendItem i ON u.id = i.player_id
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
            "SELECT id as Ident, name FROM BlendUser",
            select!(
                Ident | name
                I64   | Str;
                1       "Taehoon".to_owned();
                2       "Mike".to_owned();
                3       "Jorno".to_owned()
            ),
        ),
        (
            "SELECT (1 + 2) as foo, 2+id+2*100-1 as Ident, name FROM BlendUser",
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
            SELECT id FROM BlendUser
            WHERE id IN (
                SELECT BlendUser.id FROM BlendItem
                WHERE quantity > 5 AND BlendUser.id = player_id
            );",
            select!(id; I64; 2),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }

    let error_cases = [
        (
            "SELECT Whatever.* FROM BlendUser",
            SelectError::TableAliasNotFound("Whatever".to_owned()).into(),
        ),
        (
            "SELECT * FROM BlendUser WHERE id IN (SELECT Whatever.* FROM BlendUser)",
            SelectError::BlendTableAliasNotFound("Whatever".to_owned()).into(),
        ),
        (
            "SELECT noname FROM BlendUser",
            EvaluateError::ValueNotFound("noname".to_owned()).into(),
        ),
        (
            "SELECT (SELECT id FROM BlendItem) as id FROM BlendItem",
            EvaluateError::MoreThanOneRowReturned.into(),
        ),
    ];

    for (sql, error) in error_cases {
        test!(sql, Err(error));
    }
});
