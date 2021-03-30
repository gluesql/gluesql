use crate::*;

test_case!(async move {
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

    for sql in create_sqls.iter() {
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
        INSERT INTO Item (id, quantity, player_id) VALUES
            (101, 1, 1),
            (102, 4, 2),
            (103, 9, 4);
        ",
    ];

    for insert_sql in insert_sqls.iter() {
        run!(insert_sql);
    }

    use Value::{Null, Str, I64};

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
    test!(Ok(expected), sql);

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
    test!(Ok(expected), sql);

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
    test!(Ok(expected), sql);

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
    test!(Ok(expected), sql);
});
