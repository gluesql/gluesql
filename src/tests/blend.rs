use crate::*;

pub fn blend(mut tester: impl tests::Tester) {
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

    create_sqls.iter().for_each(|sql| tester.run_and_print(sql));

    let delete_sqls = ["DELETE FROM BlendUser", "DELETE FROM BlendItem"];

    delete_sqls.iter().for_each(|sql| tester.run_and_print(sql));

    let insert_sqls = [
        "INSERT INTO BlendUser (id, name) VALUES (1, \"Taehoon\")",
        "INSERT INTO BlendUser (id, name) VALUES (2, \"Mike\")",
        "INSERT INTO BlendUser (id, name) VALUES (3, \"Jorno\")",
        "INSERT INTO BlendItem (id, player_id, quantity) VALUES (101, 1, 1);",
        "INSERT INTO BlendItem (id, player_id, quantity) VALUES (102, 2, 4);",
        "INSERT INTO BlendItem (id, player_id, quantity) VALUES (103, 2, 9);",
        "INSERT INTO BlendItem (id, player_id, quantity) VALUES (104, 3, 2);",
        "INSERT INTO BlendItem (id, player_id, quantity) VALUES (105, 3, 1);",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }

    use Value::*;

    let mut run = |sql| tester.run(sql).expect("select");

    let test_cases = vec![
        ("SELECT 1 FROM BlendUser", select!(I64; 1; 1; 1)),
        (
            "SELECT id, name FROM BlendUser",
            select!(
                I64 Str;
                1   "Taehoon".to_owned();
                2   "Mike".to_owned();
                3   "Jorno".to_owned()
            ),
        ),
        (
            "SELECT player_id, quantity FROM BlendItem",
            select!(I64 I64; 1 1; 2 4; 2 9; 3 2; 3 1),
        ),
        (
            "SELECT player_id, player_id FROM BlendItem",
            select!(I64 I64; 1 1; 2 2; 2 2; 3 3; 3 3),
        ),
        (
            "
            SELECT u.id, i.id, player_id
            FROM BlendUser u
            JOIN BlendItem i ON u.id = 1 AND u.id = i.player_id
            ",
            select!(I64 I64 I64; 1 101 1),
        ),
        (
            "
            SELECT i.*, u.name
            FROM BlendUser u
            JOIN BlendItem i ON u.id = 2 AND u.id = i.player_id
            ",
            select!(
                I64 I64 I64 Str;
                102 2   4   "Mike".to_owned();
                103 2   9   "Mike".to_owned()
            ),
        ),
        (
            "
            SELECT u.*, i.*
            FROM BlendUser u
            JOIN BlendItem i ON u.id = i.player_id
            ",
            select!(
                I64 Str                  I64 I64 I64;
                1   "Taehoon".to_owned() 101 1   1;
                2   "Mike".to_owned()    102 2   4;
                2   "Mike".to_owned()    103 2   9;
                3   "Jorno".to_owned()   104 3   2;
                3   "Jorno".to_owned()   105 3   1
            ),
        ),
        (
            "SELECT id as Ident, name FROM BlendUser",
            select!(
                I64 Str;
                1   "Taehoon".to_owned();
                2   "Mike".to_owned();
                3   "Jorno".to_owned()
            ),
        ),
        (
            "SELECT 2+id+2*100-1 as Ident, name FROM BlendUser",
            select!(
                I64 Str;
                202   "Taehoon".to_owned();
                203   "Mike".to_owned();
                204   "Jorno".to_owned()
            ),
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(sql, expected)| assert_eq!(expected, run(sql)));

    let error_cases = vec![
        (
            BlendError::FieldDefinitionNotSupported,
            "SELECT (1 * 2) as a FROM BlendItem;",
        ),
        (
            BlendError::ColumnNotFound("a".to_owned()),
            "SELECT a FROM BlendUser",
        ),
        (
            BlendError::TableNotFound("Whatever".to_owned()),
            "SELECT Whatever.* FROM BlendUser",
        ),
    ];

    error_cases
        .into_iter()
        .for_each(|(error, sql)| tester.test_error(sql, error.into()));
}
