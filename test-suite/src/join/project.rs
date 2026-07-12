use super::*;

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
