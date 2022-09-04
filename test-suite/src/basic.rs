use {crate::*, gluesql_core::prelude::Value::*};

test_case!(basic, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#
    );
    run!(
        r#"
CREATE TABLE TestA (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#
    );

    run!("INSERT INTO Test (id, num, name) VALUES (1, 2, \"Hello\")");
    run!("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    run!("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\"), (4, 7, \"Job\")");
    run!("INSERT INTO TestA (id, num, name) SELECT id, num, name FROM Test");

    run!("CREATE TABLE TestB (id INTEGER);");
    run!("INSERT INTO TestB (id) SELECT id FROM Test");

    test! (
        name: "select all from table",
        sql : "SELECT * FROM TestB",
        expected : Ok(select!(id I64; 1; 1; 3; 4))
    );

    test!(
        "SELECT id, num, name FROM TestA",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     9     "World".to_owned();
            3     4     "Great".to_owned();
            4     7     "Job".to_owned()
        ))
    );

    count!(4, "SELECT * FROM Test");

    run!("UPDATE Test SET id = 2");

    let test_cases = [
        ("SELECT id FROM Test", Ok(select!(id; I64; 2; 2; 2; 2))),
        (
            "SELECT id, num FROM Test",
            Ok(select!(id | num; I64 | I64; 2 2; 2 9; 2 4; 2 7)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
