use {crate::*, gluesql_core::prelude::Value};

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

    test!(Ok(select!(id I64; 1; 1; 3; 4)), "SELECT * FROM TestB");

    use Value::*;

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     9     "World".to_owned();
            3     4     "Great".to_owned();
            4     7     "Job".to_owned()
        )),
        "SELECT id, num, name FROM Test"
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     9     "World".to_owned();
            3     4     "Great".to_owned();
            4     7     "Job".to_owned()
        )),
        "SELECT id, num, name FROM TestA"
    );

    count!(4, "SELECT * FROM Test");

    run!("UPDATE Test SET id = 2");

    let test_cases = vec![
        (Ok(select!(id; I64; 2; 2; 2; 2)), "SELECT id FROM Test"),
        (
            Ok(select!(id | num; I64 | I64; 2 2; 2 9; 2 4; 2 7)),
            "SELECT id, num FROM Test",
        ),
        (
            // SELECT without Table
            Ok(select!(
                1   | 'a'       | true | "1 + 2" | "'a' || 'b'"
                I64 | Str       | Bool | I64     | Str;
                1     "a".into()  true   3         "ab".into()
            )),
            "SELECT 1, 'a', true, 1 + 2, 'a' || 'b'",
        ),
        (
            // SELECT without Table in scalar subquery
            Ok(select!(
                (SELECT 1)
                I64;
                1
            )),
            "SELECT (SELECT 1)",
        ),
        (
            // SELECT without Table with Column aliases
            Ok(select!(
                id  | max
                I64 | I64;
                1     9
            )),
            "SELECT 1 AS id, (SELECT MAX(num) FROM TestA) AS max",
        ),
        (
            // SELECT without Table in Drived
            Ok(select!(
                1
                I64;
                1
            )),
            "SELECT * FROM (SELECT 1) AS Drived",
        ),
    ];

    for (expected, sql) in test_cases {
        test!(expected, sql);
    }
});
