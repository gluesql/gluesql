use crate::*;

test_case!(nullable, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)"#
    );
    run!("INSERT INTO Test (id, num, name) VALUES (NULL, 2, \"Hello\")");
    run!("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    run!("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")");

    use Value::*;

    let test_cases = vec![
        (
            "SELECT id, num, name FROM Test",
            select!(
                id      | num | name
                OptI64  | I64 | Str;
                None      2     "Hello".to_owned();
                Some(1)   9     "World".to_owned();
                Some(3)   4     "Great".to_owned()
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = NULL AND name = \'Hello\'",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id IS NULL",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id IS NOT NULL",
            select!(
                id      | num
                OptI64  | I64;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id + 1 IS NULL",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id + 1 IS NOT NULL",
            select!(
                id      | num
                OptI64  | I64;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 100 IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE 100 IS NOT NULL",
            select!(
                id      | num
                OptI64  | I64;
                None      2;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 8 + 3 IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE 8 + 3 IS NOT NULL",
            select!(
                id      | num
                OptI64  | I64;
                None      2;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE NULL IS NULL",
            select!(
                id      | num
                OptI64  | I64;
                None      2;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE NULL IS NOT NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE \"NULL\" IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE \"NULL\" IS NOT NULL",
            select!(
                id      | num
                OptI64  | I64;
                None      2;
                Some(1)   9;
                Some(3)   4
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = NULL + 1;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = 1 + NULL;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = NULL - 1;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = 1 - NULL;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = NULL * 1;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = 1 * NULL;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = NULL / 1;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id = 1 / NULL;",
            select!(
                id     | num
                OptI64 | I64;
                None     2
            ),
        ),
        (
            "SELECT id + 1, 1 + id, id - 1, 1 - id, id * 1, 1 * id, id / 1, 1 / id FROM Test WHERE id = NULL;",
            select!(
                "id + 1" | "1 + id" | "id - 1" | "1 - id" | "id * 1" | "1 * id" | "id / 1" | "1 / id";
                OptI64   | OptI64   | OptI64   | OptI64   | OptI64   | OptI64   | OptI64   | OptI64;
                None       None       None       None       None       None       None       None
            ),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }

    run!("UPDATE Test SET id = 2");

    let test_cases = vec![
        (
            "SELECT id FROM Test",
            Ok(select!(
                id
                OptI64;
                Some(2);
                Some(2);
                Some(2)
            )),
        ),
        (
            "SELECT id, num FROM Test",
            Ok(select!(
                id      | num
                OptI64  | I64;
                Some(2) 2;
                Some(2) 9;
                Some(2) 4
            )),
        ),
        (
            "INSERT INTO Test VALUES (1, NULL)",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});

test_case!(nullable_text, async move {
    run!(
        "
        CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        );
    "
    );

    let insert_sqls = [
        "INSERT INTO Foo (id, name) VALUES (1, \"Hello\")",
        "INSERT INTO Foo (id, name) VALUES (2, Null)",
    ];

    for insert_sql in insert_sqls.iter() {
        run!(insert_sql);
    }
});
