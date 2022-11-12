use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(nullable, async move {
    run!(
        "
CREATE TABLE Test (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)"
    );
    run!(
        "
        INSERT INTO Test (id, num, name) VALUES
            (NULL, 2, 'Hello'),
            (   1, 9, 'World'),
            (   3, 4, 'Great');
    "
    );

    let test_cases = [
        (
            "SELECT id, num, name FROM Test",
            select_with_null!(
                id     | num    | name;
                Null     I64(2)   Str("Hello".to_owned());
                I64(1)   I64(9)   Str("World".to_owned());
                I64(3)   I64(4)   Str("Great".to_owned())
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id IS NULL AND name = 'Hello'",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id IS NULL",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id IS NOT NULL",
            select_with_null!(
                id     | num;
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id + 1 IS NULL",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id + 1 IS NOT NULL",
            select_with_null!(
                id     | num;
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 100 IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE 100 IS NOT NULL",
            select_with_null!(
                id     | num;
                Null     I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 8 + 3 IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE 8 + 3 IS NOT NULL",
            select_with_null!(
                id     | num;
                Null     I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE NULL IS NULL",
            select_with_null!(
                id     | num;
                Null     I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE NULL IS NOT NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE (NULL + id) IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE (NULL + NULL) IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 'NULL' IS NULL",
            select!(id | num),
        ),
        (
            "SELECT id, num FROM Test WHERE 'NULL' IS NOT NULL",
            select_with_null!(
                id     | num;
                Null     I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE (NULL + id) IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2);
                I64(1)   I64(9);
                I64(3)   I64(4)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id + 1 IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 1 + id IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id - 1 IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 1 - id IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id * 1 IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 1 * id IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE id / 1 IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id, num FROM Test WHERE 1 / id IS NULL;",
            select_with_null!(
                id   | num;
                Null   I64(2)
            ),
        ),
        (
            "SELECT id + 1, 1 + id, id - 1, 1 - id, id * 1, 1 * id, id / 1, 1 / id FROM Test WHERE id IS NULL;",
            select_with_null!(
                "id + 1" | "1 + id" | "id - 1" | "1 - id" | "id * 1" | "1 * id" | "id / 1" | "1 / id";
                Null       Null       Null       Null       Null       Null       Null       Null
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }

    run!("UPDATE Test SET id = 2");

    let test_cases = [
        ("SELECT id FROM Test", Ok(select!(id I64; 2; 2; 2))),
        (
            "SELECT id, num FROM Test",
            Ok(select!(
                id  | num
                I64 | I64;
                2     2;
                2     9;
                2     4
            )),
        ),
        (
            "INSERT INTO Test VALUES (1, NULL, 'ok')",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
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

    run!("INSERT INTO Foo (id, name) VALUES (1, 'Hello'), (2, Null);");
});

test_case!(nullable_implicit_insert, async move {
    run!(
        "
        CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        );
    "
    );

    run!("INSERT INTO Foo (id) VALUES (1)");
    test!(
        "SELECT id, name FROM Foo",
        Ok(select_with_null!(
            id   | name;
            I64(1)  Null
        ))
    );
});
