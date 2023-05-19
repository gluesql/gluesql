use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(delete, async move {
    run!(
        "
        CREATE TABLE Foo (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            flag BOOLEAN
        );
    "
    );

    run!(
        "
        INSERT INTO Foo VALUES
            (1, 100, TRUE),
            (2, 300, FALSE),
            (3, 700, TRUE);
    "
    );

    test! {
        sql: "SELECT * FROM Foo",
        expected: Ok(select!(
            id  | score | flag
            I64 | I64   | Bool;
            1     100     true;
            2     300     false;
            3     700     true
        ))
    };

    test! {
        name: "delete using WHERE",
        sql: "DELETE FROM Foo WHERE flag = FALSE",
        expected: Ok(Payload::Delete(1))
    };

    test! {
        sql: "SELECT * FROM Foo",
        expected: Ok(select!(
            id  | score | flag
            I64 | I64   | Bool;
            1     100     true;
            3     700     true
        ))
    };

    test! {
        name: "delete all",
        sql: "DELETE FROM Foo;",
        expected: Ok(Payload::Delete(2))
    };

    test! {
        sql: "SELECT * FROM Foo",
        expected: Ok(Payload::Select {
            labels: vec!["id".to_owned(), "score".to_owned(), "flag".to_owned()],
            rows: vec![],
        })
    };
});
