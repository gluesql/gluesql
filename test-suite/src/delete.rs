use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(delete, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Foo (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            flag BOOLEAN
        );
    ",
    )
    .await;

    g.run(
        "
        INSERT INTO Foo VALUES
            (1, 100, TRUE),
            (2, 300, FALSE),
            (3, 700, TRUE);
    ",
    )
    .await;

    g.test(
        "SELECT * FROM Foo",
        Ok(select!(
            id  | score | flag
            I64 | I64   | Bool;
            1     100     true;
            2     300     false;
            3     700     true
        )),
    )
    .await;

    g.named_test(
        "delete using WHERE",
        "DELETE FROM Foo WHERE flag = FALSE",
        Ok(Payload::Delete(1)),
    )
    .await;

    g.test(
        "SELECT * FROM Foo",
        Ok(select!(
            id  | score | flag
            I64 | I64   | Bool;
            1     100     true;
            3     700     true
        )),
    )
    .await;

    g.named_test("delete all", "DELETE FROM Foo;", Ok(Payload::Delete(2)))
        .await;

    g.test(
        "SELECT * FROM Foo",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "score".to_owned(), "flag".to_owned()],
            rows: vec![],
        }),
    )
    .await;
});
