use {
    crate::*,
    gluesql_core::{
        error::TranslateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(basic, {
    let g = get_tester!();

    let queries = [
        r#"
        CREATE TABLE Test (
            id INTEGER,
            num INTEGER,
            name TEXT
        )
        "#,
        r#"
        CREATE TABLE TestA (
            id INTEGER,
            num INTEGER,
            name TEXT
        )
        "#,
        "CREATE TABLE EmptyTest",
        "INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')",
        "INSERT INTO Test (id, num, name) VALUES (1, 9, 'World')",
        "INSERT INTO Test (id, num, name) VALUES (3, 4, 'Great'), (4, 7, 'Job')",
        "INSERT INTO TestA (id, num, name) SELECT id, num, name FROM Test",
        "CREATE TABLE TestB (id INTEGER);",
        "INSERT INTO TestB (id) SELECT id FROM Test",
    ];

    for query in queries {
        g.run(query).await;
    }

    g.named_test(
        "select all from table",
        "SELECT * FROM TestB",
        Ok(select!(
            id I64;
            1; 1; 3; 4
        )),
    )
    .await;

    g.test(
        "SELECT id, num, name FROM TestA",
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            1     9     "World".to_owned();
            3     4     "Great".to_owned();
            4     7     "Job".to_owned()
        )),
    )
    .await;

    g.test("SELECT * FROM EmptyTest", Ok(Payload::SelectMap(vec![])))
        .await;

    g.test(
        "SELECT * FROM (SELECT * FROM EmptyTest) AS Empty",
        Ok(Payload::SelectMap(vec![])),
    )
    .await;

    g.count("SELECT * FROM Test", 4).await;

    g.run("UPDATE Test SET id = 2").await;

    g.test(
        "SELECT id FROM Test",
        Ok(select!(
            id I64;
            2; 2; 2; 2
        )),
    )
    .await;

    g.test(
        "SELECT id, num FROM Test",
        Ok(select!(
            id  | num;
            I64 | I64;
            2     2;
            2     9;
            2     4;
            2     7
        )),
    )
    .await;

    g.test(
        "SELECT id FROM FOO.Test",
        Err(TranslateError::CompoundObjectNotSupported("FOO.Test".to_owned()).into()),
    )
    .await;
});
