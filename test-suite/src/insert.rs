use {
    crate::*,
    gluesql_core::{
        error::InsertError,
        prelude::{Payload, Value::*},
    },
};

test_case!(insert, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER NULL,
    name TEXT NOT NULL
);",
    )
    .await;

    g.named_test(
        "basic insert - single item",
        "INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hi boo');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.named_test(
        "insert multiple rows",
        "
            INSERT INTO Test (id, num, name)
            VALUES
                (3, 9, 'Kitty!'),
                (2, 7, 'Monsters');
        ",
        Ok(Payload::Insert(2)),
    )
    .await;

    g.test(
        "INSERT INTO Test VALUES(17, 30, 'Sullivan');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test(
        "INSERT INTO Test (num, name) VALUES (28, 'Wazowski');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test(
        "INSERT INTO Test (name) VALUES ('The end');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test(
        "INSERT INTO Test (id, num) VALUES (1, 10);",
        Err(InsertError::LackOfRequiredColumn("name".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT * FROM Test;",
        Ok(select_with_null!(
            id     | num     | name;
            I64(1)   I64(2)    Str("Hi boo".to_owned());
            I64(3)   I64(9)    Str("Kitty!".to_owned());
            I64(2)   I64(7)    Str("Monsters".to_owned());
            I64(17)  I64(30)   Str("Sullivan".to_owned());
            I64(1)   I64(28)   Str("Wazowski".to_owned());
            I64(1)   Null      Str("The end".to_owned())
        )),
    )
    .await;

    g.run("CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0;")
        .await;

    g.named_test(
        "insert into target from source",
        "INSERT INTO Target SELECT * FROM Test;",
        Ok(Payload::Insert(6)),
    )
    .await;

    g.named_test(
        "target rows are equivalent to source rows",
        "SELECT * FROM Target;",
        Ok(select_with_null!(
            id     | num     | name;
            I64(1)   I64(2)    Str("Hi boo".to_owned());
            I64(3)   I64(9)    Str("Kitty!".to_owned());
            I64(2)   I64(7)    Str("Monsters".to_owned());
            I64(17)  I64(30)   Str("Sullivan".to_owned());
            I64(1)   I64(28)   Str("Wazowski".to_owned());
            I64(1)   Null      Str("The end".to_owned())
        )),
    )
    .await;
});
