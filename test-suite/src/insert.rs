use {
    crate::*,
    gluesql_core::{
        error::InsertError,
        prelude::{Payload, Value::*},
    },
};

test_case!(insert, async move {
    run!(
        "
CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER NULL,
    name TEXT NOT NULL,
);"
    );

    test! {
        name: "basic insert - single item",
        sql: "INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hi boo');",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        name: "insert multiple rows",
        sql: "
            INSERT INTO Test (id, num, name)
            VALUES
                (3, 9, 'Kitty!'),
                (2, 7, 'Monsters');
        ",
        expected: Ok(Payload::Insert(2))
    };

    test! {
        sql: "INSERT INTO Test VALUES(17, 30, 'Sullivan');",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: "INSERT INTO Test (num, name) VALUES (28, 'Wazowski');",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: "INSERT INTO Test (name) VALUES ('The end');",
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: "INSERT INTO Test (id, num) VALUES (1, 10);",
        expected: Err(InsertError::LackOfRequiredColumn("name".to_owned()).into())
    };

    test! {
        sql: "SELECT * FROM Test;",
        expected: Ok(select_with_null!(
            id     | num     | name;
            I64(1)   I64(2)    Str("Hi boo".to_owned());
            I64(3)   I64(9)    Str("Kitty!".to_owned());
            I64(2)   I64(7)    Str("Monsters".to_owned());
            I64(17)  I64(30)   Str("Sullivan".to_owned());
            I64(1)   I64(28)   Str("Wazowski".to_owned());
            I64(1)   Null      Str("The end".to_owned())
        ))
    };

    run!("CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0;");

    test! {
        name: "insert into target from source",
        sql: "INSERT INTO Target SELECT * FROM Test;",
        expected: Ok(Payload::Insert(6))
    };

    test! {
        name: "target rows are equivalent to source rows",
        sql: "SELECT * FROM Target;",
        expected: Ok(select_with_null!(
            id     | num     | name;
            I64(1)   I64(2)    Str("Hi boo".to_owned());
            I64(3)   I64(9)    Str("Kitty!".to_owned());
            I64(2)   I64(7)    Str("Monsters".to_owned());
            I64(17)  I64(30)   Str("Sullivan".to_owned());
            I64(1)   I64(28)   Str("Wazowski".to_owned());
            I64(1)   Null      Str("The end".to_owned())
        ))
    };
});
