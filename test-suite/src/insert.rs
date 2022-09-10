use {
    crate::*,
    gluesql_core::{
        prelude::{Payload, Value::*},
    },
};

test_case!(insert, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER NULL,
    name TEXT
)"#
    );

    test! {
        name: "basic insert - single item",
        sql: r#"INSERT INTO Test (id, num, name) VALUES (1, 2, "Hi boo");"#,
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
        sql:r#"INSERT INTO Test VALUES(17, 30, "Sullivan");"#,
        expected: Ok(Payload::Insert(1))
    };

    test! {
        sql: r#"INSERT INTO Test (num, name) VALUES (28, "Wazowski");"#,
        expected: Ok(Payload:: Insert(1))
    };

    test! {
        sql: r#"INSERT INTO Test (name) VALUES ("The end");"#,
        expected: Ok(Payload:: Insert(1)) 
    };

    test! {
        sql: "SELECT * FROM Test",
        expected: Ok(select_with_null!(
            id  | num | name;
            I64(1)   I64(2)     Str("Hi boo".to_owned());
            I64(3)   I64(9)     Str("Kitty!".to_owned());
            I64(2)   I64(7)     Str("Monsters".to_owned());
            I64(17)  I64(30)    Str("Sullivan".to_owned());
            I64(1)   I64(28)    Str("Wazowski".to_owned());
            I64(1)   Null      Str("The end".to_owned())
        ))
    };
});
