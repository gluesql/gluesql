use {
    crate::*,
    gluesql_core::{
        ast::IndexOperator::*,
        prelude::{Payload, Value::*},
    },
};

test_case!(nested, async move {
    run!(
        r#"
CREATE TABLE User (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#
    );

    run!(
        r#"
        INSERT INTO User
            (id, num, name)
        VALUES
            (1, 2, "Hello"),
            (2, 4, "World"),
            (3, 9, "Office"),
            (4, 1, "Origin"),
            (5, 2, "Builder");
    "#
    );

    test!(Ok(Payload::CreateIndex), "CREATE INDEX idx_id ON User (id)");

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
        "
        SELECT * FROM User u1
        WHERE (
            SELECT u1.id = id FROM User
            WHERE id = 1
            LIMIT 1
        )"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
        "
        SELECT * FROM User u1
        WHERE EXISTS(
            SELECT * FROM User
            WHERE id = 1 AND u1.id = id
        )"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, Eq, "1"),
        "
        SELECT * FROM User u1
        WHERE id IN (
            SELECT * FROM User WHERE id = 1
        )"
    );
});
