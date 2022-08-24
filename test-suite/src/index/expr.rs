use {
    crate::*,
    gluesql_core::{
        ast::IndexOperator::*,
        executor::AlterError,
        prelude::{Payload, Value::*},
    },
};

test_case!(expr, async move {
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
        INSERT INTO Test
            (id, num, name)
        VALUES
            (1, 2, "Hello");
    "#
    );

    test!(Ok(Payload::CreateIndex), "CREATE INDEX idx_id ON Test (id)");

    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_typed_string ON Test ((id))"
    );

    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_binary_op ON Test (num || name);"
    );

    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_unary_op ON Test (-num);"
    );

    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));"
    );

    test!(
        Err(AlterError::IdentifierNotFound(expr!("100")).into()),
        "CREATE INDEX idx_literal ON Test (100)"
    );

    test!(
        Ok(Payload::Insert(1)),
        r#"INSERT INTO Test VALUES (4, 7, "Well");"#
    );

    test!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned();
            4     7     "Well".to_owned()
        )),
        "SELECT id, num, name FROM Test"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "1"),
        "SELECT id, num, name FROM Test WHERE id <= 1"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_id, LtEq, "(1)"),
        "SELECT id, num, name FROM Test WHERE id <= (1)"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, r#""2Hello""#),
        r#"SELECT id, num, name FROM Test WHERE num || name = "2Hello""#
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_binary_op, Eq, r#""2Hello""#),
        r#"SELECT id, num, name FROM Test WHERE (num || name) = "2Hello""#
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_binary_op, Eq, r#""7Well""#),
        r#"SELECT id, num, name FROM Test WHERE "7Well" = (num || name)"#
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_unary_op, Lt, "-2"),
        "SELECT id, num, name FROM Test WHERE -num < -2"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            4     7     "Well".to_owned()
        )),
        idx!(idx_cast, Eq, r#""4""#),
        r#"SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = "4""#
    );
});
