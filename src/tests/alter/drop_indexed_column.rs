#![cfg(all(feature = "alter-table", feature = "index"))]

use crate::*;

test_case!(drop_indexed_column, async move {
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

    use {
        ast::{AstLiteral, Expr, IndexOperator::*},
        Value::*,
    };

    // create indexes
    run!("CREATE INDEX idx_name ON Test (num + 1)");
    run!("CREATE INDEX idx_id ON Test (id)");
    run!("CREATE INDEX idx_typed_string ON Test ((id))");
    run!("CREATE INDEX idx_binary_op ON Test (id || name);");
    run!("CREATE INDEX idx_unary_op ON Test (-id);");
    run!("CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));");

    // check indexes working
    test!(
        Err(AlterError::IdentifierNotFound(format!(
            "{:#?}",
            Expr::Literal(AstLiteral::Number("100".to_owned()))
        ))
        .into()),
        "CREATE INDEX idx_literal ON Test (100)"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(),
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
        idx!(idx_binary_op, Eq, r#""1Hello""#),
        r#"SELECT id, num, name FROM Test WHERE id || name = "1Hello""#
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, GtEq, "-7"),
        "SELECT id, num, name FROM Test WHERE -id >= -7"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_unary_op, Gt, "-7"),
        "SELECT id, num, name FROM Test WHERE -id > -7"
    );

    test_idx!(
        Ok(select!(
            id  | num | name
            I64 | I64 | Str;
            1     2     "Hello".to_owned()
        )),
        idx!(idx_cast, Eq, r#""1""#),
        r#"SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = "1""#
    );

    test!(
        Err(AlterError::TableNotFound("Noname".to_owned()).into()),
        "ALTER TABLE Noname DROP COLUMN id"
    );

    run!("ALTER TABLE Test DROP COLUMN id");

    test_idx!(
        Ok(select!(
            num | name
            I64 | Str;
            2     "Hello".to_owned()
        )),
        idx!(),
        "SELECT * FROM Test"
    );

    // Only idx_name remains.
    assert_eq!(1, schema!("Test").indexes.len());
});
