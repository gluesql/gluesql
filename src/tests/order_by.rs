use crate::*;

#[cfg(feature = "sorter")]
test_case!(order_by, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT NULL,
    rate FLOAT NULL
)"#
    );
    run!(
        r#"
        INSERT INTO Test (id, num, name, rate)
        VALUES
            (1, 2, "Hello",    3.0),
            (1, 9, NULL,       NULL),
            (3, 4, "World",    1.0),
            (4, 7, "Thursday", NULL);
    "#
    );

    use Value::*;

    test!(
        Ok(select!(
            id  | num
            I64 | I64;
            1     2;
            1     9;
            3     4;
            4     7
        )),
        "SELECT id, num FROM Test"
    );

    macro_rules! s {
        ($v: literal) => {
            Str($v.to_owned())
        };
    }

    test!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(9)   Null;
            I64(4)   I64(7)   s!("Thursday")
        )),
        "SELECT id, num, name FROM Test ORDER BY id + num ASC"
    );

    test!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(9)   Null;
            I64(4)   I64(7)   s!("Thursday");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(2)   s!("Hello")
        )),
        "SELECT id, num, name FROM Test ORDER BY num DESC"
    );

    test!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(4)   I64(7)   s!("Thursday");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(9)   Null
        )),
        "SELECT id, num, name FROM Test ORDER BY name"
    );

    test!(
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(9)   Null;
            I64(3)   I64(4)   s!("World");
            I64(4)   I64(7)   s!("Thursday");
            I64(1)   I64(2)   s!("Hello")
        )),
        "SELECT id, num, name FROM Test ORDER BY name DESC"
    );

    test!(
        Ok(select_with_null!(
            id     | num    | name           | rate;
            I64(4)   I64(7)   s!("Thursday")   Null;
            I64(1)   I64(9)   Null             Null;
            I64(1)   I64(2)   s!("Hello")      F64(3.0);
            I64(3)   I64(4)   s!("World")      F64(1.0)
        )),
        "SELECT id, num, name, rate FROM Test ORDER BY rate DESC, id DESC"
    );

    test!(
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
        "SELECT id, num FROM Test ORDER BY id ASC, num DESC"
    );

    test!(
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
        "
        SELECT id, num FROM Test
        ORDER BY
            (SELECT id FROM Test t2 WHERE Test.id = t2.id LIMIT 1) ASC,
            num DESC
        "
    );

    test!(
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
        "
        SELECT id, num FROM Test
        ORDER BY
            (SELECT t2.id FROM Test t2
                WHERE Test.id = t2.id
                ORDER BY (Test.id + t2.id) LIMIT 1
            ) ASC,
            num DESC;
        "
    );

    test!(
        Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into()),
        "SELECT * FROM Test ORDER BY id NULLS FIRST"
    );
});

#[cfg(not(feature = "sorter"))]
test_case!(order_by, async move {
    use ast::{Expr, OrderByExpr};

    run!("CREATE TABLE Test (id INTEGER);");

    test!(
        Err(
            SelectError::OrderByOnNonIndexedExprNotSupported(vec![OrderByExpr {
                expr: Expr::Identifier("id".to_owned()),
                asc: Some(false),
            }])
            .into()
        ),
        "SELECT * FROM Test ORDER BY id DESC"
    );

    test!(
        Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into()),
        "SELECT * FROM Test ORDER BY id NULLS LAST"
    );
});
