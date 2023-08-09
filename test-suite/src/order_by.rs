use {
    crate::*,
    gluesql_core::{
        error::{SortError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(order_by, async move {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT NULL,
    rate FLOAT NULL
)",
    )
    .await
    .unwrap();
    g.run(
        "
        INSERT INTO Test (id, num, name, rate)
        VALUES
            (1, 2, 'Hello',    3.0),
            (1, 9, NULL,       NULL),
            (3, 4, 'World',    1.0),
            (4, 7, 'Thursday', NULL);
    ",
    )
    .await
    .unwrap();

    g.test(
        "SELECT id, num FROM Test",
        Ok(select!(
            id  | num
            I64 | I64;
            1     2;
            1     9;
            3     4;
            4     7
        )),
    )
    .await;

    macro_rules! s {
        ($v: literal) => {
            Str($v.to_owned())
        };
    }

    g.test(
        "SELECT id, num, name FROM Test ORDER BY id + num ASC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(9)   Null;
            I64(4)   I64(7)   s!("Thursday")
        )),
    )
    .await;

    g.test(
        "SELECT id, num, name FROM Test ORDER BY num DESC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(9)   Null;
            I64(4)   I64(7)   s!("Thursday");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(2)   s!("Hello")
        )),
    )
    .await;

    g.test(
        "SELECT id, num, name FROM Test ORDER BY name",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(2)   s!("Hello");
            I64(4)   I64(7)   s!("Thursday");
            I64(3)   I64(4)   s!("World");
            I64(1)   I64(9)   Null
        )),
    )
    .await;

    g.test(
        "SELECT id, num, name FROM Test ORDER BY name DESC",
        Ok(select_with_null!(
            id     | num    | name;
            I64(1)   I64(9)   Null;
            I64(3)   I64(4)   s!("World");
            I64(4)   I64(7)   s!("Thursday");
            I64(1)   I64(2)   s!("Hello")
        )),
    )
    .await;

    g.test(
        "SELECT id, num, name, rate FROM Test ORDER BY rate DESC, id DESC",
        Ok(select_with_null!(
            id     | num    | name           | rate;
            I64(4)   I64(7)   s!("Thursday")   Null;
            I64(1)   I64(9)   Null             Null;
            I64(1)   I64(2)   s!("Hello")      F64(3.0);
            I64(3)   I64(4)   s!("World")      F64(1.0)
        )),
    )
    .await;

    g.test(
        "SELECT id, num FROM Test ORDER BY id ASC, num DESC",
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;

    g.test(
        "
        SELECT id, num FROM Test
        ORDER BY
            (SELECT id FROM Test t2 WHERE Test.id = t2.id LIMIT 1) ASC,
            num DESC
        ",
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;

    g.test(
        "
        SELECT id, num FROM Test
        ORDER BY
            (SELECT t2.id FROM Test t2
                WHERE Test.id = t2.id
                ORDER BY (Test.id + t2.id) LIMIT 1
            ) ASC,
            num DESC;
        ",
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;

    g.test(
        "SELECT * FROM Test ORDER BY id NULLS FIRST",
        Err(TranslateError::OrderByNullsFirstOrLastNotSupported.into()),
    )
    .await;
    g.named_test(
        "ORDER BY aliases",
        "SELECT id AS C1, num AS C2 FROM Test ORDER BY C1 ASC, C2 DESC",
        Ok(select!(
            C1  | C2
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;
    g.named_test(
        "original column_names still work even if aliases were used at SELECT clause",
        "SELECT id AS C1, num AS C2 FROM Test ORDER BY id ASC, num DESC",
        Ok(select!(
            C1  | C2
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;
    g.named_test(
        "ORDER BY I64 and UnaryOperator::PLUS work as COLUMN_INDEX",
        "SELECT id, num FROM Test ORDER BY 1 ASC, +2 DESC",
        Ok(select!(
            id  | num
            I64 | I64;
            1     9;
            1     2;
            3     4;
            4     7
        )),
    )
    .await;
    g.named_test(
        "ORDER BY UnaryOperator::MINUS works as a normal integer",
        "SELECT id, num FROM Test ORDER BY -1",
        Ok(select!(
            id  | num
            I64 | I64;
            1     2;
            1     9;
            3     4;
            4     7
        )),
    )
    .await;
    g.named_test(
        "ORDER BY COLUMN_INDEX should be larger than 0",
        "SELECT id, num FROM Test ORDER BY 0",
        Err(SortError::ColumnIndexOutOfRange(0).into()),
    )
    .await;
    g.named_test(
        "ORDER BY COLUMN_INDEX should be less than the number of columns",
        "SELECT id, num FROM Test ORDER BY 3",
        Err(SortError::ColumnIndexOutOfRange(3).into()),
    )
    .await;
});
