use {crate::*, gluesql_core::ast::IndexOperator::*, gluesql_core::prelude::*, Value::*};

test_case!(and, async move {
    run!(
        r#"
CREATE TABLE NullIdx (
    id INTEGER,
    date DATE,
    flag BOOLEAN
)"#
    );

    run!(
        r#"
        INSERT INTO NullIdx
            (id, date, flag)
        VALUES
            (1, "2020-03-20", True),
            (2, "2021-01-01", True),
            (3, "1989-02-01", False),
            (4, "2002-06-11", True),
            (5, "2030-03-01", False);
    "#
    );

    test!(
        "CREATE INDEX idx_id ON NullIdx (id)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_date ON NullIdx (date)",
        Ok(Payload::CreateIndex)
    );

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Lt, r#"DATE "2040-12-24""#),
        r#"
        SELECT id, date, flag FROM NullIdx
        WHERE
            date < DATE "2040-12-24"
            AND flag = false
        "#
    );

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false
        )),
        idx!(idx_date, Lt, r#"DATE "2020-12-24""#),
        r#"
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND date < DATE "2020-12-24"
        "#
    );

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Lt, r#"DATE "2030-11-24""#),
        r#"
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND DATE "2030-11-24" > date
            AND id > 1
        "#
    );

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_id, Gt, "1"),
        r#"
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id > 1
            AND DATE "2030-11-24" > date
        "#
    );

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            5     date!("2030-03-01")   false
        )),
        idx!(),
        r#"
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id * 2 > 6
        "#
    );

    test_idx!(
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Eq, r#"DATE "2030-03-01""#),
        r#"
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id * 2 > 6
            AND (date = DATE "2030-03-01" AND flag != True);
        "#
    );
});
