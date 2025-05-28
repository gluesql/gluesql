use {
    crate::*,
    Value::*,
    gluesql_core::{ast::IndexOperator::*, prelude::*},
};

test_case!(and, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE NullIdx (
    id INTEGER,
    date DATE,
    flag BOOLEAN
)",
    )
    .await;

    g.run(
        "
        INSERT INTO NullIdx
            (id, date, flag)
        VALUES
            (1, '2020-03-20', True),
            (2, '2021-01-01', True),
            (3, '1989-02-01', False),
            (4, '2002-06-11', True),
            (5, '2030-03-01', False);
    ",
    )
    .await;

    g.test(
        "CREATE INDEX idx_id ON NullIdx (id)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_date ON NullIdx (date)",
        Ok(Payload::CreateIndex),
    )
    .await;

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    g.test_idx(
        "
        SELECT id, date, flag FROM NullIdx
        WHERE
            date < DATE '2040-12-24'
            AND flag = false
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Lt, "DATE '2040-12-24'"),
    )
    .await;

    g.test_idx(
        "
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND date < DATE '2020-12-24'
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false
        )),
        idx!(idx_date, Lt, "DATE '2020-12-24'"),
    )
    .await;

    g.test_idx(
        "
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND DATE '2030-11-24' > date
            AND id > 1
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Lt, "DATE '2030-11-24'"),
    )
    .await;

    g.test_idx(
        "
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id > 1
            AND DATE '2030-11-24' > date
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_id, Gt, "1"),
    )
    .await;

    g.test_idx(
        "
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id * 2 > 6
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            5     date!("2030-03-01")   false
        )),
        idx!(),
    )
    .await;

    g.test_idx(
        "
        SELECT * FROM NullIdx
        WHERE
            flag = False
            AND id * 2 > 6
            AND (date = DATE '2030-03-01' AND flag != True);
        ",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            5     date!("2030-03-01")   false
        )),
        idx!(idx_date, Eq, "DATE '2030-03-01'"),
    )
    .await;
});
