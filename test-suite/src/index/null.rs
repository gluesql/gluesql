use {
    crate::*,
    Value::*,
    gluesql_core::{ast::IndexOperator::*, prelude::*},
};

test_case!(null, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE NullIdx (
    id INTEGER NULL,
    date DATE NULL,
    flag BOOLEAN NULL
)",
    )
    .await;

    g.run(
        "
        INSERT INTO NullIdx
            (id, date, flag)
        VALUES
            (NULL, NULL,         True),
            (1,    '2020-03-20', True),
            (2,    NULL,         NULL),
            (3,    '1989-02-01', False),
            (4,    NULL,         True);
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
    g.test(
        "CREATE INDEX idx_flag ON NullIdx (flag)",
        Ok(Payload::CreateIndex),
    )
    .await;

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    g.test_idx(
        "SELECT id, date, flag FROM NullIdx WHERE date < DATE '2040-12-24'",
        Ok(select!(
            id  | date                | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            1     date!("2020-03-20")   true
        )),
        idx!(idx_date, Lt, "DATE '2040-12-24'"),
    )
    .await;

    g.test_idx(
        "SELECT id, date, flag FROM NullIdx WHERE date >= DATE '2040-12-24'",
        Ok(select_with_null!(
            id     | date | flag;
            Null     Null   Bool(true);
            I64(2)   Null   Null;
            I64(4)   Null   Bool(true)
        )),
        idx!(idx_date, GtEq, "DATE '2040-12-24'"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM NullIdx WHERE flag = True",
        Ok(select_with_null!(
            id     | date                      | flag;
            Null     Null                        Bool(true);
            I64(1)   Date(date!("2020-03-20"))   Bool(true);
            I64(4)   Null                        Bool(true)
        )),
        idx!(idx_flag, Eq, "True"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM NullIdx WHERE id > 2",
        Ok(select_with_null!(
            id     | date                      | flag;
            I64(3)   Date(date!("1989-02-01"))   Bool(false);
            I64(4)   Null                        Bool(true);
            Null     Null                        Bool(true)
        )),
        idx!(idx_id, Gt, "2"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM NullIdx WHERE id IS NULL",
        Ok(select_with_null!(
            id   | date | flag;
            Null   Null   Bool(true)
        )),
        idx!(idx_id, Eq, "NULL"),
    )
    .await;

    g.test_idx(
        "SELECT id, date, flag FROM NullIdx WHERE date IS NOT NULL",
        Ok(select!(
            id     | date | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            1     date!("2020-03-20")   true
        )),
        idx!(idx_date, Lt, "NULL"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM NullIdx WHERE id = NULL",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "date".to_owned(), "flag".to_owned()],
            rows: vec![],
        }),
        idx!(),
    )
    .await;
});
