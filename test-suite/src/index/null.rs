use {
    crate::*,
    gluesql_core::{ast::IndexOperator::*, prelude::*},
    Value::*,
};

test_case!(null, async move {
    run!(
        "
CREATE TABLE NullIdx (
    id INTEGER NULL,
    date DATE NULL,
    flag BOOLEAN NULL
)"
    );

    run!(
        "
        INSERT INTO NullIdx
            (id, date, flag)
        VALUES
            (NULL, NULL,         True),
            (1,    '2020-03-20', True),
            (2,    NULL,         NULL),
            (3,    '1989-02-01', False),
            (4,    NULL,         True);
    "
    );

    test!(
        "CREATE INDEX idx_id ON NullIdx (id)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_date ON NullIdx (date)",
        Ok(Payload::CreateIndex)
    );
    test!(
        "CREATE INDEX idx_flag ON NullIdx (flag)",
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
            1     date!("2020-03-20")   true
        )),
        idx!(idx_date, Lt, "DATE '2040-12-24'"),
        "SELECT id, date, flag FROM NullIdx WHERE date < DATE '2040-12-24'"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | date | flag;
            Null     Null   Bool(true);
            I64(2)   Null   Null;
            I64(4)   Null   Bool(true)
        )),
        idx!(idx_date, GtEq, "DATE '2040-12-24'"),
        "SELECT id, date, flag FROM NullIdx WHERE date >= DATE '2040-12-24'"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | date                      | flag;
            Null     Null                        Bool(true);
            I64(1)   Date(date!("2020-03-20"))   Bool(true);
            I64(4)   Null                        Bool(true)
        )),
        idx!(idx_flag, Eq, "True"),
        "SELECT * FROM NullIdx WHERE flag = True"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | date                      | flag;
            I64(3)   Date(date!("1989-02-01"))   Bool(false);
            I64(4)   Null                        Bool(true);
            Null     Null                        Bool(true)
        )),
        idx!(idx_id, Gt, "2"),
        "SELECT * FROM NullIdx WHERE id > 2"
    );

    test_idx!(
        Ok(select_with_null!(
            id   | date | flag;
            Null   Null   Bool(true)
        )),
        idx!(idx_id, Eq, "NULL"),
        "SELECT * FROM NullIdx WHERE id IS NULL"
    );

    test_idx!(
        Ok(select!(
            id     | date | flag
            I64 | Date                | Bool;
            3     date!("1989-02-01")   false;
            1     date!("2020-03-20")   true
        )),
        idx!(idx_date, Lt, "NULL"),
        "SELECT id, date, flag FROM NullIdx WHERE date IS NOT NULL"
    );

    test_idx!(
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "date".to_owned(), "flag".to_owned()],
            rows: vec![],
        }),
        idx!(),
        "SELECT * FROM NullIdx WHERE id = NULL"
    );
});
