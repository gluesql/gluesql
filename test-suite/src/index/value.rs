use {
    crate::*,
    Value::*,
    chrono::NaiveTime,
    gluesql_core::{ast::IndexOperator::*, prelude::*},
};

test_case!(value, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE IdxValue (
    id INTEGER NULL,
    time TIME NULL,
    flag BOOLEAN
)",
    )
    .await;

    g.run(
        "
        INSERT INTO IdxValue
        VALUES
            (NULL, '01:30 PM', True),
            (1,    '12:10 AM', False),
            (2,    NULL,       True);
    ",
    )
    .await;

    g.test(
        "CREATE INDEX idx_id ON IdxValue (id)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_time ON IdxValue (time)",
        Ok(Payload::CreateIndex),
    )
    .await;
    g.test(
        "CREATE INDEX idx_flag ON IdxValue (flag)",
        Ok(Payload::CreateIndex),
    )
    .await;

    let t = |h, m| NaiveTime::from_hms_opt(h, m, 0).unwrap();

    g.test_idx(
        "SELECT * FROM IdxValue WHERE id = 1",
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, "1"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE time <= TIME '13:30:00'",
        Ok(select_with_null!(
            id     | time            | flag;
            I64(1)   Time(t(0, 10))    Bool(false);
            Null     Time(t(13, 30))   Bool(true)
        )),
        idx!(idx_time, LtEq, "TIME '13:30:00'"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE flag = ('ABC' IS NULL)",
        Ok(select_with_null!(
            id     | time           | flag;
            I64(1)   Time(t(0, 10))   Bool(false)
        )),
        idx!(idx_flag, Eq, "('ABC' IS NULL)"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE flag = (100 IS NOT NULL)",
        Ok(select_with_null!(
            id     | time            | flag;
            Null     Time(t(13, 30))   Bool(true);
            I64(2)   Null              Bool(true)
        )),
        idx!(idx_flag, Eq, "(100 IS NOT NULL)"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE id = +1",
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)   false
        )),
        idx!(idx_id, Eq, "+1"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE id = CAST('1' AS INTEGER)",
        Ok(select!(
            id  | time      | flag
            I64 | Time      | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, "CAST('1' AS INTEGER)"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE id = (1)",
        Ok(select!(
            id  | time      | flag
            I64 | Time      | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, "(1)"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE id = 1 + 1 * 5 / 5",
        Ok(select_with_null!(
            id     | time | flag;
            I64(2)   Null   Bool(true)
        )),
        idx!(idx_id, Eq, "1 + 1 * 5 / 5"),
    )
    .await;

    g.test_idx(
        "SELECT * FROM IdxValue WHERE flag = (True AND False)",
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)   false
        )),
        idx!(idx_flag, Eq, "(True AND False)"),
    )
    .await;
});
