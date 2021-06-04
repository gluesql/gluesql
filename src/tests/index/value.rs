use crate::*;

test_case!(value, async move {
    run!(
        r#"
CREATE TABLE IdxValue (
    id INTEGER NULL,
    time TIME NULL,
    flag BOOLEAN
)"#
    );

    run!(
        r#"
        INSERT INTO IdxValue
        VALUES
            (NULL, "01:30 PM", True),
            (1,    "12:10 AM", False),
            (2,    NULL,       True);
    "#
    );

    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_id ON IdxValue (id)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_time ON IdxValue (time)"
    );
    test!(
        Ok(Payload::CreateIndex),
        "CREATE INDEX idx_flag ON IdxValue (flag)"
    );

    use ast::IndexOperator::*;
    use chrono::NaiveTime;
    use Value::*;

    let t = |h, m| NaiveTime::from_hms(h, m, 0);

    test_idx!(
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, "1"),
        "SELECT * FROM IdxValue WHERE id = 1"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | time            | flag;
            I64(1)   Time(t(0, 10))    Bool(false);
            Null     Time(t(13, 30))   Bool(true)
        )),
        idx!(idx_time, LtEq, r#"TIME "13:30:00""#),
        r#"SELECT * FROM IdxValue WHERE time <= TIME "13:30:00""#
    );

    test_idx!(
        Ok(select_with_null!(
            id     | time           | flag;
            I64(1)   Time(t(0, 10))   Bool(false)
        )),
        idx!(idx_flag, Eq, r#"("ABC" IS NULL)"#),
        r#"SELECT * FROM IdxValue WHERE flag = ("ABC" IS NULL)"#
    );

    test_idx!(
        Ok(select_with_null!(
            id     | time            | flag;
            Null     Time(t(13, 30))   Bool(true);
            I64(2)   Null              Bool(true)
        )),
        idx!(idx_flag, Eq, "(100 IS NOT NULL)"),
        "SELECT * FROM IdxValue WHERE flag = (100 IS NOT NULL)"
    );

    test_idx!(
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)   false
        )),
        idx!(idx_id, Eq, "+1"),
        "SELECT * FROM IdxValue WHERE id = +1"
    );

    test_idx!(
        Ok(select!(
            id  | time      | flag
            I64 | Time      | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, r#"CAST("1" AS INTEGER)"#),
        r#"SELECT * FROM IdxValue WHERE id = CAST("1" AS INTEGER)"#
    );

    test_idx!(
        Ok(select!(
            id  | time      | flag
            I64 | Time      | Bool;
            1     t(0, 10)    false
        )),
        idx!(idx_id, Eq, "(1)"),
        "SELECT * FROM IdxValue WHERE id = (1)"
    );

    test_idx!(
        Ok(select_with_null!(
            id     | time | flag;
            I64(2)   Null   Bool(true)
        )),
        idx!(idx_id, Eq, "1 + 1 * 5 / 5"),
        "SELECT * FROM IdxValue WHERE id = 1 + 1 * 5 / 5"
    );

    test_idx!(
        Ok(select!(
            id  | time     | flag
            I64 | Time     | Bool;
            1     t(0, 10)   false
        )),
        idx!(idx_flag, Eq, "(True AND False)"),
        "SELECT * FROM IdxValue WHERE flag = (True AND False)"
    );
});
