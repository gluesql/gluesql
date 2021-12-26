use crate::*;

test_case!(interval, async move {
    use gluesql_core::data::{self, IntervalError};
    run!(
        r#"
CREATE TABLE IntervalLog (
    id INTEGER,
    interval1 INTERVAL,
    interval2 INTERVAL,
)"#
    );

    run!(
        r#"
INSERT INTO IntervalLog VALUES
    (1, INTERVAL "1-2" YEAR TO MONTH,         INTERVAL "30" MONTH),
    (2, INTERVAL "12" DAY,                    INTERVAL "35" HOUR),
    (3, INTERVAL "12" MINUTE,                 INTERVAL "300" SECOND),
    (4, INTERVAL "-3 14" DAY TO HOUR,         INTERVAL "3 12:30" DAY TO MINUTE),
    (5, INTERVAL "3 14:00:00" DAY TO SECOND,  INTERVAL "3 12:30:12.1324" DAY TO SECOND),
    (6, INTERVAL "12:00" HOUR TO MINUTE,      INTERVAL "-12:30:12" HOUR TO SECOND),
    (7, INTERVAL "-1000-11" YEAR TO MONTH,    INTERVAL "-30:11" MINUTE TO SECOND);
"#
    );

    use data::Interval as I;
    use gluesql_core::prelude::Value::*;

    test!(
        Ok(select!(
            id  | interval1           | interval2
            I64 | Interval            | Interval;
            1     I::months(14)         I::months(30);
            2     I::days(12)           I::hours(35);
            3     I::minutes(12)        I::minutes(5);
            4     I::hours(-86)         I::minutes(84 * 60 + 30);
            5     I::minutes(86 * 60)   I::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400);
            6     I::hours(12)          I::seconds(-(12 * 3600 + 30 * 60 + 12));
            7     I::months(-12_011)    I::seconds(-(30 * 60 + 11))
        )),
        "SELECT * FROM IntervalLog;"
    );

    test!(
        Ok(select!(
            id  | i1            | i2
            I64 | Interval      | Interval;
            1     I::months(28)   I::months(66)
        )),
        r#"SELECT
            id,
            interval1 * 2 AS i1,
            interval2 - INTERVAL "-3" YEAR AS i2
        FROM IntervalLog WHERE id = 1"#
    );

    test!(
        Ok(select!(
            id  | i1         | i2           | i3
            I64 | Interval   | Interval     | Interval;
            2     I::days(4)   I::hours(34)   I::minutes(1)
        )),
        r#"SELECT
            id,
            interval1 / 3 AS i1,
            interval2 - INTERVAL "3600" SECOND AS i2,
            INTERVAL "30" SECOND + INTERVAL "10" SECOND * 3 AS i3
        FROM IntervalLog WHERE id = 2;"#
    );

    test!(
        Err(IntervalError::UnsupportedRange("Minute".to_owned(), "Hour".to_owned()).into()),
        r#"INSERT INTO IntervalLog VALUES (1, INTERVAL "20:00" MINUTE TO HOUR, INTERVAL "1-2" YEAR TO MONTH)"#
    );

    test!(
        Err(IntervalError::AddBetweenYearToMonthAndHourToSecond.into()),
        r#"SELECT INTERVAL "1" YEAR + INTERVAL "1" HOUR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::SubtractBetweenYearToMonthAndHourToSecond.into()),
        r#"SELECT INTERVAL "1" YEAR - INTERVAL "1" HOUR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseInteger("1.4".to_owned()).into()),
        r#"SELECT INTERVAL "1.4" YEAR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseDecimal("1.4ab".to_owned()).into()),
        r#"SELECT INTERVAL "1.4ab" HOUR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseTime("111:34".to_owned()).into()),
        r#"SELECT INTERVAL "111:34" HOUR TO MINUTE FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseYearToMonth("111".to_owned()).into()),
        r#"SELECT INTERVAL "111" YEAR TO MONTH FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseDayToHour("111".to_owned()).into()),
        r#"SELECT INTERVAL "111" DAY TO HOUR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseDayToHour("111".to_owned()).into()),
        r#"SELECT INTERVAL "111" DAY TO HOUR FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseDayToMinute("111".to_owned()).into()),
        r#"SELECT INTERVAL "111" DAY TO MINUTE FROM IntervalLog;"#
    );

    test!(
        Err(IntervalError::FailedToParseDayToSecond("111".to_owned()).into()),
        r#"SELECT INTERVAL "111" DAY TO Second FROM IntervalLog;"#
    );
});
