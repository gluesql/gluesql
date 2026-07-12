use super::*;

test_case!(value, {
    let g = get_tester!();

    // More test cases are in `gluesql::Value` unit tests.

    let test_cases = [
        (
            "
            CREATE TABLE Item (
                id INTEGER NULL,
                flag BOOLEAN,
                ratio FLOAT NULL,
                number TEXT
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES (0, TRUE, NULL, '1')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item",
            Ok(select!(cast I64; 1)),
        ),
        (
            "SELECT CAST(id AS BOOLEAN) AS cast FROM Item",
            Ok(select!(cast Bool; false)),
        ),
        (
            "SELECT CAST(flag AS TEXT) AS cast FROM Item",
            Ok(select!(cast Str; "TRUE".to_owned())),
        ),
        (
            "SELECT CAST(ratio AS INTEGER) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(number AS BOOLEAN) FROM Item",
            Err(ValueError::ConvertFailed {
                value: Str("1".to_owned()),
                data_type: DataType::Boolean,
            }
            .into()),
        ),
        (
            "
        CREATE TABLE IntervalLog (
        id INTEGER,
        interval_str_1 TEXT,
        interval_str_2 TEXT
    )",
            Ok(Payload::Create),
        ),
        (
            "
        INSERT INTO IntervalLog VALUES
        (1, '''1-2'' YEAR TO MONTH',         '''30'' MONTH'),
        (2, '''12'' DAY',                    '''35'' HOUR'),
        (3, '''12'' MINUTE',                 '''300'' SECOND'),
        (4, '''-3 14'' DAY TO HOUR',         '''3 12:30'' DAY TO MINUTE'),
        (5, '''3 14:00:00'' DAY TO SECOND',  '''3 12:30:12.1324'' DAY TO SECOND'),
        (6, '''12:00'' HOUR TO MINUTE',      '''-12:30:12'' HOUR TO SECOND'),
        (7, '''-1000-11'' YEAR TO MONTH',    '''-30:11'' MINUTE TO SECOND');
    ",
            Ok(Payload::Insert(7)),
        ),
        (
            "SELECT id, CAST(interval_str_1 as INTERVAL) as stoi_1, CAST(interval_str_2 as INTERVAL) as stoi_2 FROM IntervalLog;",
            Ok(select!(
            id  | stoi_1              | stoi_2
            I64 | Interval            | Interval;
            1     I::months(14)         I::months(30);
            2     I::days(12)           I::hours(35);
            3     I::minutes(12)        I::minutes(5);
            4     I::hours(-86)         I::minutes(84 * 60 + 30);
            5     I::minutes(86 * 60)   I::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400);
            6     I::hours(12)          I::seconds(-(12 * 3600 + 30 * 60 + 12));
            7     I::months(-12_011)    I::seconds(-(30 * 60 + 11))
            )),
        ),
        (
            "SELECT CAST(1 AS STRING FORMAT 'ASCII') AS bytes_to_string;",
            Err(TranslateError::UnsupportedCastFormat("'ASCII'".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
