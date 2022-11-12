use crate::*;

test_case!(format, async move {
    use gluesql_core::{executor::EvaluateError, prelude::Value::*};

    let test_cases = vec![
        (
            "VALUES(FORMAT(DATE '2017-06-15', '%Y-%m'))",
            Ok(select!(
                column1
                Str;
                "2017-06".to_owned()
            )),
        ),
        (
            "SELECT FORMAT(DATE '2017-06-15','%Y-%m') AS date",
            Ok(select!(
                date
                Str;
                "2017-06".to_owned()
            )),
        ),
        (
            "SELECT FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp",
            Ok(select!(
                timestamp
                Str;
                "2015-09-05 23:56:04".to_owned()
            )),
        ),
        (
            "SELECT FORMAT(TIME '23:56:04','%H:%M') AS time",
            Ok(select!(
                time
                Str;
                "23:56".to_owned()
            )),
        ),
        (
            "SELECT 
                FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y') AS year
               ,FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%m') AS month
               ,FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%d') AS day
            ",
            Ok(select!(
            year              | month           | day;
            Str               | Str             | Str;
            "2015".to_owned()   "09".to_owned()   "05".to_owned()
            )),
        ),
        (
            "SELECT FORMAT('2015-09-05 23:56:04', '%Y-%m-%d %H') AS timestamp",
            Err(
                EvaluateError::UnsupportedExprForFormatFunction("2015-09-05 23:56:04".to_owned())
                    .into(),
            ),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
