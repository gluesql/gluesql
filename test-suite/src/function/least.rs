use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(least, {
    let g = get_tester!();

    g.test(
        "SELECT LEAST(1,6,9,7,0,10) AS goat;",
        Ok(select!(
            "goat"; I64; 0
        )),
    )
    .await;

    g.test(
        "SELECT LEAST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat;",
        Ok(select!(
            "goat" ; F64; 0.1
        )),
    )
    .await;

    g.test(
        "SELECT LEAST('bibibik', 'babamba', 'melona') AS goat;",
        Ok(select!(
            "goat"; Str; "babamba".to_owned()
        )),
    )
    .await;

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    g.test(
        "SELECT LEAST(
          DATE '2023-07-17', 
          DATE '2022-07-17', 
          DATE '2023-06-17', 
          DATE '2024-07-17',
          DATE '2024-07-18') AS goat;",
        Ok(select!(
            "goat"; Date; date!("2022-07-17")
        )),
    )
    .await;

    g.test(
        "SELECT LEAST() AS goat;",
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "LEAST".to_owned(),
            expected_minimum: 2,
            found: 0,
        }
        .into()),
    )
    .await;

    g.test(
        "SELECT LEAST(1, 2, 'bibibik') AS goat;",
        Err(EvaluateError::NonComparableArgumentError("LEAST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT LEAST(NULL, 'bibibik', 'babamba', 'melona') AS goat;",
        Err(EvaluateError::NonComparableArgumentError("LEAST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT LEAST(NULL, NULL, NULL) AS goat;",
        Err(EvaluateError::NonComparableArgumentError("LEAST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT LEAST(true, false) AS goat;",
        Ok(select!(
            "goat"; Bool; false
        )),
    )
    .await;
});
