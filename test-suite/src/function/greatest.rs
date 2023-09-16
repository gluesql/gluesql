use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(greatest, {
    let g = get_tester!();

    g.test(
        "SELECT GREATEST(1,6,9,7,0,10) AS goat;",
        Ok(select!(
            "goat"; I64; 10
        )),
    )
    .await;

    g.test(
        "SELECT GREATEST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat;",
        Ok(select!(
            "goat" ; F64; 10.5
        )),
    )
    .await;

    g.test(
        "SELECT GREATEST('bibibik', 'babamba', 'melona') AS goat;",
        Ok(select!(
            "goat"; Str; "melona".to_owned()
        )),
    )
    .await;

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    g.test(
        "SELECT GREATEST(
            DATE '2023-07-17', 
            DATE '2022-07-17', 
            DATE '2023-06-17', 
            DATE '2024-07-17',
            DATE '2024-07-18') AS goat;",
        Ok(select!(
            "goat"; Date; date!("2024-07-18")
        )),
    )
    .await;

    g.test(
        "SELECT GREATEST() AS goat;",
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "GREATEST".to_owned(),
            expected_minimum: 2,
            found: 0,
        }
        .into()),
    )
    .await;

    g.test(
        "SELECT GREATEST(1, 2, 'bibibik') AS goat;",
        Err(EvaluateError::NonComparableArgumentError("GREATEST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat;",
        Err(EvaluateError::NonComparableArgumentError("GREATEST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT GREATEST(NULL, NULL, NULL) AS goat;",
        Err(EvaluateError::NonComparableArgumentError("GREATEST".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT GREATEST(true, false) AS goat;",
        Ok(select!(
            "goat"; Bool; true
        )),
    )
    .await;
});
