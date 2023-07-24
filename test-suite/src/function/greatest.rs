use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(greatest, async move {
    test!(
        "SELECT GREATEST(1,6,9,7,0,10) AS goat;",
        Ok(select!(
            "goat"; I64; 10
        ))
    );

    test!(
        "SELECT GREATEST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat;",
        Ok(select!(
            "goat" ; F64; 10.5
        ))
    );

    test!(
        "SELECT GREATEST('bibibik', 'babamba', 'melona') AS goat;",
        Ok(select!(
            "goat"; Str; "melona".to_owned()
        ))
    );

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    test!(
        "SELECT GREATEST(
            DATE '2023-07-17', 
            DATE '2022-07-17', 
            DATE '2023-06-17', 
            DATE '2024-07-17',
            DATE '2024-07-18') AS goat;",
        Ok(select!(
            "goat"; Date; date!("2024-07-18")
        ))
    );

    test!(
        "SELECT GREATEST() AS goat;",
        Err(TranslateError::FunctionArgsLengthNotMatchingMin {
            name: "GREATEST".to_owned(),
            expected_minimum: 2,
            found: 0,
        }
        .into())
    );

    test!(
        "SELECT GREATEST(1, 2, 'bibibik') AS goat;",
        Err(EvaluateError::CannotCompareDifferentTypes.into())
    );

    test!(
        "SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat;",
        Ok(select!(
            "goat"; Str; "melona".to_owned()
        ))
    );

    test!(
        "SELECT GREATEST(NULL, NULL, NULL) AS goat;",
        Ok(select_with_null!(
            "goat"; Null
        ))
    );

    test!(
        "SELECT GREATEST(1, NULL, NULL) AS goat;",
        Ok(select!(
            "goat"; I64; 1
        ))
    );

    test!(
        "SELECT GREATEST(NULL, NULL, 1) AS goat;",
        Ok(select!(
            "goat"; I64; 1
        ))
    );
});
