use {crate::*, gluesql_core::prelude::Value::*};

test_case!(greatest, async move {
    test!(
        "SELECT GREATEST(1,6,9,7,0,10) AS goat;",
        Ok(select!(
            "goat" ; I64 ; 10
        ))
    );

    test!(
        "SELECT GREATEST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat;",
        Ok(select!(
            "goat" ; F64 ; 10.5
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
        "SELECT GREATEST('2023-07-17', '2022-07-17', '2023-06-17', '2023-07-20', '2024-07-17','2024-07-17','2024-07-16','2024-07-18') AS goat;",
        Ok(select!(
            "goat"
            Date;
            date!("2024-07-18")
        ))
    );

    test!(
        "SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat;",
        Ok(select!(
            "goat"; Str; "melona".to_owned()
        ))
    );
});
