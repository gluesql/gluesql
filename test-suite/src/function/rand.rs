use crate::*;

test_case!(rand, async move {
    use gluesql_core::prelude::{Payload, Value::*};

    let test_cases = vec![
        ("CREATE TABLE mytable (random float)", Ok(Payload::Create)),
        (
            "SELECT RAND() as r FROM mytable",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT RAND(123) as r FROM mytable",
            Ok(select!(
                r
                F64;
                0.17325464426155657
            )),
        ),
        (
            "SELECT RAND(789) as r FROM mytable",
            Ok(select!(
                r
                F64;
                0.9635218234007941
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
