use {
    crate::*,
    gluesql_core::{
        data::KeyError, executor::AggregateError, prelude::Value::*, translate::TranslateError,
    },
};

test_case!(aggregate, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, age) VALUES
            (1, 10,   11),
            (2,  0,   90),
            (3,  9, NULL),
            (4,  3,    3),
            (5, 25, NULL);
    "
    );

    let test_cases = vec![
        ("SELECT COUNT(*) FROM Item", select!("COUNT(*)"; I64; 5)),
        ("SELECT count(*) FROM Item", select!("count(*)"; I64; 5)),
        (
            "SELECT COUNT(*), COUNT(*) FROM Item",
            select!("COUNT(*)" | "COUNT(*)"; I64 | I64; 5 5),
        ),
        (
            "SELECT SUM(quantity), MAX(quantity), MIN(quantity) FROM Item",
            select!(
                "SUM(quantity)" | "MAX(quantity)" | "MIN(quantity)"
                I64             | I64             | I64;
                47                25                0
            ),
        ),
        (
            "SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item",
            select!("SUM(quantity) * 2 + MAX(quantity) - 3 / 1"; I64; 116),
        ),
        (
            "SELECT SUM(age), MAX(age), MIN(age) FROM Item",
            select_with_null!(
                "SUM(age)" | "MAX(age)" | "MIN(age)";
                Null         I64(90)     I64(3)
            ),
        ),
        (
            "SELECT AVG(age) FROM Item",
            select_with_null!("AVG(age)"; Null),
        ),
        (
            "SELECT VARIANCE(age) FROM Item",
            select_with_null!("VARIANCE(age)"; Null),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item",
            select!("COUNT(age)" | "COUNT(quantity)"; I64 | I64; 3 5),
        ),
        (
            "SELECT AVG(id), AVG(quantity) FROM Item",
            select!(
                "AVG(id)" | "AVG(quantity)"
                F64       | F64;
                3.0         9.4
            ),
        ),
        (
            "SELECT VARIANCE(id), VARIANCE(quantity) FROM Item",
            select!(
                "VARIANCE(id)" | "VARIANCE(quantity)"
                F64            | F64;
                2.0              74.64
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }

    let error_cases = vec![
        (
            AggregateError::OnlyIdentifierAllowed.into(),
            "SELECT SUM(ifnull(age, 0)) from Item;",
        ),
        (
            AggregateError::UnsupportedCompoundIdentifier(expr!("id.name.ok")).into(),
            "SELECT SUM(id.name.ok) FROM Item;",
        ),
        (
            AggregateError::OnlyIdentifierAllowed.into(),
            "SELECT SUM(1 + 2) FROM Item;",
        ),
        (
            AggregateError::ValueNotFound("num".to_owned()).into(),
            "SELECT SUM(num) FROM Item;",
        ),
        (
            TranslateError::QualifiedWildcardInCountNotSupported("Foo.*".to_owned()).into(),
            "SELECT COUNT(Foo.*) FROM Item;",
        ),
        (
            TranslateError::WildcardFunctionArgNotAccepted.into(),
            "SELECT SUM(*) FROM Item;",
        ),
    ];

    for (error, sql) in error_cases {
        test!(Err(error), sql);
    }
});

test_case!(group_by, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER NULL,
            city TEXT,
            ratio FLOAT,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, city, ratio) VALUES
            (1,   10,   \"Seoul\",  0.2),
            (2,    0,   \"Dhaka\",  0.9),
            (3, NULL, \"Beijing\",  1.1),
            (3,   30, \"Daejeon\",  3.2),
            (4,   11,   \"Seoul\",   11),
            (5,   24, \"Seattle\", 6.11);
    "
    );

    let test_cases = vec![
        (
            "SELECT id, COUNT(*) FROM Item GROUP BY id",
            select!(id | "COUNT(*)"; I64 | I64; 1 1; 2 1; 3 2; 4 1; 5 1),
        ),
        (
            "SELECT id FROM Item GROUP BY id",
            select!(id; I64; 1; 2; 3; 4; 5),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city",
            select_with_null!(
                "SUM(quantity)" | "COUNT(*)" | city;
                I64(21)           I64(2)       Str("Seoul".to_owned());
                I64(0)            I64(1)       Str("Dhaka".to_owned());
                Null              I64(1)       Str("Beijing".to_owned());
                I64(30)           I64(1)       Str("Daejeon".to_owned());
                I64(24)           I64(1)       Str("Seattle".to_owned())
            ),
        ),
        (
            "SELECT id, city FROM Item GROUP BY city",
            select!(
                id  | city
                I64 | Str;
                1     "Seoul".to_owned();
                2     "Dhaka".to_owned();
                3     "Beijing".to_owned();
                3     "Daejeon".to_owned();
                5     "Seattle".to_owned()
            ),
        ),
        (
            "SELECT ratio FROM Item GROUP BY id, city",
            select!(ratio; F64; 0.2; 0.9; 1.1; 3.2; 11.0; 6.11),
        ),
        (
            "SELECT ratio FROM Item GROUP BY id, city HAVING ratio > 10",
            select!(ratio; F64; 11.0),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1",
            select!(
                "SUM(quantity)" | "COUNT(*)" | city
                I64             | I64        | Str;
                21                2            "Seoul".to_owned()
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }

    test!(
        Err(KeyError::FloatTypeKeyNotSupported.into()),
        "SELECT * FROM Item GROUP BY ratio;"
    );
});
