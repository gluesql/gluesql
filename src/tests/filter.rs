use {
    crate::*,
    bigdecimal::BigDecimal,
    std::{borrow::Cow, str::FromStr},
};

test_case!(filter, async move {
    let create_sqls = [
        "
        CREATE TABLE Boss (
            id INTEGER,
            name TEXT,
            strength FLOAT
        );",
        "
        CREATE TABLE Hunter (
            id INTEGER,
            name TEXT
        );",
    ];

    for sql in create_sqls.iter() {
        run!(sql);
    }

    let insert_sqls = [
        "
        INSERT INTO Boss (id, name, strength) VALUES
            (1,    \"Amelia\", 10.10),
            (2,      \"Doll\", 20.20),
            (3, \"Gascoigne\", 30.30),
            (4,   \"Gehrman\", 40.40),
            (5,     \"Maria\", 50.50);
        ",
        "
        INSERT INTO Hunter (id, name) VALUES
            (1, \"Gascoigne\"),
            (2,   \"Gehrman\"),
            (3,     \"Maria\");
        ",
    ];

    for sql in insert_sqls.iter() {
        run!(sql);
    }

    let select_sqls = [
        (3, "SELECT id, name FROM Boss WHERE id BETWEEN 2 AND 4"),
        (
            3,
            "SELECT id, name FROM Boss WHERE name BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            2,
            "SELECT name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            2,
            "SELECT strength, name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            3,
            "SELECT name 
             FROM Boss 
             WHERE EXISTS (
                SELECT * FROM Hunter WHERE Hunter.name = Boss.name
             )",
        ),
        (
            2,
            "SELECT name 
             FROM Boss 
             WHERE NOT EXISTS (
                SELECT * FROM Hunter WHERE Hunter.name = Boss.name
             )",
        ),
        (5, "SELECT name FROM Boss WHERE +1 = 1"),
        (3, "SELECT id FROM Hunter WHERE -1 = -1"),
        (5, "SELECT name FROM Boss WHERE -2.0 < -1.0"),
        (3, "SELECT id FROM Hunter WHERE +2 > +1.0"),
        (2, "SELECT name FROM Boss WHERE id <= +2"),
        (2, "SELECT name FROM Boss WHERE +id <= 2"),
        (2, "SELECT name FROM Boss WHERE name LIKE '_a%'"),
        (2, "SELECT name FROM Boss WHERE name LIKE '%r%'"),
        (2, "SELECT name FROM Boss WHERE name LIKE '%a'"),
        (5, "SELECT name FROM Boss WHERE name LIKE '%%'"),
        (0, "SELECT name FROM Boss WHERE name LIKE 'g%'"),
        (2, "SELECT name FROM Boss WHERE name ILIKE '_A%'"),
        (2, "SELECT name FROM Boss WHERE name ILIKE 'g%'"),
        (5, "SELECT name FROM Boss WHERE name ILIKE '%%'"),
        (1, "SELECT name FROM Boss WHERE name NOT LIKE '%a%'"),
        (1, "SELECT name FROM Boss WHERE name NOT ILIKE '%A%'"),
        (5, "SELECT name FROM Boss WHERE 'ABC' LIKE '_B_'"),
        (5, "SELECT name FROM Boss WHERE 'abc' ILIKE '_B_'"),
        (5, "SELECT name FROM Boss WHERE 'ABC' ILIKE '_B_'"),
    ];

    for (num, sql) in select_sqls.iter() {
        count!(*num, sql);
    }

    let select_opt_sqls = [
        (5, "SELECT name FROM Boss WHERE 2 = 1.0 + 1"),
        (3, "SELECT id FROM Hunter WHERE -1.0 - 1.0 < -1"),
        (5, "SELECT name FROM Boss WHERE -2.0 * -3.0 = 6"),
        (3, "SELECT id FROM Hunter WHERE +2 / 1.0 > +1.0"),
    ];

    for (num, sql) in select_opt_sqls.iter() {
        count!(*num, sql);
    }

    let error_sqls = vec![
        (
            LiteralError::UnaryOperationOnNonNumeric.into(),
            "SELECT id FROM Hunter WHERE +'abcd' > 1.0",
        ),
        (
            LiteralError::UnaryOperationOnNonNumeric.into(),
            "SELECT id FROM Hunter WHERE -'abcd' < 1.0",
        ),
        (
            ValueError::UnaryPlusOnNonNumeric.into(),
            "SELECT id FROM Hunter WHERE +name > 1.0",
        ),
        (
            ValueError::UnaryMinusOnNonNumeric.into(),
            "SELECT id FROM Hunter WHERE -name < 1.0",
        ),
        (
            LiteralError::LikeOnNonString(
                format!("{:?}", Literal::Text(Cow::Owned("ABC".to_string()))),
                format!("{:?}", Literal::Number(BigDecimal::from_str("10").unwrap())),
            )
            .into(),
            "SELECT name FROM Boss WHERE 'ABC' LIKE 10",
        ),
        (
            ValueError::LikeOnNonString(Value::Str("Amelia".to_string()), Value::I64(10)).into(),
            "SELECT name FROM Boss WHERE name = 'Amelia' AND name LIKE 10",
        ),
        (
            ValueError::ILikeOnNonString(Value::Str("Amelia".to_string()), Value::I64(10)).into(),
            "SELECT name FROM Boss WHERE name = 'Amelia' AND name ILIKE 10",
        ),
    ];

    for (error, sql) in error_sqls {
        test!(Err(error), sql);
    }
});
