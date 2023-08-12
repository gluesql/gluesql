use {crate::*, gluesql_core::prelude::Value::*};

test_case!(splice, async move {
    run!(
        "
        CREATE TABLE ListTable (
            id INTEGER,
            items LIST
        );
        "
    );

    run!(
        r#"
        INSERT INTO ListTable VALUES
            (1, '[1, 2, 3]'),
            (2, '["1", "2", "3"]'),
            (3, '["1", 2, 3]')
        "#
    );

    test! (
        name: "SPLICE('[1, 2, 3, 4, 5]', 1, 3) should return '[1, 4, 5]'",
        sql: "SELECT SPLICE('[1, 2, 3, 4, 5]', 1, 3) AS actual",
        expected: Ok(select!(actual List; vec![I64(1), I64(4), I64(5)]))
    );

    test!(
        name: "SPLICE('[1, 2, 3, 4, 5]', 1, 3, '[100, 99]') should return '[1, 100, 99, 4, 5]'",
        sql: "SPLICE('[1, 2, 3, 4, 5]', 1, 3, '[100, 99]') AS actual",
        expected: Ok(select!(actual List; vec![I64(1), I64(100), I64(99), I64(4), I64(5)]))
    );

    test!(
        name: "SPLICE('[1, 2, 3]', -1, 2, '[100, 99]') should return '[100, 99, 3]'",
        sql: "SPLICE('[1, 2, 3]', -1, 2, '[100, 99]') AS actual",
        expected: Ok(select!(actual List; vec![I64(100), I64(99), I64(3)]))
    );

    test!(
        name: "SPLICE('[1, 2, 3]', 1, 100, '[100, 99]') should return '[1, 100, 99]')",
        sql: "SPLICE('[1, 2, 3]', 1, 100, '[100, 99]') AS actual",
        expected: Ok(select!(actual List; vec!(I64(1), I64(100), I64(99))))
    );
});
