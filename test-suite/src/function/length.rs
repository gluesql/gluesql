use {crate::*, gluesql_core::prelude::Value::*};

test_case!(length, async move {
    test! {
        name: "test length with string",
        sql: "SELECT LENGTH('Hello.');",
        expected: Ok(select!(
            "LENGTH('Hello.')"
            U64;
            6
        ))
    };

    test! {
        name: "test length with list",
        sql: r#"SELECT LENGTH(CAST('[1, 2, 3]' AS LIST))"#,
        expected: Ok(select!(
            "LENGTH(CAST('[1, 2, 3]' AS LIST))"
            U64;
            3
        ))
    };

    test! {
        name: "test length with map",
        sql: r#"SELECT LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP))"#,
        expected: Ok(select!(
            "LENGTH(CAST('{\"a\": 1, \"b\": 5, \"c\": 9, \"d\": 10}' AS MAP))"
            U64;
            4
        ))
    };

    test! {
        name: "test length string - wide chars 1",
        sql: "SELECT LENGTH('한글');",
        expected: Ok(select!(
            "LENGTH('한글')"
            U64;
            2
        ))
    };

    test! {
        name: "test length string - wide chars 2",
        sql: "SELECT LENGTH('한글 abc');",
        expected: Ok(select!(
            "LENGTH('한글 abc')"
            U64;
            6
        ))
    };

    test! {
        name: "test length string - wide chars 3",
        sql: "SELECT LENGTH('é');",
        expected: Ok(select!(
            "LENGTH('é')"
            U64;
            1
        ))
    };

    test! {
        name: "test length string - wide chars 4",
        sql: "SELECT LENGTH('🧑');",
        expected: Ok(select!(
            "LENGTH('🧑')"
            U64;
            1
        ))
    };

    test! {
        name: "test length string - wide chars 5",
        sql: "SELECT LENGTH('❤️');",
        expected: Ok(select!(
            "LENGTH('❤️')"
            U64;
            2
        ))
    };

    test! {
        name: "test length string - wide chars 6",
        sql: "SELECT LENGTH('👩‍🔬');",
        expected: Ok(select!(
            "LENGTH('👩‍🔬')"
            U64;
            3
        ))
    };
});
