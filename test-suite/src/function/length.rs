use {crate::*, gluesql_core::prelude::Value::*};

test_case!(length, {
    let g = get_tester!();

    g.named_test(
        "test length with string",
        "SELECT LENGTH('Hello.');",
        Ok(select!(
            "LENGTH('Hello.')"
            U64;
            6
        )),
    );

    g.named_test(
        "test length with list",
        r"SELECT LENGTH(CAST('[1, 2, 3]' AS LIST))",
        Ok(select!(
            "LENGTH(CAST('[1, 2, 3]' AS LIST))"
            U64;
            3
        )),
    );

    g.named_test(
        "test length with map",
        r#"SELECT LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP))"#,
        Ok(select!(
            "LENGTH(CAST('{\"a\": 1, \"b\": 5, \"c\": 9, \"d\": 10}' AS MAP))"
            U64;
            4
        )),
    );

    g.named_test(
        "test length string - wide chars 1",
        "SELECT LENGTH('한글');",
        Ok(select!(
            "LENGTH('한글')"
            U64;
            2
        )),
    );

    g.named_test(
        "test length string - wide chars 2",
        "SELECT LENGTH('한글 abc');",
        Ok(select!(
            "LENGTH('한글 abc')"
            U64;
            6
        )),
    );

    g.named_test(
        "test length string - wide chars 3",
        "SELECT LENGTH('é');",
        Ok(select!(
            "LENGTH('é')"
            U64;
            1
        )),
    );

    g.named_test(
        "test length string - wide chars 4",
        "SELECT LENGTH('🧑');",
        Ok(select!(
            "LENGTH('🧑')"
            U64;
            1
        )),
    );

    g.named_test(
        "test length string - wide chars 5",
        "SELECT LENGTH('❤️');",
        Ok(select!(
            "LENGTH('❤️')"
            U64;
            2
        )),
    );

    g.named_test(
        "test length string - wide chars 6",
        "SELECT LENGTH('👩‍🔬');",
        Ok(select!(
            "LENGTH('👩‍🔬')"
            U64;
            3
        )),
    );
});
