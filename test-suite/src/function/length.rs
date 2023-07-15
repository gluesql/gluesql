use gluesql_core::prelude::Value;

use {crate::*, gluesql_core::prelude::Payload};

test_case!(length, async move {
    test! {
        name: "test length with string",
        sql: "SELECT LENGTH('Hello.');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('Hello.')".to_owned()], rows: vec![vec![Value::U64(6)]] })
    };
    test! {
        name: "test length with list",
        sql: r#"SELECT LENGTH(CAST('[1, 2, 3]' AS LIST))"#,
        expected: Ok(Payload::Select { labels: vec![r#"LENGTH(CAST('[1, 2, 3]' AS LIST))"#.to_owned()], rows: vec![vec![Value::U64(3)]] })
    };
    test! {
        name: "test length with map",
        sql: r#"SELECT LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP))"#,
        expected: Ok(Payload::Select { labels: vec![r#"LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP))"#.to_owned()], rows: vec![vec![Value::U64(4)]] })
    };

    test! {
        name: "test length string - wide chars 1",
        sql: "SELECT LENGTH('한글');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('한글')".to_owned()], rows: vec![vec![Value::U64(2)]] })
    };
    test! {
        name: "test length string - wide chars 2",
        sql: "SELECT LENGTH('한글 abc');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('한글 abc')".to_owned()], rows: vec![vec![Value::U64(6)]] })
    };
    test! {
        name: "test length string - wide chars 3",
        sql: "SELECT LENGTH('é');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('é')".to_owned()], rows: vec![vec![Value::U64(1)]] })
    };
    test! {
        name: "test length string - wide chars 4",
        sql: "SELECT LENGTH('🧑');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('🧑')".to_owned()], rows: vec![vec![Value::U64(1)]] })
    };
    test! {
        name: "test length string - wide chars 5",
        sql: "SELECT LENGTH('❤️');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('❤️')".to_owned()], rows: vec![vec![Value::U64(2)]] })
    };
    test! {
        name: "test length string - wide chars 6",
        sql: "SELECT LENGTH('👩‍🔬');",
        expected: Ok(Payload::Select { labels: vec!["LENGTH('👩‍🔬')".to_owned()], rows: vec![vec![Value::U64(3)]] })
    };
});
