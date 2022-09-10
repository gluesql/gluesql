use {
    crate::*,
    chrono::NaiveDate,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(default, async move {
    let test_cases = [
        (
            "CREATE TABLE Test (
                id INTEGER DEFAULT 1,
                num INTEGER,
                flag BOOLEAN NULL DEFAULT false
            )",
            Payload::Create,
        ),
        ("INSERT INTO Test VALUES (8, 80, true);", Payload::Insert(1)),
        ("INSERT INTO Test (num) VALUES (10);", Payload::Insert(1)),
        (
            "INSERT INTO Test (num, id) VALUES (20, 2);",
            Payload::Insert(1),
        ),
        (
            "INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true);",
            Payload::Insert(2),
        ),
        (
            "SELECT * FROM Test;",
            select_with_null!(
                id     | num     | flag;
                I64(8)   I64(80)   Bool(true);
                I64(1)   I64(10)   Bool(false);
                I64(2)   I64(20)   Bool(false);
                I64(1)   I64(30)   Null;
                I64(1)   I64(40)   Bool(true)
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }

    let stateless_function_test_cases = [
        (
            "CREATE TABLE FunctionTest (
                uuid UUID,
                num FLOAT
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO FunctionTest VALUES (GENERATE_UUID(), 1.0)",
            Ok(Payload::Insert(1)),
        ),
        (
            "INSERT INTO FunctionTest VALUES (GENERATE_UUID(), (SELECT id FROM Foo))",
            Err(EvaluateError::UnsupportedStatelessExpr(expr!("(SELECT id FROM Foo)")).into()),
        ),
    ];

    for (sql, expected) in stateless_function_test_cases {
        test!(sql, expected);
    }

    test!(
        r#"
        CREATE TABLE TestExpr (
            id INTEGER,
            date DATE DEFAULT DATE "2020-01-01",
            num INTEGER DEFAULT -(-1 * +2),
            flag BOOLEAN DEFAULT CAST("TRUE" AS BOOLEAN),
            flag2 BOOLEAN DEFAULT 1 IN (1, 2, 3),
            flag3 BOOLEAN DEFAULT 10 BETWEEN 1 AND 2,
            flag4 BOOLEAN DEFAULT (1 IS NULL OR NULL IS NOT NULL)
        )"#,
        Ok(Payload::Create)
    );

    run!("INSERT INTO TestExpr (id) VALUES (1);");

    let d = NaiveDate::from_ymd;

    test!(
        "SELECT * FROM TestExpr",
        Ok(select!(
            id  | date          | num | flag | flag2 | flag3 | flag4;
            I64 | Date          | I64 | Bool | Bool  | Bool  | Bool;
            1     d(2020, 1, 1)   2     true   true    false   false
        ))
    );
});
