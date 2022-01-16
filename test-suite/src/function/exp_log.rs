use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(log2, async move {
    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT LOG2(1024))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            LOG2(64.0) as log2_1,
            LOG2(0.04) as log2_2
            FROM SingleItem",
            Ok(select!(
                log2_1          | log2_2;
                F64             | F64;
                64.0_f64.log2()   0.04_f64.log2()
            )),
        ),
        (
            "SELECT LOG2(32) as log2_with_int FROM SingleItem",
            Ok(select!(
                log2_with_int
                F64;
                f64::from(32).log2()
            )),
        ),
        (
            "SELECT LOG2('string') AS log2 FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG2")).into()),
        ),
        (
            "SELECT LOG2(NULL) AS log2 FROM SingleItem",
            Ok(select_with_null!(log2; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(log10, async move {
    use gluesql_core::prelude::Value::{Null, F64};

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT LOG10(100))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            LOG10(64.0) as log10_1,
            LOG10(0.04) as log10_2
            FROM SingleItem",
            Ok(select!(
                log10_1           | log10_2;
                F64               | F64;
                64.0_f64.log10()    0.04_f64.log10()
            )),
        ),
        (
            "SELECT LOG10(10) as log10_with_int FROM SingleItem",
            Ok(select!(
                log10_with_int
                F64;
                f64::from(10).log10()
            )),
        ),
        (
            "SELECT LOG10('string') AS log10 FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG10")).into()),
        ),
        (
            "SELECT LOG10(NULL) AS log10 FROM SingleItem",
            Ok(select_with_null!(log10; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(ln, async move {
    use gluesql_core::prelude::Value::{Null, F64};

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT LN(10))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            LN(64.0) as ln1,
            LN(0.04) as ln2
            FROM SingleItem",
            Ok(select!(
                ln1             | ln2;
                F64             | F64;
                64.0_f64.ln()     0.04_f64.ln()
            )),
        ),
        (
            "SELECT LN(10) as ln_with_int FROM SingleItem",
            Ok(select!(
                ln_with_int
                F64;
                f64::from(10).ln()
            )),
        ),
        (
            "SELECT LN('string') AS log10 FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LN")).into()),
        ),
        (
            "SELECT LN(NULL) AS ln FROM SingleItem",
            Ok(select_with_null!(ln; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(log, async move {
    use gluesql_core::prelude::Value::{Null, F64};

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT LOG(2, 64))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            LOG(64.0, 2.0) as log_1,
            LOG(0.04, 10.0) as log_2
            FROM SingleItem",
            Ok(select!(
                log_1               | log_2;
                F64                 | F64;
                64.0_f64.log(2.0)     0.04_f64.log(10.0)
            )),
        ),
        (
            "SELECT LOG(10, 10) as log_with_int FROM SingleItem",
            Ok(select!(
                log_with_int
                F64;
                f64::from(10).log(10.0)
            )),
        ),
        (
            "SELECT LOG('string', 10) AS log FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG")).into()),
        ),
        (
            "SELECT LOG(10, 'string') AS log FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("LOG")).into()),
        ),
        (
            "SELECT LOG(NULL, 10) AS log FROM SingleItem",
            Ok(select_with_null!(log; Null)),
        ),
        (
            "SELECT LOG(10, NULL) AS log FROM SingleItem",
            Ok(select_with_null!(log; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(exp, async move {
    use gluesql_core::prelude::Value::{Null, F64};

    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER DEFAULT EXP(3.3))",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT
            EXP(2.0) as exp1,
            EXP(5.5) as exp2
            FROM SingleItem",
            Ok(select!(
                exp1            | exp2;
                F64             | F64;
                2.0_f64.exp()     5.5_f64.exp()
            )),
        ),
        (
            "SELECT EXP(3) as exp_with_int FROM SingleItem",
            Ok(select!(
                exp_with_int
                F64;
                f64::from(3).exp()
            )),
        ),
        (
            "SELECT EXP('string') AS exp FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("EXP")).into()),
        ),
        (
            "SELECT EXP(NULL) AS exp FROM SingleItem",
            Ok(select_with_null!(exp; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
