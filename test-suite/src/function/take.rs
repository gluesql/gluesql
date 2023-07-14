use {crate::*, gluesql_core::executor::EvaluateError, gluesql_core::prelude::Value::*};

test_case!(take, async move {
    run!(
        "
        CREATE TABLE Take (
            items LIST
        );
        "
    );
    run!(
        r#"
            INSERT INTO Take VALUES
            (TAKE(CAST('[1, 2, 3, 4, 5]' AS LIST), 5));
        "#
    );
    test!(
        r#"select take(items, 0) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![]
        ))
    );
    test!(
        r#"select take(items, 3) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3)]
        ))
    );
    test!(
        r#"select take(items, 5) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3), I64(4), I64(5)]
        ))
    );
    test!(
        r#"select take(items, 10) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3), I64(4), I64(5)]
        ))
    );
    test!(
        r#"select take(items, -5) as mymistake from Take;"#,
        Err(EvaluateError::FunctionRequiresUSizeValue("TAKE".to_owned()).into())
    );
    test!(
        r#"select take(items, 'TEST') as mymistake from Take;"#,
        Err(EvaluateError::FunctionRequiresIntegerValue("TAKE".to_owned()).into())
    );
    test!(
        r#"select take(0, 3) as mymistake from Take;"#,
        Err(EvaluateError::ListTypeRequired.into())
    );
});
