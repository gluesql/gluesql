use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(take, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Take (
            items LIST
        );
        ",
    )
    .await;
    g.run(
        r#"
            INSERT INTO Take VALUES
            (TAKE(CAST('[1, 2, 3, 4, 5]' AS LIST), 5));
        "#,
    )
    .await;
    g.test(
        r#"select take(items, 0) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![]
        )),
    )
    .await;
    g.test(
        r#"select take(items, 3) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3)]
        )),
    )
    .await;
    g.test(
        r#"select take(items, 5) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3), I64(4), I64(5)]
        )),
    )
    .await;
    g.test(
        r#"select take(items, 10) as mygoodtake from Take;"#,
        Ok(select!(
            mygoodtake
            List;
            vec![I64(1), I64(2), I64(3), I64(4), I64(5)]
        )),
    )
    .await;
    g.test(
        r#"select take(NULL, 3) as mynulltake from Take;"#,
        Ok(select_with_null!(mynulltake; Null)),
    )
    .await;
    g.test(
        r#"select take(items, NULL) as mynulltake from Take;"#,
        Ok(select_with_null!(mynulltake; Null)),
    )
    .await;

    g.test(
        r#"select take(items, -5) as mymistake from Take;"#,
        Err(EvaluateError::FunctionRequiresUSizeValue("TAKE".to_owned()).into()),
    )
    .await;
    g.test(
        r#"select take(items, 'TEST') as mymistake from Take;"#,
        Err(EvaluateError::FunctionRequiresIntegerValue("TAKE".to_owned()).into()),
    )
    .await;
    g.test(
        r#"select take(0, 3) as mymistake from Take;"#,
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;
});
