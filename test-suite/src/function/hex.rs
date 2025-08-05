use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(hex, {
    let g = get_tester!();

    g.test(
        "VALUES(HEX('Hello World'))",
        Ok(select!(
            column1
            Str;
            "48656C6C6F20576F726C64".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX('ABC'))",
        Ok(select!(
            column1
            Str;
            "414243".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX(''))",
        Ok(select!(
            column1
            Str;
            "".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX('228'))",
        Ok(select!(
            column1
            Str;
            "323238".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX(228))",
        Ok(select!(
            column1
            Str;
            "E4".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX(0))",
        Ok(select!(
            column1
            Str;
            "0".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX(-123))",
        Ok(select!(
            column1
            Str;
            "FFFFFFFFFFFFFF85".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(HEX(3.14))",
        Err(EvaluateError::FunctionRequiresIntegerOrStringValue("HEX".to_owned()).into()),
    )
    .await;

    g.test(r#"VALUES(HEX(NULL))"#, Ok(select_with_null!(column1; Null)))
        .await;

    g.test(
        r#"VALUES(HEX())"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "HEX".to_owned(),
            expected: 1,
            found: 0,
        }
        .into()),
    )
    .await;

    g.test(
        r#"VALUES(HEX('test', 'extra'))"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "HEX".to_owned(),
            expected: 1,
            found: 2,
        }
        .into()),
    )
    .await;
});
