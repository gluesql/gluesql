use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        data::Literal,
        error::EvaluateError,
        error::TranslateError,
        prelude::Value::*,
    },
    std::borrow::Cow,
};

test_case!(iif, {
    let g = get_tester!();

    g.named_test(
        "IIF with TRUE should return THEN",
        "SELECT IIF(TRUE, 'a', 'b') AS result",
        Ok(select!(result; Str; "a".to_owned())),
    )
    .await;

    g.named_test(
        "IIF with FALSE should return ELSE",
        "SELECT IIF(FALSE, 1, 2) AS result",
        Ok(select!(result; I64; 2)),
    )
    .await;

    g.named_test(
        "IIF with boolean expression",
        "SELECT IIF(1 < 2, 10, 20) AS result",
        Ok(select!(result; I64; 10)),
    )
    .await;

    g.named_test(
        "IIF with non-boolean condition should throw EvaluateError",
        "SELECT IIF(1, 'a', 'b') AS result",
        Err(EvaluateError::BooleanTypeRequired(format!(
            "{:?}",
            Literal::Number(Cow::Owned(BigDecimal::from(1)))
        ))
        .into()),
    )
    .await;

    g.named_test(
        "IIF with wrong arity (2 args) should throw TranslateError",
        "SELECT IIF(1, 2) AS result",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "IIF".to_owned(),
            expected: 3,
            found: 2,
        }
        .into()),
    )
    .await;
}); 