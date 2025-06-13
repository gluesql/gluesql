use {
    crate::*,
    gluesql_core::{error::TranslateError, prelude::Value::*},
};

test_case!(md5, {
    let g = get_tester!();

    g.test(
        "VALUES(MD5('GlueSQL'))",
        Ok(select!(
            column1
            Str;
            "4274ecec96f3ee59b51b168dc6137231".to_owned()
        )),
    )
    .await;

    g.test(
        "VALUES(MD5('GlueSQL Hi'))",
        Ok(select!(
            column1
            Str;
            "eab30259ac1a92b66794f301a6ac3ff3".to_owned()
        )),
    )
    .await;

    g.test(r#"VALUES(MD5(NULL))"#, Ok(select_with_null!(column1; Null)))
        .await;

    g.test(
        r#"VALUES(MD5())"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "MD5".to_owned(),
            expected: 1,
            found: 0,
        }
        .into()),
    )
    .await;
});
