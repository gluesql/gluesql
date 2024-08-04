use {
    crate::*,
    gluesql_core::{
        error::TranslateError,
        prelude::{Payload, Value::*},
    },
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
        "CREATE TABLE MD5 (id INTEGER, text TEXT);",
        Ok(Payload::Create),
    )
    .await;

    g.test(
        "INSERT INTO MD5 VALUES (1, 'GlueSQL Hi');",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.test(
        "SELECT MD5(text) AS md5 FROM MD5;",
        Ok(select!(
            md5
            Str;
            "eab30259ac1a92b66794f301a6ac3ff3".to_owned()
        )),
    )
    .await;

    g.test(
        r#"SELECT MD5(NULL) AS md5 FROM MD5;"#,
        Ok(select_with_null!(md5; Null)),
    )
    .await;

    g.test(
        r#"SELECT MD5() FROM MD5;"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "MD5".to_owned(),
            expected: 1,
            found: 0,
        }
        .into()),
    )
    .await;
});
