use {
    crate::*,
    gluesql_core::{error::TranslateError, prelude::Payload, prelude::Value::*},
};

test_case!(md5, async move {
    test!(
        "VALUES(MD5('GlueSQL'))",
        Ok(select!(
            column1
            Str;
            "4274ECEC96F3EE59B51B168DC6137231".to_owned()
        ))
    );

    test!(
        "CREATE TABLE MD5 (id INTEGER, text TEXT);",
        Ok(Payload::Create)
    );

    test!(
        "INSERT INTO MD5 VALUES (1, 'GlueSQL Hi');",
        Ok(Payload::Insert(1))
    );

    test!(
        "SELECT MD5(text) FROM MD5;",
        Ok(select!(
            md5
            Str;
            "EAB30259AC1A92B66794F301A6AC3FF3".to_owned()
        ))
    );

    test!(
        r#"SELECT MD5(NULL) FROM MD5;"#,
        Ok(select_with_null!(md5; Null))
    );

    test!(
        r#"SELECT MD5() FROM MD5;"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "MD5".to_owned(),
            expected: 1,
            found: 0
        }
        .into())
    );
});
