use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(chr, async move {
    run!(
        "
        CREATE TABLE Chr (
            id INTEGER,
            num INTEGER
        );
    "
    );
    run!(r#"INSERT INTO Chr VALUES (1, 70);"#);

    test!(
        r#"select chr(num) as chr from Chr;"#,
        Ok(select!(
            chr
            Str;
            "F".to_owned()
        ))
    );

    test!(
        r#"select chr(65) as chr from Chr;"#,
        Ok(select!(
           chr
           Str;
           "A".to_owned()
        ))
    );

    test!(
        r#"select chr(532) as chr from Chr;"#,
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
    );
    test!(
        r#"select chr("ukjhg") as chr from Chr;"#,
        Err(EvaluateError::FunctionRequiresIntegerValue("CHR".to_owned()).into())
    );

    run!(r#"INSERT INTO Chr VALUES (1, 4345);"#);

    test!(
        r#"select chr(num) as chr from Chr;"#,
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
    );
});
