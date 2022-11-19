use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(chr, async move {
    test!(
        "VALUES(CHR(70))",
        Ok(select!(
            column1
            Str;
            "F".to_owned()
        ))
    );
    test!(
        "VALUES(CHR(7070))",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
    );
    run!(
        "
        CREATE TABLE Chr (
            id INTEGER,
            num INTEGER
        );
    "
    );
    run!("INSERT INTO Chr VALUES (1, 70);");

    test!(
        "select chr(num) as chr from Chr;",
        Ok(select!(
            chr
            Str;
            "F".to_owned()
        ))
    );

    test!(
        "select chr(65) as chr from Chr;",
        Ok(select!(
           chr
           Str;
           "A".to_owned()
        ))
    );

    test!(
        "select chr(532) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
    );
    test!(
        "select chr('ukjhg') as chr from Chr;",
        Err(EvaluateError::FunctionRequiresIntegerValue("CHR".to_owned()).into())
    );

    run!("INSERT INTO Chr VALUES (1, 4345);");

    test!(
        "select chr(num) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
    );
});
