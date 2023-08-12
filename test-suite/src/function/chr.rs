use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(chr, {
    let g = get_tester!();

    g.test(
        "VALUES(CHR(70))",
        Ok(select!(
            column1
            Str;
            "F".to_owned()
        )),
    )
    .await;
    g.test(
        "VALUES(CHR(7070))",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    )
    .await;
    g.run(
        "
        CREATE TABLE Chr (
            id INTEGER,
            num INTEGER
        );
    ",
    )
    .await;
    g.run("INSERT INTO Chr VALUES (1, 70);").await;

    g.test(
        "select chr(num) as chr from Chr;",
        Ok(select!(
            chr
            Str;
            "F".to_owned()
        )),
    )
    .await;

    g.test(
        "select chr(65) as chr from Chr;",
        Ok(select!(
           chr
           Str;
           "A".to_owned()
        )),
    )
    .await;

    g.test(
        "select chr(532) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    )
    .await;
    g.test(
        "select chr('ukjhg') as chr from Chr;",
        Err(EvaluateError::FunctionRequiresIntegerValue("CHR".to_owned()).into()),
    )
    .await;

    g.run("INSERT INTO Chr VALUES (1, 4345);").await;

    g.test(
        "select chr(num) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    )
    .await;
});
