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
    );
    g.test(
        "VALUES(CHR(7070))",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    );
    g.run(
        "
        CREATE TABLE Chr (
            id INTEGER,
            num INTEGER
        );
    ",
    );
    g.run("INSERT INTO Chr VALUES (1, 70);");

    g.test(
        "select chr(num) as chr from Chr;",
        Ok(select!(
            chr
            Str;
            "F".to_owned()
        )),
    );

    g.test(
        "select chr(65) as chr from Chr;",
        Ok(select!(
           chr
           Str;
           "A".to_owned()
        )),
    );

    g.test(
        "select chr(532) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    );
    g.test(
        "select chr('ukjhg') as chr from Chr;",
        Err(EvaluateError::FunctionRequiresIntegerValue("CHR".to_owned()).into()),
    );

    g.run("INSERT INTO Chr VALUES (1, 4345);");

    g.test(
        "select chr(num) as chr from Chr;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    );
});
