use gluesql_core::executor::EvaluateError;

use {crate::*, gluesql_core::prelude::Value::*};

test_case!(st_y, async move {
    let test_cases = [
        (
            r#"SELECT ST_Y(ST_GEOFROMTEXT('POINT(-71.064544 42.28787)')) AS ptx"#,
            Ok(select!(
                ptx
                F64;
                42.28787
            )),
        ),
        (
            r#"SELECT ST_Y('cheese') AS ptx"#,
            Err(EvaluateError::FunctionRequiresPointValue("ST_Y".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
