use gluesql_core::executor::EvaluateError;

use {crate::*, gluesql_core::prelude::Value::*};

test_case!(st_x, async move {
    let test_cases = [
        (
            r#"SELECT ST_X(ST_GEOFROMTEXT('POINT(-71.064544 42.28787)')) AS ptx"#,
            Ok(select!(
                ptx
                F64;
                -71.064544

            )),
        ),
        (
            r#"SELECT ST_X('cheese') AS ptx"#,
            Err(EvaluateError::FunctionRequiresPointValue("ST_X".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
