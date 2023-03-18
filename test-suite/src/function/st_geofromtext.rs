use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::Point, prelude::Value::*},
};

test_case!(st_geofromtext, async move {
    let test_cases = [
        (
            r#"SELECT ST_GEOFROMTEXT('POINT(-71.064544 42.28787)') AS pt"#,
            Ok(select!(
                pt
                Point;
                gluesql_core::data::Point::new(-71.064544, 42.28787)

            )),
        ),
        (
            r#"SELECT ST_GEOFROMTEXT('POINT(-71.06454t4 42.28787)') AS pt"#,
            Err(ValueError::FailedToParsePoint(
                Str("POINT(-71.06454t4 42.28787)".to_owned()).into(),
            )
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
