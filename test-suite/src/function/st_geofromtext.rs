use {
    crate::*,
    gluesql_core::{data::ValueError, executor::Payload, prelude::Value::Point, prelude::Value::*},
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
        (
            "CREATE TABLE POINT (point_field POINT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO POINT VALUES (ST_GEOFROMTEXT('POINT(-71.064544 42.28787)'))"#,
            Ok(Payload::Insert(1)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
