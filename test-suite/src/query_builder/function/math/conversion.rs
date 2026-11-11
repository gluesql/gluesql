use {
    crate::*,
    gluesql_core::{
        executor::Payload,
        prelude::Value::*,
        query_builder::{function as f, *},
    },
    std::f64::consts::*,
};

test_case!(conversion, {
    let glue = get_glue!();

    // Create table - Angle
    let actual = table("Angle")
        .create_table()
        .add_column("degrees FLOAT")
        .add_column("radians FLOAT")
        .execute(glue);
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Angle");

    // Insert angles in degrees and radians
    let actual = table("Angle")
        .insert()
        .values(vec!["0, 0.0", "90, PI() / 2", "180, PI()", "360, PI() * 2"])
        .execute(glue);
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into Angle");

    // Example Using DEGREES
    let actual = table("Angle")
        .select()
        .project("radians")
        .project(f::degrees("radians"))
        .project(col("radians").degrees())
        .execute(glue);
    let expected = Ok(select!(
        radians   | r#"DEGREES("radians")"#  | r#"DEGREES("radians")"#
        F64       | F64                      | F64;
        0.0         0.0                        0.0;
        FRAC_PI_2   90.0                       90.0;
        PI          180.0                      180.0;
        TAU         360.0                      360.0
    ));
    assert_eq!(actual, expected, "Example Using DEGREES");

    // Example Using RADIANS
    let actual = table("Angle")
        .select()
        .project("degrees")
        .project(f::radians("degrees"))
        .project(col("degrees").radians())
        .execute(glue);
    let expected = Ok(select!(
        degrees | r#"RADIANS("degrees")"#  | r#"RADIANS("degrees")"#
        F64     | F64                      | F64;
        0.0       0.0                        0.0;
        90.0      FRAC_PI_2                  FRAC_PI_2;
        180.0     PI                         PI;
        360.0     TAU                        TAU
    ));
    assert_eq!(actual, expected, "Example Using RADIANS");
});
