use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
    std::f64::consts::*,
};

test_case!(conversion, {
    let glue = get_glue!();

    // Create table - Number
    let actual = table("Number")
        .create_table()
        .add_column("input INTEGER")
        .add_column("number FLOAT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Number");

    // Insert a row into the Number
    let actual = table("Number")
        .insert()
        .values(vec!["0, 0.0", "90, 90.0", "180, 180.0", "360, 360.0"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into Number");

    // Example Using DEGREES
    let actual = table("Number")
        .select()
        .project("input")
        .project(f::degrees("number"))
        .project(col("number").degrees())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        input   | r#"DEGREES("number")"#    | r#"DEGREES("number")"#
        I64     | F64                       | F64;
        0         0.0                         0.0;
        90        5156.620156177409           5156.620156177409;
        180       10313.240312354817          10313.240312354817;
        360       20626.480624709635          20626.480624709635
    ));
    assert_eq!(actual, expected, "Example Using DEGREES");

    // Example Using RADIANS
    let actual = table("Number")
        .select()
        .project("input")
        .project(f::radians("number"))
        .project(col("number").radians())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        input   | r#"RADIANS("number")"#    | r#"RADIANS("number")"#
        I64     | F64                       | F64;
        0         0.0                         0.0;
        90        FRAC_PI_2                   FRAC_PI_2;
        180       PI                          PI;
        360       TAU                         TAU
    ));
    assert_eq!(actual, expected, "Example Using RADIANS");
});
