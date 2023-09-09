use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(basic_arithmetic, {
    let glue = get_glue!();

    // Create table - Number
    let actual = table("Number")
        .create_table()
        .add_column("id INTEGER")
        .add_column("number INTEGER")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Number");

    // Insert a row into the Number
    let actual = table("Number")
        .insert()
        .values(vec!["0, 0", "1, 3", "2, 4", "3, 29"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into Number");

    // Example Using ABS
    let actual = values(vec!["0, 0", "1, -3", "2, 4", "3, -29"])
        .alias_as("number")
        .select()
        .project("column1")
        .project(f::abs("column2"))
        .project(col("column2").abs())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        column1 | r#"ABS("column2")"#   | r#"ABS("column2")"#
        I64     | I64                   | I64;
        0         0                       0;
        1         3                       3;
        2         4                       4;
        3         29                      29
    ));
    assert_eq!(actual, expected, "Example Using ABS");

    //Example Using DIV
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::divide("number", 3))
        .project(f::divide(col("number"), 3))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"DIV("number", 3)"# | r#"DIV("number", 3)"#
        I64 | I64                   | I64;
        0     0                       0;
        1     1                       1;
        2     1                       1;
        3     9                       9
    ));
    assert_eq!(actual, expected, "Example Using DIV");

    //Example Using MOD
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::modulo("number", 4))
        .project(f::modulo(col("number"), 4))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"MOD("number", 4)"# | r#"MOD("number", 4)"#
        I64 | I64                   | I64;
        0     0                       0;
        1     3                       3;
        2     0                       0;
        3     1                       1
    ));
    assert_eq!(actual, expected, "Example Using MOD");

    //Example Using GCD
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::gcd("number", 12))
        .project(f::gcd(col("number"), 12))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"GCD("number", 12)"# | r#"GCD("number", 12)"#
        I64 | I64                   | I64;
        0     12                      12;
        1     3                       3;
        2     4                       4;
        3     1                       1
    ));
    assert_eq!(actual, expected, "Example Using GCD");

    //Example Using LCM
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::lcm("number", 3))
        .project(f::lcm(col("number"), 3))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"LCM("number", 3)"# | r#"LCM("number", 3)"#
        I64 | I64                   | I64;
        0     0                       0;
        1     3                       3;
        2     12                      12;
        3     87                      87
    ));
    assert_eq!(actual, expected, "Example Using LCM");
});
