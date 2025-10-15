use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(rounding, {
    let glue = get_glue!();

    // create table - Number
    let actual = table("Number")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("number FLOAT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Number");

    // insert into Number
    let actual = table("Number")
        .insert()
        .values(vec!["1, 0.3", "2, -0.8", "3, 10", "4, 6.87421"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    assert_eq!(actual, expected, "insert into Number");

    // ceil
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::ceil("number"))
        .project(col("number").ceil())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "CEIL(\"number\")" | "CEIL(\"number\")"
        I64 | F64                | F64;
        1     1.0                  1.0;
        2     0.0                  0.0;
        3     10.0                 10.0;
        4     7.0                  7.0
    ));
    assert_eq!(actual, expected, "ceil");

    //floor
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::floor("number"))
        .project(col("number").floor())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "FLOOR(\"number\")" | "FLOOR(\"number\")"
        I64 | F64                 | F64;
        1     0.0                   0.0;
        2     f64::from(-1)         f64::from(-1);
        3     10.0                  10.0;
        4     6.0                   6.0
    ));
    assert_eq!(actual, expected, "floor");

    //round
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::round("number", None))
        .project(col("number").round())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "ROUND(\"number\")" | "ROUND(\"number\")"
        I64 | F64                 | F64;
        1     0.0                   0.0;
        2     f64::from(-1)         f64::from(-1);
        3     10.0                  10.0;
        4     7.0                   7.0
    ));
    assert_eq!(actual, expected, "round");

    // round with precision
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::round("number", Some(num(2))))
        .project(col("number").round_with_precision(num(2)))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "ROUND(\"number\", 2)" | "ROUND(\"number\", 2)"
        I64 | F64                    | F64;
        1     0.3                      0.3;
        2     f64::from(-0.8)          f64::from(-0.8);
        3     10.0                     10.0;
        4     6.87                     6.87
    ));
    assert_eq!(actual, expected, "round with precision");

    //trunc
    let actual = table("Number")
        .select()
        .project("id")
        .project(f::trunc("number"))
        .project(col("number").trunc())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "TRUNC(\"number\")" | "TRUNC(\"number\")"
        I64 | F64                 | F64;
        1     0.0                   0.0;
        2     0.0                   0.0;
        3     10.0                  10.0;
        4     6.0                   6.0
    ));
    assert_eq!(actual, expected, "trunc");
});
