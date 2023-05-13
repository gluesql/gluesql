use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(rounding, async move {
    let glue = get_glue!();

    // create table - Number
    let actual = table("Number")
        .create_table()
        .add_column("id INTEGER PRIMARY KEY")
        .add_column("number FLOAT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    // insert into Number
    let actual = table("Number")
        .insert()
        .values(vec!["1, 0.3", "2, -0.8", "3, 10","4, 6.87421"])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(4));
    test(actual, expected);

    // ceil
    let actual = table("Number")
        .select()
        .project("id")
        .project(ceil("number"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "CEIL(\"number\")"
        I64 | F64;
        1     1.0;
        2     0.0;
        3     10.0;
        4     7.0
    ));
    test(actual, expected);

    //floor
    let actual = table("Number")
        .select()
        .project("id")
        .project(floor("number"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "FLOOR(\"number\")"
        I64 | F64;
        1     0.0;
        2     f64::from(-1);
        3     10.0;
        4     6.0
    ));
    test(actual, expected);

    //round
    let actual = table("Number")
        .select()
        .project("id")
        .project(round("number"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "ROUND(\"number\")"
        I64 | F64;
        1     0.0;
        2     f64::from(-1);
        3     10.0;
        4     7.0
    ));
    test(actual, expected);
    

});
