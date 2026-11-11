use {
    crate::*,
    gluesql_core::{executor::Payload, prelude::Value::*, query_builder::*},
};

test_case!(data_aggregation, {
    let glue = get_glue!();

    let actual = table("User")
        .create_table()
        .add_column("id INT")
        .add_column("name TEXT")
        .add_column("age INT")
        .execute(glue);
    let expected = Ok(Payload::Create);
    test!(actual, expected, "create table");

    let actual = table("User")
        .insert()
        .columns("id, name, age")
        .values(vec![
            vec![num(1), text("Alice"), num(20)],
            vec![num(2), text("Bob"), num(20)],
            vec![num(3), text("Carol"), num(40)],
        ])
        .execute(glue);
    let expected = Ok(Payload::Insert(3));
    test!(actual, expected, "insert");

    let actual = table("User")
        .select()
        .group_by("age")
        .project("age, count(*)")
        .execute(glue);
    let expected = Ok(select!(
        age | r"count(*)";
        I64 | I64;
        20    2;
        40    1
    ));
    test!(actual, expected, "group by");

    let actual = table("User")
        .select()
        .group_by("age")
        .having("count(*) > 1")
        .project("age, count(*)")
        .execute(glue);
    let expected = Ok(select!(
        age | r"count(*)";
        I64 | I64;
        20    2
    ));
    test!(actual, expected, "having");

    let actual = table("User")
        .select()
        .project(col("age").count_distinct().alias_as("unique_ages"))
        .execute(glue);
    let expected = Ok(select!(
        unique_ages
        I64;
        2
    ));
    test!(actual, expected, "Query Builder COUNT DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").sum_distinct().alias_as("sum_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        sum_distinct
        I64;
        60
    ));
    test!(actual, expected, "Query Builder SUM DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").avg_distinct().alias_as("avg_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        avg_distinct
        F64;
        30.0
    ));
    test!(actual, expected, "Query Builder AVG DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").min_distinct().alias_as("min_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        min_distinct
        I64;
        20
    ));
    test!(actual, expected, "Query Builder MIN DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").max_distinct().alias_as("max_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        max_distinct
        I64;
        40
    ));
    test!(actual, expected, "Query Builder MAX DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").variance_distinct().alias_as("variance_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        variance_distinct
        F64;
        100.0
    ));
    test!(actual, expected, "Query Builder VARIANCE DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").stdev_distinct().alias_as("stdev_distinct"))
        .execute(glue);
    let expected = Ok(select!(
        stdev_distinct
        F64;
        10.0
    ));
    test!(actual, expected, "Query Builder STDEV DISTINCT example");
});
