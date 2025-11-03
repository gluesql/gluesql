use {
    crate::*,
    gluesql_core::{ast_builder::*, executor::Payload, prelude::Value::*},
};

test_case!(data_aggregation, {
    let glue = get_glue!();

    let actual = table("User")
        .create_table()
        .add_column("id INT")
        .add_column("name TEXT")
        .add_column("age INT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table");

    let actual = table("User")
        .insert()
        .columns("id, name, age")
        .values(vec![
            vec![num(1), text("Alice"), num(20)],
            vec![num(2), text("Bob"), num(30)],
            vec![num(3), text("Carol"), num(30)],
            vec![num(4), text("Dave"), num(50)],
            vec![num(5), text("Eve"), num(50)],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(5));
    assert_eq!(actual, expected, "insert");

    let actual = table("User")
        .select()
        .group_by("age")
        .project("age, count(*)")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        age | r"count(*)";
        I64 | I64;
        20    1;
        30    2;
        50    2
    ));
    assert_eq!(actual, expected, "group by");

    let actual = table("User")
        .select()
        .group_by("age")
        .having("count(*) > 1")
        .project("age, count(*)")
        .execute(glue)
        .await;
    let expected = Ok(select!(
        age | r"count(*)";
        I64 | I64;
        30    2;
        50    2
    ));
    assert_eq!(actual, expected, "having");

    let actual = table("User")
        .select()
        .project(col("age").count_distinct().alias_as("unique_ages"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        unique_ages
        I64;
        3
    ));
    assert_eq!(actual, expected, "AST Builder COUNT DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").sum_distinct().alias_as("sum_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        sum_distinct
        I64;
        100
    ));
    assert_eq!(actual, expected, "AST Builder SUM DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").avg_distinct().alias_as("avg_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        avg_distinct
        F64;
        33.333333333333336
    ));
    assert_eq!(actual, expected, "AST Builder AVG DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").min_distinct().alias_as("min_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        min_distinct
        I64;
        20
    ));
    assert_eq!(actual, expected, "AST Builder MIN DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").max_distinct().alias_as("max_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        max_distinct
        I64;
        50
    ));
    assert_eq!(actual, expected, "AST Builder MAX DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").variance_distinct().alias_as("variance_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        variance_distinct
        F64;
        155.55555555555554
    ));
    assert_eq!(actual, expected, "AST Builder VARIANCE DISTINCT example");

    let actual = table("User")
        .select()
        .project(col("age").stdev_distinct().alias_as("stdev_distinct"))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        stdev_distinct
        F64;
        12.47219128924647
    ));
    assert_eq!(actual, expected, "AST Builder STDEV DISTINCT example");
});
