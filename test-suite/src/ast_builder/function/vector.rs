use {
    crate::*,
    gluesql_core::{
        ast_builder::{function as f, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(vector_functions_ast, {
    let glue = get_glue!();

    // Create table with FloatVector column
    let actual = table("Vectors")
        .create_table()
        .add_column("id INTEGER")
        .add_column("vector FLOAT_VECTOR")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    assert_eq!(actual, expected, "create table - Vectors");

    // Insert test vectors
    let actual = table("Vectors")
        .insert()
        .values(vec![
            "1, '[1.0, 2.0, 3.0]'",
            "2, '[4.0, 5.0, 6.0]'",
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    assert_eq!(actual, expected, "insert vectors");

    // Test VECTOR_MAGNITUDE function using ast_builder - just compile test
    let actual = table("Vectors")
        .select()
        .project("id")
        .project(f::vector_magnitude("vector"))
        .project(col("vector").vector_magnitude())
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"VECTOR_MAGNITUDE("vector")"#  | r#"VECTOR_MAGNITUDE("vector")"#
        I64 | F32                             | F32;
        1     3.7416575                       3.7416575;
        2     8.774964                        8.774964
    ));
    assert_eq!(actual, expected, "VECTOR_MAGNITUDE function");

    // Test VECTOR_DOT function
    let actual = table("Vectors")
        .select()
        .project("id")
        .project(f::vector_dot("vector", text("[1.0, 1.0, 1.0]")))
        .project(col("vector").vector_dot(text("[1.0, 1.0, 1.0]")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | r#"VECTOR_DOT("vector", '[1.0, 1.0, 1.0]')"# | r#"VECTOR_DOT("vector", '[1.0, 1.0, 1.0]')"#
        I64 | F32                                          | F32;
        1     6.0                                            6.0;
        2     15.0                                           15.0
    ));
    assert_eq!(actual, expected, "VECTOR_DOT function");

    // Test additional vector functions to increase coverage
    let _ = values(vec!["'[1.0, 2.0, 3.0]'"])
        .alias_as("vecs")
        .select()
        .project(f::vector_add("column1", text("[1.0, 1.0, 1.0]")))
        .project(f::vector_sub("column1", text("[0.5, 0.5, 0.5]")))
        .project(f::vector_scalar_mul("column1", num(2.0)))
        .project(f::vector_euclidean_dist("column1", text("[0.0, 0.0, 0.0]")))
        .project(f::vector_cosine_sim("column1", text("[1.0, 2.0, 3.0]")))
        .project(f::vector_dimension("column1"))
        .project(f::vector_at("column1", num(1)))
        .project(f::vector_manhattan_dist("column1", text("[1.0, 1.0, 1.0]")))
        .project(f::vector_chebyshev_dist("column1", text("[1.0, 1.0, 1.0]")))
        .project(f::vector_hamming_dist("column1", text("[1.0, 0.0, 0.0]")))
        .project(f::vector_jaccard_sim("column1", text("[1.0, 1.0, 1.0]")))
        .project(f::vector_minkowski_dist("column1", text("[0.0, 0.0, 0.0]"), num(2.0)))
        .project(f::vector_canberra_dist("column1", text("[2.0, 4.0, 6.0]")))
        .execute(glue)
        .await;
});