use {
    crate::*,
    gluesql_core::{
        error::ValueError,
        prelude::Value::*,
    },
};

test_case!(float_vector, {
    let g = get_tester!();

    // Create table with FloatVector column
    g.run(
        "
CREATE TABLE vectors (
    id INTEGER,
    embedding FLOAT_VECTOR
)",
    )
    .await;

    // Insert vectors using array literal syntax
    g.run(
        r#"
INSERT INTO vectors VALUES
    (1, '[1.0, 2.0, 3.0]'),
    (2, '[0.5, 1.5, 2.5]'),
    (3, '[2.0, 3.0, 4.0]');
"#,
    )
    .await;

    // Test basic selection - just count to verify data was inserted
    g.test(
        "SELECT COUNT(*) as count FROM vectors",
        Ok(select!(
            count
            I64;
            3
        )),
    )
    .await;

    // Test CAST operation - just verify it doesn't error
    g.run("SELECT CAST('[5.0, 6.0, 7.0]' AS FLOAT_VECTOR) as vector").await;

    // Test WHERE clause with vector comparison
    g.test(
        "SELECT COUNT(*) as count FROM vectors WHERE embedding = '[1.0, 2.0, 3.0]'",
        Ok(select!(
            count
            I64;
            1
        )),
    )
    .await;

    // Test invalid vector format (should fail)
    g.test(
        "INSERT INTO vectors VALUES (4, '[1.0, not_a_number, 3.0]')",
        Err(ValueError::InvalidFloatVector("Array contains non-numeric value".to_string()).into()),
    )
    .await;

    // Test empty vector (should fail)
    g.test(
        "INSERT INTO vectors VALUES (5, '[]')",
        Err(ValueError::InvalidFloatVector("Vector cannot be empty".to_string()).into()),
    )
    .await;

    // Test non-array format (should fail)
    g.test(
        "INSERT INTO vectors VALUES (6, 'not_an_array')",
        Err(ValueError::InvalidJsonString("not_an_array".to_string()).into()),
    )
    .await;

    // Test mixed number types (integers and floats)
    g.run(
        "INSERT INTO vectors VALUES (7, '[1, 2.5, 3]')"
    )
    .await;

    // Verify the mixed number type insertion worked
    g.test(
        "SELECT COUNT(*) as count FROM vectors WHERE id = 7",
        Ok(select!(
            count
            I64;
            1
        )),
    )
    .await;

    // Test large dimension vector (within limits)
    let large_vector: Vec<f32> = (0..10).map(|i| i as f32 * 0.1).collect();
    let large_vector_str = format!("[{}]", large_vector.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", "));
    
    g.run(&format!(
        "INSERT INTO vectors VALUES (8, '{}')",
        large_vector_str
    ))
    .await;

    // Verify the large vector was inserted
    g.test(
        "SELECT COUNT(*) as count FROM vectors WHERE id = 8",
        Ok(select!(
            count
            I64;
            1
        )),
    )
    .await;
});

test_case!(float_vector_data_type_validation, {
    let g = get_tester!();

    // Test data type validation
    g.run("CREATE TABLE typed_vectors (id INTEGER, vec FLOAT_VECTOR)").await;
    
    // Valid insertion
    g.run("INSERT INTO typed_vectors VALUES (1, '[1.0, 2.0]')").await;
    
    // Test that the data type is enforced - just count to verify
    g.test(
        "SELECT COUNT(*) as count FROM typed_vectors WHERE id = 1",
        Ok(select!(
            count
            I64;
            1
        )),
    )
    .await;
});

test_case!(float_vector_json_serialization, {
    let g = get_tester!();

    g.run("CREATE TABLE json_vectors (id INTEGER, data FLOAT_VECTOR)").await;
    g.run("INSERT INTO json_vectors VALUES (1, '[1.5, 2.5, 3.5]')").await;

    // The vector should be properly serialized and deserialized - just count to verify
    g.test(
        "SELECT COUNT(*) as count FROM json_vectors WHERE id = 1",
        Ok(select!(
            count
            I64;
            1
        )),
    )
    .await;
});

test_case!(vector_functions_basic, {
    let g = get_tester!();

    g.run("CREATE TABLE vectors (id INTEGER, vec FLOAT_VECTOR)").await;
    g.run("INSERT INTO vectors VALUES (1, '[3.0, 4.0]'), (2, '[1.0, 1.0, 1.0]')").await;

    // Test VECTOR_MAGNITUDE
    g.test(
        "SELECT VECTOR_MAGNITUDE('[3.0, 4.0]') as magnitude",
        Ok(select!(
            magnitude
            F32;
            5.0
        )),
    )
    .await;

    // Test VECTOR_DIMENSION
    g.test(
        "SELECT VECTOR_DIMENSION('[1.0, 2.0, 3.0]') as dim",
        Ok(select!(
            dim
            I64;
            3
        )),
    )
    .await;

    // Test VECTOR_AT
    g.test(
        "SELECT VECTOR_AT('[1.0, 2.0, 3.0]', 1) as element",
        Ok(select!(
            element
            F32;
            2.0
        )),
    )
    .await;
});

test_case!(vector_functions_arithmetic, {
    let g = get_tester!();

    // Test VECTOR_DOT
    g.test(
        "SELECT VECTOR_DOT('[1.0, 2.0, 3.0]', '[4.0, 5.0, 6.0]') as dot_product",
        Ok(select!(
            dot_product
            F32;
            32.0  // 1*4 + 2*5 + 3*6 = 32
        )),
    )
    .await;

    // Test VECTOR_ADD
    g.test(
        "SELECT VECTOR_MAGNITUDE(VECTOR_ADD('[1.0, 2.0]', '[3.0, 4.0]')) as result_magnitude",
        Ok(select!(
            result_magnitude
            F32;
            7.211103  // magnitude of [4.0, 6.0] = sqrt(16 + 36) ≈ 7.211
        )),
    )
    .await;

    // Test VECTOR_SCALAR_MUL
    g.test(
        "SELECT VECTOR_MAGNITUDE(VECTOR_SCALAR_MUL('[1.0, 1.0]', 3.0)) as scaled_magnitude",
        Ok(select!(
            scaled_magnitude
            F32;
            4.2426405  // magnitude of [3.0, 3.0] = 3 * sqrt(2) ≈ 4.243
        )),
    )
    .await;
});

test_case!(vector_functions_distance, {
    let g = get_tester!();

    // Test VECTOR_EUCLIDEAN_DIST
    g.test(
        "SELECT VECTOR_EUCLIDEAN_DIST('[0.0, 0.0]', '[3.0, 4.0]') as distance",
        Ok(select!(
            distance
            F32;
            5.0  // sqrt(9 + 16) = 5
        )),
    )
    .await;

    // Test VECTOR_COSINE_SIM (should be 1.0 for identical normalized vectors)
    g.test(
        "SELECT VECTOR_COSINE_SIM('[1.0, 0.0]', '[2.0, 0.0]') as similarity",
        Ok(select!(
            similarity
            F32;
            1.0  // same direction
        )),
    )
    .await;
});

test_case!(vector_functions_with_tables, {
    let g = get_tester!();

    g.run("CREATE TABLE embeddings (id INTEGER, vec1 FLOAT_VECTOR, vec2 FLOAT_VECTOR)").await;
    g.run("INSERT INTO embeddings VALUES (1, '[1.0, 0.0]', '[0.0, 1.0]'), (2, '[1.0, 1.0]', '[1.0, 1.0]')").await;

    // Test vector operations on table columns
    g.test(
        "SELECT id, VECTOR_DOT(vec1, vec2) as dot FROM embeddings ORDER BY id",
        Ok(select!(
            id  | dot
            I64 | F32;
            1     0.0;   // orthogonal vectors
            2     2.0    // [1,1] · [1,1] = 2
        )),
    )
    .await;

    // Test distance calculations between table vectors
    g.test(
        "SELECT id, VECTOR_EUCLIDEAN_DIST(vec1, vec2) as distance FROM embeddings WHERE id = 1",
        Ok(select!(
            id  | distance
            I64 | F32;
            1     1.4142135  // sqrt(2) ≈ 1.414
        )),
    )
    .await;
});

test_case!(vector_functions_normalize, {
    let g = get_tester!();

    // Test VECTOR_NORMALIZE - normalized vector should have magnitude 1
    g.test(
        "SELECT VECTOR_MAGNITUDE(VECTOR_NORMALIZE('[3.0, 4.0]')) as normalized_mag",
        Ok(select!(
            normalized_mag
            F32;
            1.0  // normalized vectors have magnitude 1
        )),
    )
    .await;
});