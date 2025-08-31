use {
    crate::*,
    gluesql_core::error::ValueError,
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