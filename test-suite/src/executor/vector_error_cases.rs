use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::Value::*,
    },
};

test_case!(vector_function_error_cases, {
    let g = get_tester!();

    // Test eval_to_float_vector error cases
    
    // 1. Invalid type conversion
    g.test(
        "SELECT VECTOR_MAGNITUDE(123)",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE".to_owned()).into()),
    )
    .await;

    // 2. Invalid JSON format
    g.test(
        "SELECT VECTOR_MAGNITUDE('not_json')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE: Invalid vector format 'not_json'".to_owned()).into()),
    )
    .await;

    // 3. Invalid array format
    g.test(
        "SELECT VECTOR_MAGNITUDE('[1.0, invalid, 3.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE: Invalid vector format '[1.0, invalid, 3.0]'".to_owned()).into()),
    )
    .await;

    // 4. Empty vector
    g.test(
        "SELECT VECTOR_MAGNITUDE('[]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE: Invalid vector format '[]'".to_owned()).into()),
    )
    .await;

    // Test vector operation error cases

    // 5. Dimension mismatch in dot product
    g.test(
        "SELECT VECTOR_DOT('[1.0, 2.0]', '[1.0, 2.0, 3.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_DOT: Dimension mismatch: expected 2, got 3".to_owned()).into()),
    )
    .await;

    // 6. Dimension mismatch in addition
    g.test(
        "SELECT VECTOR_ADD('[1.0, 2.0]', '[1.0, 2.0, 3.0, 4.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_ADD: Dimension mismatch: expected 2, got 4".to_owned()).into()),
    )
    .await;

    // 7. Dimension mismatch in subtraction
    g.test(
        "SELECT VECTOR_SUB('[1.0]', '[1.0, 2.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_SUB: Dimension mismatch: expected 1, got 2".to_owned()).into()),
    )
    .await;

    // 8. Zero magnitude normalization error
    g.test(
        "SELECT VECTOR_NORMALIZE('[0.0, 0.0, 0.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_NORMALIZE: Invalid float value: Cannot normalize zero vector".to_owned()).into()),
    )
    .await;

    // 9. Negative index access (error case)
    g.test(
        "SELECT VECTOR_AT('[1.0, 2.0]', -1)",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_AT: Index cannot be negative".to_owned()).into()),
    )
    .await;

    // 10. Distance calculation with dimension mismatch
    g.test(
        "SELECT VECTOR_EUCLIDEAN_DIST('[1.0, 2.0]', '[1.0, 2.0, 3.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_EUCLIDEAN_DIST: Dimension mismatch: expected 2, got 3".to_owned()).into()),
    )
    .await;

    // 11. Cosine similarity with zero vector
    g.test(
        "SELECT VECTOR_COSINE_SIM('[0.0, 0.0]', '[1.0, 2.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_COSINE_SIM: Invalid float value: Cannot compute cosine similarity with zero vector".to_owned()).into()),
    )
    .await;

    // 12. Advanced distance functions with dimension mismatch
    g.test(
        "SELECT VECTOR_MANHATTAN_DIST('[1.0, 2.0]', '[1.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MANHATTAN_DIST: Dimension mismatch: expected 2, got 1".to_owned()).into()),
    )
    .await;

    // 13. Minkowski distance with invalid p value
    g.test(
        "SELECT VECTOR_MINKOWSKI_DIST('[1.0, 2.0]', '[3.0, 4.0]', 0.0)",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MINKOWSKI_DIST: Invalid float value: Invalid p parameter for Minkowski distance: 0".to_owned()).into()),
    )
    .await;

    // 14. Minkowski distance with negative p
    g.test(
        "SELECT VECTOR_MINKOWSKI_DIST('[1.0, 2.0]', '[3.0, 4.0]', -1.5)",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MINKOWSKI_DIST: Invalid float value: Invalid p parameter for Minkowski distance: -1.5".to_owned()).into()),
    )
    .await;
});

test_case!(vector_function_edge_cases, {
    let g = get_tester!();

    // Test edge cases that should work but might cause issues

    // 1. Single element vector
    g.test(
        "SELECT VECTOR_MAGNITUDE('[5.0]')",
        Ok(select!(
            r#"VECTOR_MAGNITUDE('[5.0]')"#
            F32;
            5.0
        )),
    )
    .await;

    // 2. Very small numbers close to zero
    g.test(
        "SELECT VECTOR_MAGNITUDE('[1e-10, 1e-10]')",
        Ok(select!(
            r#"VECTOR_MAGNITUDE('[1e-10, 1e-10]')"#
            F32;
            1.4142136e-10
        )),
    )
    .await;

    // 3. Large vectors (testing dimension limits)
    let large_vector: Vec<f32> = (0..100).map(|i| i as f32 * 0.01).collect();
    let large_vector_str = format!(
        "[{}]",
        large_vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    
    g.run(&format!(
        "SELECT VECTOR_DIMENSION('{}') as dim",
        large_vector_str
    ))
    .await;

    // 4. Vector with all same values
    g.test(
        "SELECT VECTOR_MAGNITUDE('[2.0, 2.0, 2.0, 2.0]')",
        Ok(select!(
            r#"VECTOR_MAGNITUDE('[2.0, 2.0, 2.0, 2.0]')"#
            F32;
            4.0  // sqrt(4 * 4) = 4
        )),
    )
    .await;

    // 5. Mixed positive and negative values
    g.test(
        "SELECT VECTOR_DOT('[-1.0, 2.0, -3.0]', '[1.0, -2.0, 3.0]')",
        Ok(select!(
            r#"VECTOR_DOT('[-1.0, 2.0, -3.0]', '[1.0, -2.0, 3.0]')"#
            F32;
            -14.0  // -1*1 + 2*(-2) + (-3)*3 = -1 - 4 - 9 = -14
        )),
    )
    .await;

    // 6. Test boundary index access
    g.test(
        "SELECT VECTOR_AT('[1.0, 2.0, 3.0]', 2)",  // 0-based indexing, so index 2 is the last element
        Ok(select!(
            r#"VECTOR_AT('[1.0, 2.0, 3.0]', 2)"#
            F32;
            3.0
        )),
    )
    .await;
});

test_case!(vector_function_null_handling, {
    let g = get_tester!();

    g.run("CREATE TABLE null_vectors (id INTEGER, vec1 FLOAT_VECTOR, vec2 FLOAT_VECTOR)")
        .await;
    
    g.run("INSERT INTO null_vectors VALUES (1, '[1.0, 2.0]', NULL), (2, NULL, '[3.0, 4.0]'), (3, NULL, NULL)")
        .await;

    // Test NULL handling - should return NULL when any operand is NULL
    g.test(
        "SELECT id, VECTOR_DOT(vec1, vec2) as result FROM null_vectors ORDER BY id",
        Ok(select_with_null!(
            id     | result;
            I64(1)   Null;
            I64(2)   Null;
            I64(3)   Null
        )),
    )
    .await;

    // Test single NULL operand
    g.test(
        "SELECT VECTOR_MAGNITUDE(NULL) as result",
        Ok(select_with_null!(result; Null)),
    )
    .await;
});

test_case!(vector_function_type_coercion_errors, {
    let g = get_tester!();

    g.run("CREATE TABLE mixed_types (id INTEGER, num INTEGER, text TEXT, bool BOOLEAN)")
        .await;
    
    g.run("INSERT INTO mixed_types VALUES (1, 42, 'hello', TRUE)")
        .await;

    // Test that vector functions reject non-vector types
    g.test(
        "SELECT VECTOR_MAGNITUDE(num) FROM mixed_types",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT VECTOR_DOT(text, '[1.0, 2.0]') FROM mixed_types",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_DOT: Invalid vector format 'hello'".to_owned()).into()),
    )
    .await;

    g.test(
        "SELECT VECTOR_ADD(bool, '[1.0, 2.0]') FROM mixed_types",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_ADD".to_owned()).into()),
    )
    .await;
});

test_case!(vector_function_complex_error_scenarios, {
    let g = get_tester!();

    // Test error handling in complex expressions
    g.test(
        "SELECT VECTOR_MAGNITUDE(VECTOR_ADD('[1.0, 2.0]', '[1.0, 2.0, 3.0]'))",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_ADD: Dimension mismatch: expected 2, got 3".to_owned()).into()),
    )
    .await;

    // Test error in WHERE clause
    g.test(
        "SELECT 1 WHERE VECTOR_MAGNITUDE('[invalid]') > 0",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_MAGNITUDE: Invalid vector format '[invalid]'".to_owned()).into()),
    )
    .await;

    // Test error in ORDER BY clause
    g.test(
        "SELECT 1 as val ORDER BY VECTOR_DOT('[1.0]', '[1.0, 2.0]')",
        Err(EvaluateError::FunctionRequiresFloatVectorValue("VECTOR_DOT: Dimension mismatch: expected 1, got 2".to_owned()).into()),
    )
    .await;
});