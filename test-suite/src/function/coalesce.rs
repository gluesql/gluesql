use {
    crate::*,
    gluesql_core::{error::EvaluateError, prelude::Value::*},
};

test_case!(coalesce, {
    let g = get_tester!();

    g.test(
        // COALESCE does not allow no arguments
        "SELECT COALESCE() AS coalesce",
        Err(EvaluateError::FunctionRequiresMoreArguments {
            function_name: "COALESCE".to_owned(),
            required_minimum: 1,
            found: 0,
        }
        .into()),
    )
    .await;

    g.test(
        "SELECT COALESCE(NULL) AS coalesce",
        Ok(select_with_null!(
            coalesce;
            Null
        )),
    )
    .await;

    g.test(
        "SELECT COALESCE(NULL, 42) AS coalesce",
        Ok(select!(
            coalesce
            I64;
            42
        )),
    )
    .await;

    g.test(
        // Test subqueries in COALESCE
        "SELECT COALESCE((SELECT NULL), (SELECT 42)) as coalesce",
        Ok(select!(
            coalesce
            I64;
            42
        )),
    )
    .await;

    g.test(
        // Test nested COALESCE
        "SELECT COALESCE(
            COALESCE(NULL),
            COALESCE(NULL, 'Answer to the Ultimate Question of Life')
        ) as coalesce",
        Ok(select!(
            coalesce
            Str;
            "Answer to the Ultimate Question of Life".to_owned()
        )),
    )
    .await;

    g.test(
        // Test COALESCE with non-NULL value as first argument
        "SELECT COALESCE('Hitchhiker', NULL) AS coalesce",
        Ok(select!(
            coalesce
            Str;
            "Hitchhiker".to_owned()
        )),
    )
    .await;

    g.test(
        // Test COALESCE with all NULL arguments
        "SELECT COALESCE(NULL, NULL, NULL) AS coalesce",
        Ok(select_with_null!(
            coalesce;
            Null
        )),
    )
    .await;

    g.test(
        // Test COALESCE with integer arguments
        "SELECT COALESCE(NULL, 42, 84) AS coalesce",
        Ok(select!(
            coalesce
            I64;
            42
        )),
    )
    .await;

    g.test(
        // Test COALESCE with float arguments
        "SELECT COALESCE(NULL, 1.23, 4.56) AS coalesce",
        Ok(select!(
            coalesce
            F64;
            1.23
        )),
    )
    .await;

    g.test(
        // Test COALESCE with boolean arguments
        "SELECT COALESCE(NULL, TRUE, FALSE) AS coalesce",
        Ok(select!(
            coalesce
            Bool;
            true
        )),
    )
    .await;

    g.test(
        // Test invalid expression in COALESCE
        "SELECT COALESCE(NULL, COALESCE());",
        Err(EvaluateError::FunctionRequiresMoreArguments {
            function_name: "COALESCE".to_owned(),
            required_minimum: 1,
            found: 0,
        }
        .into()),
    )
    .await;

    g.run(
        "
        CREATE TABLE TestCoalesce (
            id INTEGER,
            text_value TEXT NULL,
            integer_value INTEGER NULL,
            float_value FLOAT NULL,
            boolean_value BOOLEAN NULL
        );",
    )
    .await;
    g.run(
        "
        INSERT INTO TestCoalesce (id, text_value, integer_value, float_value, boolean_value) VALUES 
            (1, 'Hitchhiker', NULL, NULL, NULL),
            (2, NULL, 42, NULL, NULL),
            (3, NULL, NULL, 1.11, NULL),
            (4, NULL, NULL, NULL, TRUE),
            (5, 'Universe', 84, 2.22, FALSE);
        ",
    )
    .await;

    g.test(
        // Test COALESCE with table column values and different types of default values
        "SELECT
            id,
            COALESCE(text_value, 'Default') AS coalesce_text,
            COALESCE(integer_value, 0) AS coalesce_integer,
            COALESCE(float_value, 0.1) AS coalesce_float,
            COALESCE(boolean_value, FALSE) AS coalesce_boolean
        FROM TestCoalesce
        ORDER BY id ASC",
        Ok(select!(
            id  | coalesce_text                | coalesce_integer | coalesce_float | coalesce_boolean
            I64 | Str                          | I64              | F64            | Bool;
            1     "Hitchhiker".to_owned()        0                  0.1              false;
            2     "Default".to_owned()           42                 0.1              false;
            3     "Default".to_owned()           0                  1.11             false;
            4     "Default".to_owned()           0                  0.1              true;
            5     "Universe".to_owned()          84                 2.22             false
        ))
    ).await;

    g.test(
        // Test COALESCE with table column values - multiple columns
        "SELECT id, COALESCE(text_value, integer_value, float_value, boolean_value) AS coalesce FROM TestCoalesce ORDER BY id ASC",
        Ok(select_with_null!(
            id     | coalesce;
            I64(1)   Str("Hitchhiker".to_owned());
            I64(2)   I64(42);
            I64(3)   F64(1.11);
            I64(4)   Bool(true);
            I64(5)   Str("Universe".to_owned())
        ))
    ).await;
});
