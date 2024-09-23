//! Test suite to evaluate the functionality of CHECK constraints, both at the column and table level.
use {crate::*, gluesql_core::error::ValidateError, gluesql_core::prelude::Payload};

test_case!(check_constraint, {
    let g = get_tester!();

    g.run("CREATE TABLE T (a INT CHECK (a < 60), b INT, c BOOLEAN, CHECK (a > 0 AND b > 0))")
        .await;

    g.named_test(
        "Check constraint should allow inserting rows that satisfy the constraint",
        "INSERT INTO T (a, b) VALUES (1, 5)",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.named_test(
        "Check constraint should not allow inserting rows that do not satisfy the constraint",
        "INSERT INTO T (a, b) VALUES (0, 1)",
        Err(ValidateError::CheckConstraintViolation("\"a\" > 0 AND \"b\" > 0".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Check constraint should allow inserting rows that satisfy the constraint",
        "INSERT INTO T (a, b) VALUES (59, 1)",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.named_test(
        "Check constraint should not allow inserting rows that do not satisfy the constraint",
        "INSERT INTO T (a, b) VALUES (60, 1)",
        Err(ValidateError::CheckConstraintViolation("\"a\" < 60".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Check constraint should allow updating rows that satisfy the constraint",
        "UPDATE T SET a = 5 WHERE a = 1",
        Ok(Payload::Update(1)),
    )
    .await;

    g.named_test(
        "Check constraint should not allow updating rows to violate the constraint",
        "UPDATE T SET a = 0 WHERE a = 5",
        Err(ValidateError::CheckConstraintViolation("\"a\" > 0 AND \"b\" > 0".to_owned()).into()),
    )
    .await;
});
