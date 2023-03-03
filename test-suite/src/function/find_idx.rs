use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(find_idx, async move {
    let test_cases = [
        ("CREATE TABLE Meal (menu Text null)", Ok(Payload::Create)),
        ("INSERT INTO Meal VALUES ('pork')", Ok(Payload::Insert(1))),
        ("INSERT INTO Meal VALUES ('burger')", Ok(Payload::Insert(1))),
        (
            "SELECT FIND_IDX(menu, 'rg') AS test FROM Meal",
            Ok(select!(test; I64; 0; 3)),
        ),
        (
            "SELECT FIND_IDX(menu, 'r', 4) AS test FROM Meal",
            Ok(select!(test; I64; 0; 6)),
        ),
        (
            "SELECT FIND_IDX('cheese', '') AS test",
            Ok(select!(test; I64; 0)),
        ),
        (
            "SELECT FIND_IDX('cheese', 's') AS test",
            Ok(select!(test; I64; 5)),
        ),
        (
            "SELECT FIND_IDX('cheese burger', 'e', 5) AS test",
            Ok(select!(test; I64; 6)),
        ),
        (
            "SELECT FIND_IDX('cheese', NULL) AS test",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT FIND_IDX('cheese', 1) AS test",
            Err(EvaluateError::FunctionRequiresStringValue(String::from("FIND_IDX")).into()),
        ),
        (
            "SELECT FIND_IDX('cheese', 's', '5') AS test",
            Err(EvaluateError::FunctionRequiresIntegerValue(String::from("FIND_IDX")).into()),
        ),
        (
            "SELECT FIND_IDX('cheese', 's', -1) AS test",
            Err(ValueError::NonPositiveIntegerOffsetInFindIdx(String::from("-1")).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
