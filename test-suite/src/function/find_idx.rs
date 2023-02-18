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
            "SELECT FIND_IDX('rg', menu) AS test FROM Meal",
            Ok(select!(test; I64; 0; 3)),
        ),
        (
            "SELECT FIND_IDX('r', menu, 4) AS test FROM Meal",
            Ok(select!(test; I64; 0; 6)),
        ),
        (
            "SELECT FIND_IDX('', 'cheese') AS test",
            Ok(select!(test; I64; 0)),
        ),
        (
            "SELECT FIND_IDX('s', 'cheese') AS test",
            Ok(select!(test; I64; 5)),
        ),
        (
            "SELECT FIND_IDX('e', 'cheese burger', 5) AS test",
            Ok(select!(test; I64; 6)),
        ),
        (
            "SELECT FIND_IDX(NULL, 'cheese') AS test",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT FIND_IDX(1, 'cheese') AS test",
            Err(EvaluateError::FunctionRequiresStringValue(String::from("FIND_IDX")).into()),
        ),
        (
            "SELECT FIND_IDX('s', 'cheese', '5') AS test",
            Err(EvaluateError::FunctionRequiresIntegerValue(String::from("FIND_IDX")).into()),
        ),
        (
            "SELECT FIND_IDX('s', 'cheese', -1) AS test",
            Err(ValueError::NonPositiveIntegerOffsetInFindIdx(String::from("-1")).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
