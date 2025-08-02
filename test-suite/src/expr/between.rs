use {crate::*, gluesql_core::prelude::Value::*};

test_case!(between, {
    let g = get_tester!();

    // Related with non-NULL values
    for (target, lhs, rhs, expected) in [
        (0, 0, 0, true),
        (1, 1, 1, true),
        (2, 1, 3, true),
        (-1, -1, 1, true),
        (1, 2, 3, false),
        // Boundary cases for Int128
        (i128::MIN, i128::MIN, i128::MAX, true),
        (i128::MAX, i128::MIN, i128::MAX, true),
    ] {
        let expr = format!("{} BETWEEN {} AND {}", target, lhs, rhs);
        let result_macro = if expected {
            Ok(select!(res Bool; true))
        } else {
            Ok(select!(res Bool; false))
        };
        g.named_test(
            format!("'{expr}' should return {expected}").as_str(),
            format!("SELECT {expr} as res;").as_str(),
            result_macro,
        )
        .await;
    }

    // Related with NULL
    g.named_test(
        "'NULL BETWEEN ...' should return NULL",
        format!(
            "SELECT (NULL BETWEEN {} AND {}) as res;",
            i128::MIN,
            i128::MAX
        )
        .as_str(),
        Ok(select_with_null!(
           res;
           Null
        )),
    )
    .await;

    g.named_test(
        "'NULL BETWEEN NULL AND NULL' should return NULL",
        "SELECT (NULL BETWEEN NULL AND NULL) as res;",
        Ok(select_with_null!(
           res;
           Null
        )),
    )
    .await;

    for (target, rhs) in [
        (1, 1),                 // 'target' is same as 'rhs' but they are positive.
        (0, 0),                 // 'target' is same as 'rhs' but they are zero.
        (-1, -1),               // 'target' is same as 'rhs' but they are negative.
        (1, 2),                 // 'target' is less than 'rhs'.
        (i128::MIN, i128::MAX), // 'target' is less than 'rhs' but big difference.
        (2, 1),                 // 'target' is greater than 'rhs'.
        (i128::MAX, i128::MIN), // 'target' is greater than 'rhs' but big difference.
    ] {
        let expr = format!("{} BETWEEN NULL AND {}", target, rhs);
        g.named_test(
            format!("'{}' should return NULL", expr).as_str(),
            format!("SELECT {} as res;", expr).as_str(),
            Ok(select_with_null!(
                    res;
                    Null
            )),
        )
        .await;
    }

    for (target, lhs) in [
        (1, 1),                 // 'target' is same as 'lhs' but they are positive.
        (0, 0),                 // 'target' is same as 'lhs' but they are zero.
        (-1, -1),               // 'target' is same as 'lhs' but they are negative.
        (1, 2),                 // 'target' is less than 'lhs'.
        (i128::MIN, i128::MAX), // 'target' is less than 'lhs' but big difference.
        (2, 1),                 // 'target' is greater than 'lhs'.
        (i128::MAX, i128::MIN), // 'target' is greater than 'lhs' but big difference.
    ] {
        let expr = format!("{} BETWEEN {} AND NULL", target, lhs);
        g.named_test(
            format!("'{}' should return NULL", expr).as_str(),
            format!("SELECT {} as res;", expr).as_str(),
            Ok(select_with_null!(
                    res;
                    Null
            )),
        )
        .await;
    }
});
