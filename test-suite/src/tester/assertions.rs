use {
    gluesql_core::prelude::{Payload, Result, Value},
    pretty_assertions::assert_eq,
};

const F32_TOLERANCE: f64 = 1e-6;
const F64_TOLERANCE: f64 = 1e-12;

#[track_caller]
pub(crate) fn assert_result(actual: &Result<Payload>, expected: &Result<Payload>, context: &str) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => assert_payload(actual, expected, context),
        _ => assert_eq!(actual, expected, "{context}\nresult mismatch"),
    }
}

#[track_caller]
pub(crate) fn assert_payload(actual: &Payload, expected: &Payload, context: &str) {
    match (actual, expected) {
        (
            Payload::Select {
                labels: actual_labels,
                rows: actual_rows,
            },
            Payload::Select {
                labels: expected_labels,
                rows: expected_rows,
            },
        ) => {
            assert_eq!(actual_labels, expected_labels, "{context}\nlabel mismatch");
            assert_eq!(
                actual_rows.len(),
                expected_rows.len(),
                "{context}\nrow count mismatch"
            );

            for (row_index, (actual_row, expected_row)) in
                actual_rows.iter().zip(expected_rows).enumerate()
            {
                assert_eq!(
                    actual_row.len(),
                    expected_row.len(),
                    "{context}\ncolumn count mismatch in row {row_index}"
                );

                for (column_index, (actual, expected)) in
                    actual_row.iter().zip(expected_row).enumerate()
                {
                    let label = &expected_labels[column_index];
                    assert_value(
                        actual,
                        expected,
                        &format!(
                            "{context}\nvalue mismatch in row {row_index}, column {column_index} ({label})"
                        ),
                    );
                }
            }
        }
        (Payload::SelectMap(actual_rows), Payload::SelectMap(expected_rows)) => {
            assert_eq!(
                actual_rows.len(),
                expected_rows.len(),
                "{context}\nrow count mismatch"
            );

            for (row_index, (actual, expected)) in actual_rows.iter().zip(expected_rows).enumerate()
            {
                assert_value(
                    &Value::Map(actual.clone()),
                    &Value::Map(expected.clone()),
                    &format!("{context}\nvalue mismatch in map row {row_index}"),
                );
            }
        }
        _ => assert_eq!(actual, expected, "{context}\npayload mismatch"),
    }
}

#[track_caller]
fn assert_value(actual: &Value, expected: &Value, context: &str) {
    match (actual, expected) {
        (Value::F32(actual), Value::F32(expected)) => {
            assert_float(
                f64::from(*actual),
                f64::from(*expected),
                F32_TOLERANCE,
                context,
            );
        }
        (Value::F64(actual), Value::F64(expected)) => {
            assert_float(*actual, *expected, F64_TOLERANCE, context);
        }
        (Value::Map(actual), Value::Map(expected)) => {
            let actual_keys = actual.keys().collect::<Vec<_>>();
            let expected_keys = expected.keys().collect::<Vec<_>>();
            assert_eq!(actual_keys, expected_keys, "{context}\nmap key mismatch");

            for (key, expected) in expected {
                assert_value(&actual[key], expected, &format!("{context}, key {key:?}"));
            }
        }
        (Value::List(actual), Value::List(expected)) => {
            assert_eq!(
                actual.len(),
                expected.len(),
                "{context}\nlist length mismatch"
            );

            for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                assert_value(actual, expected, &format!("{context}, list index {index}"));
            }
        }
        _ => assert_eq!(actual, expected, "{context}"),
    }
}

#[track_caller]
fn assert_float(actual: f64, expected: f64, tolerance: f64, context: &str) {
    let matches = (actual.is_nan() && expected.is_nan())
        || actual == expected
        || (actual.is_finite()
            && expected.is_finite()
            && (actual - expected).abs() <= tolerance * actual.abs().max(expected.abs()).max(1.0));

    assert!(
        matches,
        "{context}\nexpected {expected:?}, found {actual:?}, tolerance {tolerance:e}"
    );
}

#[cfg(test)]
mod tests {
    use {
        super::{Payload, Value, assert_float, assert_payload, assert_result, assert_value},
        crate::*,
        gluesql_core::{
            prelude::Value::{F32, F64, I64, List, Map},
            query_builder::QueryBuilderError::FailedToParseNumeric,
        },
        serde_json::json,
    };

    #[test]
    fn assert_result_matches() {
        let actual = Ok(select!(value; F64; 1.000_000_000_000_5));
        let expected = Ok(select!(value; F64; 1.0));
        assert_result(&actual, &expected, "successful payload within tolerance");

        let actual = Err(FailedToParseNumeric("first".to_owned()).into());
        let expected = Err(FailedToParseNumeric("first".to_owned()).into());
        assert_result(&actual, &expected, "same error variant and fields");
    }

    #[test]
    #[should_panic(expected = "value mismatch")]
    fn assert_result_mismatched_payloads() {
        let actual = Ok(select!(value; I64; 2));
        let expected = Ok(select!(value; I64; 1));
        assert_result(&actual, &expected, "different successful payloads");
    }

    #[test]
    #[should_panic(expected = "result mismatch")]
    fn assert_result_mismatched_error_fields() {
        let actual = Err(FailedToParseNumeric("first".to_owned()).into());
        let expected = Err(FailedToParseNumeric("second".to_owned()).into());
        assert_result(
            &actual,
            &expected,
            "same error variant with different fields",
        );
    }

    #[test]
    #[should_panic(expected = "result mismatch")]
    fn assert_result_unexpected_success() {
        let actual = Ok(Payload::Create);
        let expected = Err(FailedToParseNumeric("invalid".to_owned()).into());
        assert_result(&actual, &expected, "success when an error is expected");
    }

    #[test]
    #[should_panic(expected = "result mismatch")]
    fn assert_result_unexpected_error() {
        let actual = Err(FailedToParseNumeric("invalid".to_owned()).into());
        let expected = Ok(Payload::Create);
        assert_result(&actual, &expected, "error when success is expected");
    }

    #[test]
    fn assert_payload_matches() {
        let actual = select!(value; F64; 1.000_000_000_000_5);
        let expected = select!(value; F64; 1.0);
        assert_payload(&actual, &expected, "Select value within tolerance");

        let actual = select_map![json!({ "value": 1.000_000_000_000_5 })];
        let expected = select_map![json!({ "value": 1.0 })];
        assert_payload(&actual, &expected, "SelectMap value within tolerance");

        let actual = Payload::Insert(1);
        let expected = Payload::Insert(1);
        assert_payload(&actual, &expected, "same Insert count");
    }

    #[test]
    #[should_panic(expected = "label mismatch")]
    fn assert_payload_mismatched_labels() {
        let actual = select!(other; I64; 1);
        let expected = select!(value; I64; 1);
        assert_payload(&actual, &expected, "different Select labels");
    }

    #[test]
    #[should_panic(expected = "row count mismatch")]
    fn assert_payload_mismatched_row_counts() {
        let actual = select!(value; I64; 1; 1);
        let expected = select!(value; I64; 1);
        assert_payload(&actual, &expected, "different Select row counts");
    }

    #[test]
    #[should_panic(expected = "column count mismatch")]
    fn assert_payload_mismatched_column_counts() {
        let actual = Payload::Select {
            labels: vec!["value".to_owned()],
            rows: vec![vec![I64(1), I64(2)]],
        };
        let expected = select!(value; I64; 1);
        assert_payload(&actual, &expected, "different Select column counts");
    }

    #[test]
    #[should_panic(expected = "value mismatch")]
    fn assert_payload_mismatched_values() {
        let actual = select!(value; I64; 2);
        let expected = select!(value; I64; 1);
        assert_payload(&actual, &expected, "different Select values");
    }

    #[test]
    #[should_panic(expected = "row count mismatch")]
    fn assert_payload_mismatched_map_row_counts() {
        let actual = select_map![json!({ "value": 1 }), json!({ "value": 1 })];
        let expected = select_map![json!({ "value": 1 })];
        assert_payload(&actual, &expected, "different SelectMap row counts");
    }

    #[test]
    #[should_panic(expected = "value mismatch in map row")]
    fn assert_payload_mismatched_map_values() {
        let actual = select_map![json!({ "value": 2 })];
        let expected = select_map![json!({ "value": 1 })];
        assert_payload(&actual, &expected, "different SelectMap values");
    }

    #[test]
    #[should_panic(expected = "payload mismatch")]
    fn assert_payload_mismatched_insert_counts() {
        let actual = Payload::Insert(2);
        let expected = Payload::Insert(1);
        assert_payload(&actual, &expected, "different Insert counts");
    }

    #[test]
    #[should_panic(expected = "payload mismatch")]
    fn assert_payload_mismatched_variants() {
        let actual = Payload::Create;
        let expected = select!(value; I64; 1);
        assert_payload(&actual, &expected, "different payload variants");
    }

    #[test]
    fn assert_value_matches() {
        assert_value(&F32(1.000_000_5), &F32(1.0), "F32 within tolerance");
        assert_value(&F64(1.000_000_000_000_5), &F64(1.0), "F64 within tolerance");

        let actual = Map([("value".to_owned(), F64(1.000_000_000_000_5))].into());
        let expected = Map([("value".to_owned(), F64(1.0))].into());
        assert_value(&actual, &expected, "Map value within tolerance");

        let actual = List(vec![F64(1.000_000_000_000_5)]);
        let expected = List(vec![F64(1.0)]);
        assert_value(&actual, &expected, "List value within tolerance");

        assert_value(&I64(1), &I64(1), "equal integers");
    }

    #[test]
    #[should_panic(expected = "F32 outside tolerance")]
    fn assert_value_f32_outside_tolerance() {
        assert_value(&F32(1.000_002), &F32(1.0), "F32 outside tolerance");
    }

    #[test]
    #[should_panic(expected = "F64 outside tolerance")]
    fn assert_value_f64_outside_tolerance() {
        assert_value(&F64(1.000_000_000_002), &F64(1.0), "F64 outside tolerance");
    }

    #[test]
    #[should_panic(expected = "map key mismatch")]
    fn assert_value_mismatched_map_keys() {
        let actual = Map([("other".to_owned(), I64(1))].into());
        let expected = Map([("value".to_owned(), I64(1))].into());
        assert_value(&actual, &expected, "different Map keys");
    }

    #[test]
    #[should_panic(expected = "mismatched map values")]
    fn assert_value_mismatched_map_values() {
        let actual = Map([("value".to_owned(), I64(2))].into());
        let expected = Map([("value".to_owned(), I64(1))].into());
        assert_value(&actual, &expected, "mismatched map values");
    }

    #[test]
    #[should_panic(expected = "list length mismatch")]
    fn assert_value_mismatched_list_lengths() {
        let actual = List(vec![I64(1), I64(2)]);
        let expected = List(vec![I64(1)]);
        assert_value(&actual, &expected, "different List lengths");
    }

    #[test]
    #[should_panic(expected = "mismatched list values")]
    fn assert_value_mismatched_list_values() {
        let actual = List(vec![I64(2)]);
        let expected = List(vec![I64(1)]);
        assert_value(&actual, &expected, "mismatched list values");
    }

    #[test]
    #[should_panic(expected = "mismatched integers")]
    fn assert_value_mismatched_integers() {
        assert_value(&I64(2), &I64(1), "mismatched integers");
    }

    #[test]
    #[should_panic(expected = "mismatched value types")]
    fn assert_value_mismatched_types() {
        assert_value(&F64(1.0), &I64(1), "mismatched value types");
    }

    #[test]
    fn assert_float_matches() {
        let tolerance = 0.125;

        assert_float(1.0, 1.0, tolerance, "equal finite values");
        assert_float(0.0, -0.0, tolerance, "signed zeros");

        assert_float(0.0625, 0.0, tolerance, "within absolute tolerance");
        assert_float(
            0.125,
            0.0,
            tolerance,
            "positive absolute tolerance boundary",
        );
        assert_float(
            -0.125,
            0.0,
            tolerance,
            "negative absolute tolerance boundary",
        );

        assert_float(7.5, 8.0, tolerance, "within relative tolerance");
        assert_float(
            7.0,
            8.0,
            tolerance,
            "relative boundary with larger expected",
        );
        assert_float(8.0, 7.0, tolerance, "relative boundary with larger actual");
        assert_float(
            -7.0,
            -8.0,
            tolerance,
            "relative boundary for negative values",
        );

        assert_float(f64::NAN, f64::NAN, tolerance, "two NaNs");
        assert_float(
            f64::INFINITY,
            f64::INFINITY,
            tolerance,
            "positive infinities",
        );
        assert_float(
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
            tolerance,
            "negative infinities",
        );
    }

    #[test]
    #[should_panic(expected = "outside absolute tolerance")]
    fn assert_float_outside_absolute_tolerance() {
        assert_float(0.25, 0.0, 0.125, "outside absolute tolerance");
    }

    #[test]
    #[should_panic(expected = "outside relative tolerance with larger expected")]
    fn assert_float_outside_relative_tolerance_with_larger_expected() {
        assert_float(
            6.875,
            8.0,
            0.125,
            "outside relative tolerance with larger expected",
        );
    }

    #[test]
    #[should_panic(expected = "outside relative tolerance with larger actual")]
    fn assert_float_outside_relative_tolerance_with_larger_actual() {
        assert_float(
            8.0,
            6.875,
            0.125,
            "outside relative tolerance with larger actual",
        );
    }

    #[test]
    #[should_panic(expected = "NaN compared with a finite value")]
    fn assert_float_nan_and_finite() {
        assert_float(f64::NAN, 1.0, 0.125, "NaN compared with a finite value");
    }

    #[test]
    #[should_panic(expected = "finite value compared with NaN")]
    fn assert_float_finite_and_nan() {
        assert_float(1.0, f64::NAN, 0.125, "finite value compared with NaN");
    }

    #[test]
    #[should_panic(expected = "opposite infinities")]
    fn assert_float_opposite_infinities() {
        assert_float(
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.125,
            "opposite infinities",
        );
    }

    #[test]
    #[should_panic(expected = "infinity compared with a finite value")]
    fn assert_float_infinite_and_finite() {
        assert_float(
            f64::INFINITY,
            1.0,
            0.125,
            "infinity compared with a finite value",
        );
    }

    #[test]
    #[should_panic(expected = "finite value compared with infinity")]
    fn assert_float_finite_and_infinite() {
        assert_float(
            1.0,
            f64::INFINITY,
            0.125,
            "finite value compared with infinity",
        );
    }
}
