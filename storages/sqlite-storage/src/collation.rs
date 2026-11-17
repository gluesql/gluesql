use {rust_decimal::Decimal, std::cmp::Ordering};

pub(super) const DECIMAL_COLLATION: &str = "gluesql_decimal";

pub(super) fn compare_decimal(left: &str, right: &str) -> Ordering {
    // Collations cannot return errors. Keep invalid text in a separate ordered group
    // so mixing numeric and lexical comparisons cannot violate transitivity.
    match (left.parse::<Decimal>(), right.parse::<Decimal>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        (Ok(_), Err(_)) => Ordering::Less,
        (Err(_), Ok(_)) => Ordering::Greater,
        (Err(_), Err(_)) => left.cmp(right),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::compare_decimal,
        std::cmp::Ordering::{Equal, Greater, Less},
    };

    #[test]
    fn compare_decimal_values() {
        for (left, right, expected) in [
            ("1.00", "1.0", Equal),
            ("-0.00", "0", Equal),
            ("2", "10", Less),
            ("10", "2", Greater),
            ("-10", "-2", Less),
            ("0.0000000000000000000000000001", "0", Greater),
            (
                "79228162514264337593543950334",
                "79228162514264337593543950335",
                Less,
            ),
        ] {
            let actual = compare_decimal(left, right);
            assert_eq!(actual, expected, "{left} compared with {right}");
        }
    }

    #[test]
    fn compare_invalid_decimal_text() {
        for (left, right, expected) in [
            ("10", "", Less),
            ("", "2", Greater),
            ("invalid", "other", Less),
            ("other", "invalid", Greater),
            ("invalid", "invalid", Equal),
        ] {
            let actual = compare_decimal(left, right);
            assert_eq!(actual, expected, "{left:?} compared with {right:?}");
        }
    }
}
