use {super::Evaluated, crate::data::Value, std::borrow::Cow};

impl<'a> Evaluated<'a> {
    pub fn concat(self, other: Evaluated) -> Evaluated<'a> {
        match (self, other) {
            (Evaluated::Number(l), Evaluated::Number(r)) => {
                Evaluated::Text(Cow::Owned(l.to_string() + &r.to_string()))
            }
            (Evaluated::Number(l), Evaluated::Text(r)) => {
                Evaluated::Text(Cow::Owned(l.to_string() + r.as_ref()))
            }
            (Evaluated::Number(l), Evaluated::Value(r)) => {
                Evaluated::Value(Cow::Owned(Value::Str(l.to_string()).concat(r.into_owned())))
            }
            (Evaluated::Number(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(Cow::Owned(Value::Str(l.to_string() + &source[range])))
            }
            (Evaluated::Text(l), Evaluated::Number(r)) => {
                Evaluated::Text(Cow::Owned(l.into_owned() + &r.to_string()))
            }
            (Evaluated::Text(l), Evaluated::Text(r)) => {
                Evaluated::Text(Cow::Owned(l.into_owned() + &r))
            }
            (Evaluated::Text(l), Evaluated::Value(r)) => Evaluated::Value(Cow::Owned(
                Value::Str(l.into_owned()).concat(r.into_owned()),
            )),
            (Evaluated::Text(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(Cow::Owned(Value::Str(l.into_owned() + &source[range])))
            }
            (Evaluated::Value(l), Evaluated::Number(r)) => {
                Evaluated::Value(Cow::Owned(l.into_owned().concat(Value::Str(r.to_string()))))
            }
            (Evaluated::Value(l), Evaluated::Text(r)) => Evaluated::Value(Cow::Owned(
                l.into_owned().concat(Value::Str(r.into_owned())),
            )),
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::Value(Cow::Owned(l.into_owned().concat(r.into_owned())))
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => Evaluated::Value(
                Cow::Owned(l.into_owned().concat(Value::Str(source[range].to_owned()))),
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Number(r)) => Evaluated::Value(
                Cow::Owned(Value::Str(source[range].to_owned() + &r.to_string())),
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Text(r)) => Evaluated::Value(
                Cow::Owned(Value::Str(source[range].to_owned() + r.as_ref())),
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => Evaluated::Value(
                Cow::Owned(Value::Str(source[range].to_owned()).concat(r.into_owned())),
            ),
            (
                Evaluated::StrSlice {
                    source: l_source,
                    range: l_range,
                },
                Evaluated::StrSlice {
                    source: r_source,
                    range: r_range,
                },
            ) => Evaluated::Value(Cow::Owned(Value::Str(
                l_source[l_range].to_owned() + &r_source[r_range],
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::data::Value,
        bigdecimal::BigDecimal,
        std::{borrow::Cow, ops::Range, str::FromStr},
    };

    fn number(value: &str) -> Evaluated<'static> {
        Evaluated::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    fn text(value: &str) -> Evaluated<'static> {
        Evaluated::Text(Cow::Owned(value.to_owned()))
    }

    fn value(value: &str) -> Evaluated<'static> {
        Evaluated::Value(Cow::Owned(Value::Str(value.to_owned())))
    }

    fn slice(source: &'static str, range: Range<usize>) -> Evaluated<'static> {
        Evaluated::StrSlice {
            source: Cow::Borrowed(source),
            range,
        }
    }

    fn assert_text(result: Evaluated<'_>, expected: &str) {
        match result {
            Evaluated::Text(actual) => assert_eq!(actual, expected),
            other => panic!("expected text('{expected}'), got {other:?}"),
        }
    }

    fn assert_value(result: Evaluated<'_>, expected: &str) {
        let Evaluated::Value(cow) = result else {
            panic!("expected value str('{expected}'), got {result:?}");
        };
        assert_eq!(cow.as_ref(), &Value::Str(expected.to_owned()));
    }

    #[test]
    fn concat_covers_all_branches() {
        // number
        assert_text(number("1").concat(number("2")), "12");
        assert_text(number("1").concat(text("foo")), "1foo");
        assert_value(number("1").concat(value("bar")), "1bar");
        assert_value(number("1").concat(slice("xyz", 1..3)), "1yz");

        // text
        assert_text(text("foo").concat(number("1")), "foo1");
        assert_text(text("foo").concat(text("bar")), "foobar");
        assert_value(text("foo").concat(value("bar")), "foobar");
        assert_value(text("foo").concat(slice("xyz", 0..2)), "fooxy");

        // value
        assert_value(value("foo").concat(number("1")), "foo1");
        assert_value(value("foo").concat(text("bar")), "foobar");
        assert_value(value("foo").concat(value("bar")), "foobar");
        assert_value(value("foo").concat(slice("xyz", 2..3)), "fooz");

        // slice
        assert_value(slice("ab", 0..1).concat(number("1")), "a1");
        assert_value(slice("ab", 1..2).concat(text("cd")), "bcd");
        assert_value(slice("hello", 0..5).concat(value("!")), "hello!");
        assert_value(slice("left", 0..2).concat(slice("right", 1..5)), "leight");
    }

    #[test]
    #[should_panic(expected = "expected text")]
    fn assert_text_panics_on_non_text() {
        assert_text(value("foo"), "foo");
    }

    #[test]
    #[should_panic(expected = "expected value str")]
    fn assert_value_panics_on_non_value() {
        assert_value(text("foo"), "foo");
    }
}
