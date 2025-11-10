use {
    super::Evaluated,
    crate::{data::Value, result::Result},
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (
                left @ (Evaluated::Number(_) | Evaluated::Text(_)),
                right @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => concat_literals(left, right),
            (literal @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(r)) => {
                Evaluated::Value(Value::try_from(literal)?.concat(r))
            }
            (Evaluated::Value(l), literal @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                Evaluated::Value(l.concat(Value::try_from(literal)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::Value(l.concat(r)),
            (
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
                Evaluated::StrSlice { source, range },
            ) => Evaluated::Value(
                Value::try_from(literal)?.concat(Value::Str(source[range].to_owned())),
            ),
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.concat(Value::Str(source[range].to_owned())))
            }
            (
                Evaluated::StrSlice { source, range },
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => Evaluated::Value(
                Value::Str(source[range].to_owned()).concat(Value::try_from(literal)?),
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(r))
            }
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => {
                Evaluated::Value(Value::Str(a[ar].to_owned()).concat(Value::Str(b[br].to_owned())))
            }
        };

        Ok(evaluated)
    }
}

fn concat_literals(left: Evaluated<'_>, right: Evaluated<'_>) -> Evaluated<'static> {
    Evaluated::Text(Cow::Owned(
        literal_to_string(left) + &literal_to_string(right),
    ))
}

fn literal_to_string(literal: Evaluated<'_>) -> String {
    match literal {
        Evaluated::Number(value) => value.to_string(),
        Evaluated::Text(value) => value.into_owned(),
        _ => unreachable!("literal_to_string only accepts literal values"),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    fn text(value: &str) -> Evaluated<'static> {
        Evaluated::Text(Cow::Owned(value.to_owned()))
    }

    fn num(value: &str) -> Evaluated<'static> {
        Evaluated::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    #[test]
    fn literal_concat_via_evaluated() {
        assert_eq!(text("Foo").concat(text("Bar")).unwrap(), text("FooBar"));

        assert_eq!(num("1").concat(num("2")).unwrap(), text("12"));
    }
}
