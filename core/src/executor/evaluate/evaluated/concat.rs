use {
    super::Evaluated,
    crate::{data::Value, executor::evaluate::literal::Literal, result::Result},
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                Evaluated::Literal(concat_literals(l, r))
            }
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::Value((Value::try_from(l)?).concat(r))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::Value(l.concat(Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::Value(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value((Value::try_from(l)?).concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(Value::try_from(r)?))
            }
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

fn concat_literals(left: Literal<'_>, right: Literal<'_>) -> Literal<'static> {
    fn literal_to_string(literal: Literal<'_>) -> String {
        match literal {
            Literal::Number(value) => value.to_string(),
            Literal::Text(value) => value.into_owned(),
        }
    }

    Literal::Text(Cow::Owned(
        literal_to_string(left) + &literal_to_string(right),
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    fn text(value: &str) -> Literal<'static> {
        Literal::Text(Cow::Owned(value.to_owned()))
    }

    fn num(value: &str) -> Literal<'static> {
        Literal::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    #[test]
    fn literal_concat_via_evaluated() {
        assert_eq!(
            Evaluated::Literal(text("Foo"))
                .concat(Evaluated::Literal(text("Bar")))
                .unwrap(),
            Evaluated::Literal(text("FooBar"))
        );

        assert_eq!(
            Evaluated::Literal(num("1"))
                .concat(Evaluated::Literal(num("2")))
                .unwrap(),
            Evaluated::Literal(text("12"))
        );
    }
}
