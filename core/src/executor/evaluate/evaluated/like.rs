use {
    super::Evaluated,
    crate::{
        data::{StringExt, Value},
        executor::evaluate::error::EvaluateError,
        result::Result,
    },
};

impl<'a> Evaluated<'a> {
    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Text(lhs), Evaluated::Text(rhs)) => Evaluated::Value(Value::Bool(
                lhs.as_ref().like(rhs.as_ref(), case_sensitive)?,
            )),
            (
                left @ (Evaluated::Number(_) | Evaluated::Text(_)),
                right @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => {
                return Err(EvaluateError::LikeOnNonStringLiteral {
                    base: left.to_string(),
                    pattern: right.to_string(),
                    case_sensitive,
                }
                .into());
            }
            (literal @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(r)) => {
                Evaluated::Value(Value::try_from(literal.clone())?.like(&r, case_sensitive)?)
            }
            (Evaluated::Value(l), literal @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                Evaluated::Value(l.like(&Value::try_from(literal.clone())?, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::Value(l.like(&r, case_sensitive)?)
            }
            (
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
                Evaluated::StrSlice { source, range },
            ) => Evaluated::Value(
                Value::try_from(literal.clone())?
                    .like(&Value::Str(source[range].to_owned()), case_sensitive)?,
            ),
            (
                Evaluated::StrSlice { source, range },
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => Evaluated::Value(
                Value::Str(source[range.clone()].to_owned())
                    .like(&Value::try_from(literal.clone())?, case_sensitive)?,
            ),
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => Evaluated::Value(
                Value::Str(a[ar.clone()].to_owned())
                    .like(&Value::Str(b[br].to_owned()), case_sensitive)?,
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => Evaluated::Value(
                Value::Str(source[range.clone()].to_owned()).like(&r, case_sensitive)?,
            ),
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.like(&Value::Str(source[range].to_owned()), case_sensitive)?)
            }
        };

        Ok(evaluated)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Evaluated,
        crate::{data::Value, executor::EvaluateError},
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
    };

    #[test]
    fn like() {
        let text = |s: &str| Evaluated::Text(Cow::Owned(s.to_owned()));
        let num = |s: &str| Evaluated::Number(Cow::Owned(BigDecimal::from_str(s).unwrap()));
        let val_str = |s: &str| Evaluated::Value(Value::Str(s.to_owned()));
        let slice = |s: &'static str| Evaluated::StrSlice {
            source: Cow::Owned(s.to_owned()),
            range: 0..s.len(),
        };
        let like_ok = |left: Evaluated, right: Evaluated| {
            assert_eq!(
                left.like(right, true),
                Ok(Evaluated::Value(Value::Bool(true)))
            );
        };

        like_ok(text("hello"), text("h%"));
        like_ok(text("hello"), val_str("%llo"));
        like_ok(val_str("hello"), text("h%"));
        like_ok(val_str("hello"), val_str("h%"));
        like_ok(text("hello"), slice("h%"));
        like_ok(slice("hello"), text("h%"));
        like_ok(slice("hello"), slice("h%"));
        like_ok(slice("hello"), val_str("h%"));
        like_ok(val_str("hello"), slice("h%"));

        assert_eq!(
            num("42").like(num("42"), true),
            Err(EvaluateError::LikeOnNonStringLiteral {
                base: "42".to_owned(),
                pattern: "42".to_owned(),
                case_sensitive: true,
            }
            .into())
        );
        assert_eq!(
            num("42").like(text("%"), true),
            Err(EvaluateError::LikeOnNonStringLiteral {
                base: "42".to_owned(),
                pattern: "%".to_owned(),
                case_sensitive: true,
            }
            .into())
        );
        assert_eq!(
            text("hello").like(num("42"), true),
            Err(EvaluateError::LikeOnNonStringLiteral {
                base: "hello".to_owned(),
                pattern: "42".to_owned(),
                case_sensitive: true,
            }
            .into())
        );
    }
}
