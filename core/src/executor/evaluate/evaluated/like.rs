use {
    super::{Evaluated, as_str},
    crate::{
        data::{RegexCache, StringExt, Value, like_with_cache},
        executor::evaluate::error::EvaluateError,
        result::Result,
    },
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub(crate) fn like_with_cache(
        &self,
        other: Evaluated<'a>,
        case_sensitive: bool,
        cache: &mut RegexCache,
    ) -> Result<Evaluated<'a>> {
        self.like_inner(other, case_sensitive, Some(cache))
    }

    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        self.like_inner(other, case_sensitive, None)
    }

    fn like_inner(
        &self,
        other: Evaluated<'a>,
        case_sensitive: bool,
        mut cache: Option<&mut RegexCache>,
    ) -> Result<Evaluated<'a>> {
        if let (Some(left), Some(right)) = (as_str(self), as_str(&other)) {
            let matched = match cache.as_mut() {
                Some(cache) => like_with_cache(left, right, case_sensitive, cache)?,
                None => left.like(right, case_sensitive)?,
            };
            return Ok(Evaluated::Value(Cow::Owned(Value::Bool(matched))));
        }

        self.like_values(other, case_sensitive)
    }

    fn like_values(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Text(lhs), Evaluated::Text(rhs)) => Evaluated::Value(Cow::Owned(
                Value::Bool(lhs.as_ref().like(rhs.as_ref(), case_sensitive)?),
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
                Evaluated::Value(Cow::Owned(
                    Value::try_from(literal.clone())?.like(r.as_ref(), case_sensitive)?,
                ))
            }
            (Evaluated::Value(l), literal @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                Evaluated::Value(Cow::Owned(
                    l.as_ref()
                        .like(&Value::try_from(literal.clone())?, case_sensitive)?,
                ))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::Value(Cow::Owned(l.as_ref().like(r.as_ref(), case_sensitive)?))
            }
            (
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
                Evaluated::StrSlice { source, range },
            ) => Evaluated::Value(Cow::Owned(
                Value::try_from(literal.clone())?
                    .like(&Value::Str(source[range].to_owned()), case_sensitive)?,
            )),
            (
                Evaluated::StrSlice { source, range },
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => Evaluated::Value(Cow::Owned(
                Value::Str(source[range.clone()].to_owned())
                    .like(&Value::try_from(literal.clone())?, case_sensitive)?,
            )),
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => Evaluated::Value(Cow::Owned(
                Value::Str(a[ar.clone()].to_owned())
                    .like(&Value::Str(b[br].to_owned()), case_sensitive)?,
            )),
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                Evaluated::Value(Cow::Owned(
                    Value::Str(source[range.clone()].to_owned())
                        .like(r.as_ref(), case_sensitive)?,
                ))
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(Cow::Owned(
                    l.as_ref()
                        .like(&Value::Str(source[range].to_owned()), case_sensitive)?,
                ))
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
        let val_str = |s: &str| Evaluated::Value(Cow::Owned(Value::Str(s.to_owned())));
        let slice = |s: &'static str| Evaluated::StrSlice {
            source: Cow::Owned(s.to_owned()),
            range: 0..s.len(),
        };
        let like_ok = |left: Evaluated, right: Evaluated| {
            assert_eq!(
                left.like(right, true),
                Ok(Evaluated::Value(Cow::Owned(Value::Bool(true))))
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

    #[test]
    fn like_with_cache_uses_cache_for_strings_and_falls_back_for_numbers() {
        let text = |value: &str| Evaluated::Text(Cow::Owned(value.to_owned()));
        let number = || Evaluated::Number(Cow::Owned(BigDecimal::from(42)));
        let value = |value: &str| Evaluated::Value(Cow::Owned(Value::Str(value.to_owned())));
        let boolean = || Evaluated::Value(Cow::Owned(Value::Bool(true)));
        let slice = |value: &str| Evaluated::StrSlice {
            source: Cow::Owned(value.to_owned()),
            range: 0..value.len(),
        };
        let mut cache = crate::data::RegexCache::new();

        assert_eq!(
            text("hello")
                .like_with_cache(text("h%"), false, &mut cache)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            slice("hello")
                .like_with_cache(slice("h%"), false, &mut cache)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            value("hello")
                .like_with_cache(value("h%"), false, &mut cache)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert!(
            number()
                .like_with_cache(text("%"), true, &mut cache)
                .is_err()
        );
        assert!(
            text("hello")
                .like_with_cache(number(), true, &mut cache)
                .is_err()
        );
        assert!(
            boolean()
                .like_with_cache(text("%"), true, &mut cache)
                .is_err()
        );
        assert!(
            text("hello")
                .like_with_cache(boolean(), true, &mut cache)
                .is_err()
        );
    }
}
