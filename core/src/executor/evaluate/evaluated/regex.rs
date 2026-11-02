use {
    super::{Evaluated, as_str},
    crate::{
        data::{RegexCache, StringExt, Value, regex_with_cache},
        result::Result,
    },
    std::borrow::Cow,
};

impl<'a> Evaluated<'a> {
    pub(crate) fn regex_with_cache(
        &self,
        other: Evaluated<'a>,
        negated: bool,
        case_sensitive: bool,
        cache: &mut RegexCache,
    ) -> Result<Evaluated<'a>> {
        self.regex_inner(other, negated, case_sensitive, Some(cache))
    }

    pub fn regex(
        &self,
        other: Evaluated<'a>,
        negated: bool,
        case_sensitive: bool,
    ) -> Result<Evaluated<'a>> {
        self.regex_inner(other, negated, case_sensitive, None)
    }

    fn regex_inner(
        &self,
        other: Evaluated<'a>,
        negated: bool,
        case_sensitive: bool,
        mut cache: Option<&mut RegexCache>,
    ) -> Result<Evaluated<'a>> {
        if let (Some(target), Some(pattern)) = (as_str(self), as_str(&other)) {
            let matched = match cache.as_mut() {
                Some(cache) => regex_with_cache(target, pattern, case_sensitive, cache)?,
                None => target.regex(pattern, case_sensitive)?,
            };
            return Ok(Evaluated::Value(Cow::Owned(Value::Bool(matched ^ negated))));
        }

        let left = Value::try_from(self.clone())?;
        let right = Value::try_from(other)?;

        left.regex(&right, negated, case_sensitive)
            .map(|value| Evaluated::Value(Cow::Owned(value)))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Evaluated,
        crate::{data::Value, data::ValueError},
        std::borrow::Cow,
    };

    #[test]
    fn regex() {
        let text = |value: &str| Evaluated::Text(Cow::Owned(value.to_owned()));
        let value = |value: &str| Evaluated::Value(Cow::Owned(Value::Str(value.to_owned())));
        let slice = |value: &str| Evaluated::StrSlice {
            source: Cow::Owned(value.to_owned()),
            range: 0..value.len(),
        };
        let null = || Evaluated::Value(Cow::Owned(Value::Null));

        assert_eq!(
            text("Hello")
                .regex(value("ell"), false, true)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            text("Hello")
                .regex(text("^hello$"), false, false)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            slice("Hello")
                .regex(slice("ell"), false, true)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            text("Hello")
                .regex(text("ell"), true, true)
                .unwrap()
                .to_string(),
            "FALSE"
        );
        assert_eq!(
            null().regex(text("."), false, true).unwrap().to_string(),
            "NULL"
        );
        // negation must not turn NULL into a boolean
        assert_eq!(
            null().regex(text("."), true, true).unwrap().to_string(),
            "NULL"
        );
        assert!(text("Hello").regex(text("["), false, true).is_err());
        assert_eq!(
            Evaluated::Value(Cow::Owned(Value::Bool(true)))
                .regex(text("."), true, false)
                .unwrap_err(),
            ValueError::RegexOnNonString {
                base: Value::Bool(true),
                pattern: Value::Str(".".to_owned()),
                operator: "!~*".to_owned(),
            }
            .into()
        );
    }

    #[test]
    fn regex_with_cache_uses_cache_for_strings_and_falls_back_for_values() {
        let text = |value: &str| Evaluated::Text(Cow::Owned(value.to_owned()));
        let slice = |value: &str| Evaluated::StrSlice {
            source: Cow::Owned(value.to_owned()),
            range: 0..value.len(),
        };
        let number_literal = || Evaluated::Number(Cow::Owned("42".parse().unwrap()));
        let number = Evaluated::Value(Cow::Owned(Value::I64(42)));
        let mut cache = crate::data::RegexCache::new();

        assert_eq!(
            text("Hello")
                .regex_with_cache(text("^hello$"), false, false, &mut cache)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert_eq!(
            slice("Hello")
                .regex_with_cache(slice("^hello$"), false, false, &mut cache)
                .unwrap()
                .to_string(),
            "TRUE"
        );
        assert!(
            number
                .regex_with_cache(text("."), false, true, &mut cache)
                .is_err()
        );
        assert!(
            number_literal()
                .regex_with_cache(text("."), false, true, &mut cache)
                .is_err()
        );
    }
}
