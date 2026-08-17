use {
    super::Evaluated,
    crate::{
        data::{StringExt, Value},
        result::Result,
    },
    std::borrow::Cow,
};

/// Borrows the text behind an `Evaluated` without allocating, when it holds one.
fn as_str<'a>(evaluated: &'a Evaluated<'_>) -> Option<&'a str> {
    match evaluated {
        Evaluated::Text(value) => Some(value.as_ref()),
        Evaluated::StrSlice { source, range } => Some(&source[range.clone()]),
        Evaluated::Value(value) => match value.as_ref() {
            Value::Str(value) => Some(value.as_str()),
            _ => None,
        },
        Evaluated::Number(_) => None,
    }
}

impl<'a> Evaluated<'a> {
    pub fn regex(
        &self,
        other: Evaluated<'a>,
        negated: bool,
        case_sensitive: bool,
    ) -> Result<Evaluated<'a>> {
        if let (Some(target), Some(pattern)) = (as_str(self), as_str(&other)) {
            let matched = target.regex(pattern, case_sensitive)?;

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
}
