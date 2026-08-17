use {
    crate::result::Result,
    regex::{Regex, RegexBuilder},
    serde::Serialize,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum StringExtError {
    #[error("unreachable literal unary operation")]
    UnreachablePatternParsing,
    #[error("invalid regular expression {pattern:?}: {error}")]
    InvalidRegexPattern { pattern: String, error: String },
}

pub trait StringExt {
    fn like(&self, pattern: &str, case_sensitive: bool) -> Result<bool>;
    fn regex(&self, pattern: &str, case_sensitive: bool) -> Result<bool>;
}

impl StringExt for str {
    fn like(&self, pattern: &str, case_sensitive: bool) -> Result<bool> {
        let (match_string, match_pattern) = if case_sensitive {
            (self.to_owned(), pattern.to_owned())
        } else {
            let lowercase_string = self.to_lowercase();
            let lowercase_pattern = pattern.to_lowercase();

            (lowercase_string, lowercase_pattern)
        };

        Ok(Regex::new(&format!(
            "^{}$",
            regex::escape(match_pattern.as_str())
                .replace('%', ".*")
                .replace('_', ".")
        ))
        .map_err(|_| StringExtError::UnreachablePatternParsing)?
        .is_match(match_string.as_str()))
    }

    fn regex(&self, pattern: &str, case_sensitive: bool) -> Result<bool> {
        Ok(RegexBuilder::new(pattern)
            .case_insensitive(!case_sensitive)
            .build()
            .map_err(|error| StringExtError::InvalidRegexPattern {
                pattern: pattern.to_owned(),
                error: error.to_string(),
            })
            .map(|regex| regex.is_match(self))?)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{StringExt, StringExtError},
        crate::result::Error,
    };

    #[test]
    fn regex() {
        assert_eq!("Hello".regex("ell", true), Ok(true));
        assert_eq!("Hello".regex("^hello$", true), Ok(false));
        assert_eq!("Hello".regex("^hello$", false), Ok(true));
        assert!(matches!(
            "Hello".regex("[", true),
            Err(Error::StringExt(StringExtError::InvalidRegexPattern { pattern, .. })) if pattern == "["
        ));
        assert!(matches!(
            "Hello".regex("(?i)[", true),
            Err(Error::StringExt(StringExtError::InvalidRegexPattern { pattern, .. })) if pattern == "(?i)["
        ));
        assert!(matches!(
            "Hello".regex("[", false),
            Err(Error::StringExt(StringExtError::InvalidRegexPattern { pattern, .. })) if pattern == "["
        ));

        // the case-insensitive flag must not leak into the reported parse error
        assert!(matches!(
            "Hello".regex("[", false),
            Err(Error::StringExt(StringExtError::InvalidRegexPattern { error, .. }))
                if !error.contains("(?i)")
        ));
    }
}
