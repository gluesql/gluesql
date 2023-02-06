use {crate::result::Result, regex::Regex, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum StringExtError {
    #[error("unreachable literal unary operation")]
    UnreachablePatternParsing,
}

pub trait StringExt {
    fn like(&self, pattern: &str, case_sensitive: bool) -> Result<bool>;
}

impl StringExt for str {
    fn like(&self, pattern: &str, case_sensitive: bool) -> Result<bool> {
        let (match_string, match_pattern) = match case_sensitive {
            true => (self.to_owned(), pattern.to_owned()),
            false => {
                let lowercase_string = self.to_lowercase();
                let lowercase_pattern = pattern.to_lowercase();

                (lowercase_string, lowercase_pattern)
            }
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
}
