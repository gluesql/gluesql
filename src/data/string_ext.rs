use {crate::result::Result, regex::Regex, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum StringExtError {
    #[error("unreachable literal unary operation")]
    UnreachablePatternParsing,
}

pub trait StringExt {
    fn like(&self, pattern: &str) -> Result<bool>;
}

impl StringExt for String {
    fn like(&self, pattern: &str) -> Result<bool> {
        Ok(Regex::new(&format!(
            "^{}$",
            regex::escape(pattern).replace("%", ".*").replace("_", ".")
        ))
        .map_err(|_| StringExtError::UnreachablePatternParsing)?
        .is_match(self))
    }
}
