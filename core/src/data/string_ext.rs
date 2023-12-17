use regex::RegexBuilder;

use {crate::result::Result, serde::Serialize, thiserror::Error};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum StringExtError {
    #[error("unreachable literal unary operation")]
    UnreachablePatternParsing,
}

pub trait StringExt {
    fn like(&self, pattern: &str, case_sensitive: bool, escape_char: &Option<char>)
        -> Result<bool>;
}

impl StringExt for str {
    fn like(
        &self,
        pattern: &str,
        case_sensitive: bool,
        escape_char: &Option<char>,
    ) -> Result<bool> {
        let pattern = pattern
            .chars()
            .scan(false, |escaped, char| {
                if !*escaped && Some(char) == *escape_char {
                    *escaped = true;
                    return Some("".to_owned());
                }
                let regex = match (&escaped, &char) {
                    (false, '_') => ".".to_owned(),
                    (false, '%') => ".*".to_owned(),
                    (true, '\\') => "\\\\".to_owned(),
                    _ => char.to_string(),
                };
                *escaped = false;
                Some(regex)
            })
            .collect::<String>();
        RegexBuilder::new(&format!("^{pattern}$"))
            .case_insensitive(!case_sensitive)
            .build()
            .map(|regex| regex.is_match(self))
            .map_err(|_| StringExtError::UnreachablePatternParsing.into())
    }
}
