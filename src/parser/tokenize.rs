use regex::Regex;
use crate::parser::Token;

pub fn tokenize(raw_sql: String) -> Vec<Token> {
    Regex::new(r"\s+").unwrap()
        .split(&raw_sql)
        .filter(|s| s != &"")
        .map(|s| Regex::new(r"[,;]$").unwrap().replace(s, ""))
        .map(|s| Token::from(s.to_string()))
        .collect()
}
