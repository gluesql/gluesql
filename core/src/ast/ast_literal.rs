use {
    crate::ast::ToSql,
    bigdecimal::BigDecimal,
    serde::{Deserialize, Serialize},
    sqlparser::ast::Value as SqlValue,
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AstLiteral {
    Boolean(bool),
    Number(BigDecimal),
    QuotedString(String),
    HexString(String),
    Null,
}

impl ToSql for AstLiteral {
    fn to_sql(&self) -> String {
        match self {
            AstLiteral::Boolean(b) => SqlValue::Boolean(*b).to_string().to_uppercase(),
            AstLiteral::Number(n) => SqlValue::Number(n.clone(), false).to_string(),
            AstLiteral::QuotedString(qs) => SqlValue::SingleQuotedString(qs.to_owned()).to_string(),
            AstLiteral::HexString(hs) => SqlValue::HexStringLiteral(hs.to_owned()).to_string(),
            AstLiteral::Null => SqlValue::Null.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

#[cfg(test)]
mod tests {
    use {
        crate::ast::{AstLiteral, ToSql},
        bigdecimal::BigDecimal,
    };

    #[test]
    fn to_sql() {
        assert_eq!("TRUE", AstLiteral::Boolean(true).to_sql());
        assert_eq!("123", AstLiteral::Number(BigDecimal::from(123)).to_sql());
        assert_eq!(
            "'hello'",
            AstLiteral::QuotedString("hello".to_owned()).to_sql()
        );
        assert_eq!(
            "'can''t'",
            AstLiteral::QuotedString("can't".to_owned()).to_sql()
        );
        assert_eq!("X'1A2B'", AstLiteral::HexString("1A2B".to_owned()).to_sql());
        assert_eq!("NULL", AstLiteral::Null.to_sql());
    }
}
