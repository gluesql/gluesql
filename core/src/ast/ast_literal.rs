use {
    crate::{ast::ToSql, data::Interval},
    bigdecimal::BigDecimal,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AstLiteral {
    Boolean(bool),
    Number(BigDecimal),
    QuotedString(String),
    HexString(String),
    Interval {
        value: String,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Null,
}

impl ToSql for AstLiteral {
    fn to_sql(&self) -> String {
        match self {
            AstLiteral::Boolean(b) => b.to_string(),
            AstLiteral::Number(n) => n.to_string(),
            AstLiteral::QuotedString(qs) => format!("\"{}\"", qs),
            AstLiteral::HexString(hs) => format!("\"{}\"", hs),
            AstLiteral::Interval {
                value,
                leading_field,
                last_field,
            } => Interval::try_from_literal(value, leading_field.as_ref(), last_field.as_ref())
                .unwrap()
                .into(),
            AstLiteral::Null => "Null".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}
