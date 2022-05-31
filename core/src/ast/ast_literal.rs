use {
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
