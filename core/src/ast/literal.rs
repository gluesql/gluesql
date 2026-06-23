use {
    crate::ast::ToSql,
    bigdecimal::BigDecimal,
    serde::{Deserialize, Serialize},
    strum_macros::Display,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Literal {
    Number(BigDecimal),
    QuotedString(String),
}

impl ToSql for Literal {
    fn to_sql(&self) -> String {
        match self {
            Literal::Number(n) => n.to_string(),
            Literal::QuotedString(qs) => {
                let escaped = qs.replace('\'', "''");
                format!("'{escaped}'")
            }
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
        crate::ast::{Literal, ToSql},
        bigdecimal::BigDecimal,
    };

    #[test]
    fn to_sql() {
        assert_eq!("123", Literal::Number(BigDecimal::from(123)).to_sql());
        assert_eq!(
            "'hello'",
            Literal::QuotedString("hello".to_owned()).to_sql()
        );
        assert_eq!(
            "'can''t'",
            Literal::QuotedString("can't".to_owned()).to_sql()
        );
    }
}
