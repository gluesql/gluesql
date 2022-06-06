use {
    crate::ast::ToSql,
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
            AstLiteral::Boolean(b) => b.to_string().to_uppercase(),
            AstLiteral::Number(n) => n.to_string(),
            AstLiteral::QuotedString(qs) => format!(r#""{qs}""#),
            AstLiteral::HexString(hs) => format!(r#""{hs}""#),
            AstLiteral::Interval {
                value,
                leading_field,
                last_field,
            } => {
                let value = format!(r#"INTERVAL "{value}""#);
                let leading = leading_field
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "".to_owned());

                match last_field {
                    Some(last) => format!("{value} {leading} TO {last}"),
                    None => format!("{value} {leading}"),
                }
            }
            AstLiteral::Null => "NULL".to_string(),
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

#[cfg(test)]
mod tests {
    use {
        crate::ast::{AstLiteral, DateTimeField, ToSql},
        bigdecimal::BigDecimal,
    };

    #[test]
    fn to_sql() {
        assert_eq!("TRUE", AstLiteral::Boolean(true).to_sql());
        assert_eq!("123", AstLiteral::Number(BigDecimal::from(123)).to_sql());
        assert_eq!(
            r#""hello""#,
            AstLiteral::QuotedString("hello".to_owned()).to_sql()
        );
        assert_eq!(
            r#"INTERVAL "1-2" YEAR TO MONTH"#,
            AstLiteral::Interval {
                value: "1-2".to_owned(),
                leading_field: Some(DateTimeField::Year),
                last_field: Some(DateTimeField::Month),
            }
            .to_sql()
        );
        assert_eq!(
            r#"INTERVAL "10" HOUR"#,
            AstLiteral::Interval {
                value: "10".to_owned(),
                leading_field: Some(DateTimeField::Hour),
                last_field: None,
            }
            .to_sql()
        );
        assert_eq!("NULL", AstLiteral::Null.to_sql());
    }
}
