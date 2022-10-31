use bigdecimal::FromPrimitive;

use crate::{prelude::Value, result::Error};

use super::Expr;

use {
    crate::{ast::ToSql, result::Result},
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
    Null,
}

impl ToSql for AstLiteral {
    fn to_sql(&self) -> String {
        match self {
            AstLiteral::Boolean(b) => b.to_string().to_uppercase(),
            AstLiteral::Number(n) => n.to_string(),
            AstLiteral::QuotedString(qs) => format!(r#""{qs}""#),
            AstLiteral::HexString(hs) => format!(r#""{hs}""#),
            AstLiteral::Null => "NULL".to_owned(),
        }
    }
}

impl<'a> TryFrom<Value> for AstLiteral {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let ast_literal = match value {
            Value::Bool(v) => AstLiteral::Boolean(v),
            Value::I8(v) => AstLiteral::Number(BigDecimal::from_i8(v).unwrap()),
            Value::I16(v) => AstLiteral::Number(BigDecimal::from_i16(v).unwrap()),
            Value::I32(v) => AstLiteral::Number(BigDecimal::from_i32(v).unwrap()),
            Value::I64(v) => AstLiteral::Number(BigDecimal::from_i64(v).unwrap()),
            Value::I128(v) => AstLiteral::Number(BigDecimal::from_i128(v).unwrap()),
            Value::U8(v) => AstLiteral::Number(BigDecimal::from_u8(v).unwrap()),
            Value::F64(v) => AstLiteral::Number(BigDecimal::from_f64(v).unwrap()),
            Value::Decimal(v) => todo!(),
            Value::Str(v) => AstLiteral::QuotedString(v),
            Value::Bytea(v) => todo!(),
            Value::Date(v) => todo!(),
            Value::Timestamp(v) => todo!(),
            Value::Time(v) => todo!(),
            Value::Interval(v) => todo!(),
            Value::Uuid(v) => todo!(),
            Value::Map(v) => todo!(),
            Value::List(v) => todo!(),
            Value::Null => AstLiteral::Null,
        };

        Ok(ast_literal)
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
            r#""hello""#,
            AstLiteral::QuotedString("hello".to_owned()).to_sql()
        );
        assert_eq!("NULL", AstLiteral::Null.to_sql());
    }
}
