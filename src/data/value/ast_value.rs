use {
    super::{error::ValueError, Value},
    crate::result::{Error, Result},
    sqlparser::ast::{DataType, Value as AstValue},
    std::{cmp::Ordering, convert::TryFrom},
};

impl PartialEq<AstValue> for Value {
    fn eq(&self, other: &AstValue) -> bool {
        match (self, other) {
            (Value::Bool(l), AstValue::Boolean(r)) => l == r,
            (Value::I64(l), AstValue::Number(r, false)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64) == r,
                    Err(_) => false,
                },
            },
            (Value::F64(l), AstValue::Number(r, false)) => match r.parse::<f64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => *l == (r as f64),
                    Err(_) => false,
                },
            },
            (Value::Str(l), AstValue::SingleQuotedString(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<AstValue> for Value {
    fn partial_cmp(&self, other: &AstValue) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), AstValue::Number(r, false)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64).partial_cmp(&r),
                    Err(_) => None,
                },
            },
            (Value::F64(l), AstValue::Number(r, false)) => match r.parse::<f64>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => l.partial_cmp(&(r as f64)),
                    Err(_) => None,
                },
            },
            (Value::Str(l), AstValue::SingleQuotedString(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl TryFrom<&AstValue> for Value {
    type Error = Error;

    fn try_from(literal: &AstValue) -> Result<Self> {
        match literal {
            AstValue::Number(v, false) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            AstValue::Boolean(v) => Ok(Value::Bool(*v)),
            AstValue::SingleQuotedString(v) => Ok(Value::Str(v.to_string())),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }
}

pub trait TryFromLiteral {
    fn try_from_literal(data_type: &DataType, literal: &AstValue) -> Result<Value>;
}

impl TryFromLiteral for Value {
    fn try_from_literal(data_type: &DataType, literal: &AstValue) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, AstValue::SingleQuotedString(v))
            | (DataType::Boolean, AstValue::Number(v, false)) => match v.to_uppercase().as_str() {
                "TRUE" | "1" => Ok(Value::Bool(true)),
                "FALSE" | "0" => Ok(Value::Bool(false)),
                _ => Err(ValueError::LiteralCastToBooleanFailed(v.to_string()).into()),
            },
            (DataType::Int, AstValue::SingleQuotedString(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int, AstValue::Number(v, false)) => v
                .parse::<f64>()
                .map_err(|_| {
                    ValueError::UnreachableLiteralCastFromNumberToInteger(v.to_string()).into()
                })
                .map(|v| Value::I64(v.trunc() as i64)),
            (DataType::Int, AstValue::Boolean(v)) => {
                let v = if *v { 1 } else { 0 };

                Ok(Value::I64(v))
            }
            (DataType::Float(_), AstValue::SingleQuotedString(v))
            | (DataType::Float(_), AstValue::Number(v, false)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::LiteralCastToFloatFailed(v.to_string()).into()),
            (DataType::Float(_), AstValue::Boolean(v)) => {
                let v = if *v { 1.0 } else { 0.0 };

                Ok(Value::F64(v))
            }
            (DataType::Text, AstValue::Number(v, false)) => Ok(Value::Str(v.to_string())),
            (DataType::Text, AstValue::Boolean(v)) => {
                let v = if *v { "TRUE" } else { "FALSE" };

                Ok(Value::Str(v.to_owned()))
            }
            (DataType::Boolean, AstValue::Null)
            | (DataType::Int, AstValue::Null)
            | (DataType::Float(_), AstValue::Null)
            | (DataType::Text, AstValue::Null) => Ok(Value::Null),
            _ => Err(ValueError::UnimplementedLiteralCast {
                data_type: data_type.to_string(),
                literal: literal.to_string(),
            }
            .into()),
        }
    }
}
