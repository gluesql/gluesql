use {
    super::{error::ValueError, Value},
    crate::result::{Error, Result},
    sqlparser::ast::Value as AstValue,
    std::{cmp::Ordering, convert::TryFrom},
};

impl PartialEq<AstValue> for Value {
    fn eq(&self, other: &AstValue) -> bool {
        match (self, other) {
            (Value::Bool(l), AstValue::Boolean(r))
            | (Value::OptBool(Some(l)), AstValue::Boolean(r)) => l == r,
            (Value::I64(l), AstValue::Number(r, false))
            | (Value::OptI64(Some(l)), AstValue::Number(r, false)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64) == r,
                    Err(_) => false,
                },
            },
            (Value::F64(l), AstValue::Number(r, false))
            | (Value::OptF64(Some(l)), AstValue::Number(r, false)) => match r.parse::<f64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => *l == (r as f64),
                    Err(_) => false,
                },
            },
            (Value::Str(l), AstValue::SingleQuotedString(r))
            | (Value::OptStr(Some(l)), AstValue::SingleQuotedString(r)) => l == r,
            (Value::OptBool(None), AstValue::Null)
            | (Value::OptI64(None), AstValue::Null)
            | (Value::OptF64(None), AstValue::Null)
            | (Value::OptStr(None), AstValue::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd<AstValue> for Value {
    fn partial_cmp(&self, other: &AstValue) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), AstValue::Number(r, false))
            | (Value::OptI64(Some(l)), AstValue::Number(r, false)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64).partial_cmp(&r),
                    Err(_) => None,
                },
            },
            (Value::F64(l), AstValue::Number(r, false))
            | (Value::OptF64(Some(l)), AstValue::Number(r, false)) => match r.parse::<f64>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => l.partial_cmp(&(r as f64)),
                    Err(_) => None,
                },
            },
            (Value::Str(l), AstValue::SingleQuotedString(r))
            | (Value::OptStr(Some(l)), AstValue::SingleQuotedString(r)) => Some(l.cmp(r)),
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
