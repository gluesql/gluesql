use boolinator::Boolinator;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use thiserror::Error as ThisError;

use sqlparser::ast::{DataType, Expr, Ident, Value as AstValue};

use crate::executor::GroupKey;
use crate::result::{Error, Result};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum ValueError {
    #[error("sql type not supported yet")]
    SqlTypeNotSupported,

    #[error("literal not supported yet")]
    LiteralNotSupported,

    #[error("ast expr not supported: {0}")]
    ExprNotSupported(String),

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("add on non numeric value")]
    AddOnNonNumeric,

    #[error("subtract on non numeric value")]
    SubtractOnNonNumeric,

    #[error("multiply on non numeric value")]
    MultiplyOnNonNumeric,

    #[error("divide on non numeric value")]
    DivideOnNonNumeric,

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("floating numbers cannot be grouped by")]
    FloatCannotBeGroupedBy,

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    OptBool(Option<bool>),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptStr(Option<String>),
    Empty,
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r))
            | (Value::OptBool(Some(l)), Value::Bool(r))
            | (Value::Bool(l), Value::OptBool(Some(r)))
            | (Value::OptBool(Some(l)), Value::OptBool(Some(r))) => l == r,
            (Value::I64(l), Value::I64(r))
            | (Value::OptI64(Some(l)), Value::I64(r))
            | (Value::I64(l), Value::OptI64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptI64(Some(r))) => l == r,
            (Value::F64(l), Value::F64(r))
            | (Value::OptF64(Some(l)), Value::F64(r))
            | (Value::F64(l), Value::OptF64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptF64(Some(r))) => l == r,
            (Value::Str(l), Value::Str(r))
            | (Value::OptStr(Some(l)), Value::Str(r))
            | (Value::Str(l), Value::OptStr(Some(r)))
            | (Value::OptStr(Some(l)), Value::OptStr(Some(r))) => l == r,
            (Value::OptBool(None), Value::OptBool(None))
            | (Value::OptI64(None), Value::OptI64(None))
            | (Value::OptF64(None), Value::OptF64(None))
            | (Value::OptStr(None), Value::OptStr(None))
            | (Value::Empty, Value::Empty) => true,
            _ => false,
        }
    }
}

impl PartialEq<AstValue> for Value {
    fn eq(&self, other: &AstValue) -> bool {
        match (self, other) {
            (Value::Bool(l), AstValue::Boolean(r))
            | (Value::OptBool(Some(l)), AstValue::Boolean(r)) => l == r,
            (Value::I64(l), AstValue::Number(r))
            | (Value::OptI64(Some(l)), AstValue::Number(r)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64) == r,
                    Err(_) => false,
                },
            },
            (Value::F64(l), AstValue::Number(r))
            | (Value::OptF64(Some(l)), AstValue::Number(r)) => match r.parse::<f64>() {
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

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Value::I64(r))
            | (Value::OptI64(Some(l)), Value::I64(r))
            | (Value::I64(l), Value::OptI64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptI64(Some(r))) => Some(l.cmp(r)),
            (Value::I64(l), Value::F64(r))
            | (Value::OptI64(Some(l)), Value::F64(r))
            | (Value::I64(l), Value::OptF64(Some(r)))
            | (Value::OptI64(Some(l)), Value::OptF64(Some(r))) => (*l as f64).partial_cmp(r),
            (Value::F64(l), Value::F64(r))
            | (Value::OptF64(Some(l)), Value::F64(r))
            | (Value::F64(l), Value::OptF64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptF64(Some(r))) => l.partial_cmp(r),
            (Value::F64(l), Value::I64(r))
            | (Value::OptF64(Some(l)), Value::I64(r))
            | (Value::F64(l), Value::OptI64(Some(r)))
            | (Value::OptF64(Some(l)), Value::OptI64(Some(r))) => l.partial_cmp(&(*r as f64)),
            (Value::Str(l), Value::Str(r))
            | (Value::OptStr(Some(l)), Value::Str(r))
            | (Value::Str(l), Value::OptStr(Some(r)))
            | (Value::OptStr(Some(l)), Value::OptStr(Some(r))) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

impl PartialOrd<AstValue> for Value {
    fn partial_cmp(&self, other: &AstValue) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), AstValue::Number(r))
            | (Value::OptI64(Some(l)), AstValue::Number(r)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64).partial_cmp(&r),
                    Err(_) => None,
                },
            },
            (Value::F64(l), AstValue::Number(r))
            | (Value::OptF64(Some(l)), AstValue::Number(r)) => match r.parse::<f64>() {
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
            AstValue::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            AstValue::Boolean(v) => Ok(Value::Bool(*v)),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }
}

impl TryInto<GroupKey> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) | OptBool(Some(v)) => Ok(GroupKey::Bool(*v)),
            I64(v) | OptI64(Some(v)) => Ok(GroupKey::I64(*v)),
            Str(v) | OptStr(Some(v)) => Ok(GroupKey::Str(v.clone())),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(GroupKey::Null),
            F64(_) | OptF64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}

impl TryInto<GroupKey> for Value {
    type Error = Error;

    fn try_into(self) -> Result<GroupKey> {
        use Value::*;

        match self {
            Bool(v) | OptBool(Some(v)) => Ok(GroupKey::Bool(v)),
            I64(v) | OptI64(Some(v)) => Ok(GroupKey::I64(v)),
            Str(v) | OptStr(Some(v)) => Ok(GroupKey::Str(v)),
            Empty | OptBool(None) | OptI64(None) | OptStr(None) => Ok(GroupKey::Null),
            F64(_) | OptF64(_) => Err(ValueError::FloatCannotBeGroupedBy.into()),
        }
    }
}

trait BoolToValue: Sized {
    fn as_value(self, v1: Value, v2: Value) -> Value;
}

impl BoolToValue for bool {
    #[inline]
    fn as_value(self, v1: Value, v2: Value) -> Value {
        if self {
            v1
        } else {
            v2
        }
    }
}

impl Value {
    pub fn from_expr(data_type: &DataType, nullable: bool, expr: &Expr) -> Result<Self> {
        match expr {
            Expr::Value(literal) => Value::from_data_type(&data_type, nullable, literal),
            Expr::Identifier(Ident { value, .. }) => Ok(Value::Str(value.clone())),
            _ => Err(ValueError::ExprNotSupported(expr.to_string()).into()),
        }
    }

    pub fn from_data_type(
        data_type: &DataType,
        nullable: bool,
        literal: &AstValue,
    ) -> Result<Self> {
        match (data_type, literal) {
            (DataType::Int, AstValue::Number(v)) => v
                .parse()
                .map(|v| nullable.as_value(Value::OptI64(Some(v)), Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float(_), AstValue::Number(v)) => v
                .parse()
                .map(|v| nullable.as_value(Value::OptF64(Some(v)), Value::F64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Boolean, AstValue::Boolean(v)) => {
                Ok(nullable.as_value(Value::OptBool(Some(*v)), Value::Bool(*v)))
            }
            (DataType::Int, AstValue::Null) => nullable.as_result(
                Value::OptI64(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Float(_), AstValue::Null) => nullable.as_result(
                Value::OptF64(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Boolean, AstValue::Null) => nullable.as_result(
                Value::OptBool(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            (DataType::Text, AstValue::Null) => nullable.as_result(
                Value::OptStr(None),
                ValueError::NullValueOnNotNullField.into(),
            ),
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn clone_by(&self, literal: &AstValue) -> Result<Self> {
        match (self, literal) {
            (Value::I64(_), AstValue::Number(v)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptI64(_), AstValue::Number(v)) => v
                .parse()
                .map(|v| Value::OptI64(Some(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptI64(_), AstValue::Null) => Ok(Value::OptI64(None)),
            (Value::F64(_), AstValue::Number(v)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptF64(_), AstValue::Number(v)) => v
                .parse()
                .map(|v| Value::OptF64(Some(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::OptF64(_), AstValue::Null) => Ok(Value::OptF64(None)),
            (Value::Str(_), AstValue::SingleQuotedString(v)) => Ok(Value::Str(v.clone())),
            (Value::OptStr(_), AstValue::SingleQuotedString(v)) => {
                Ok(Value::OptStr(Some(v.clone())))
            }
            (Value::OptStr(_), AstValue::Null) => Ok(Value::OptStr(None)),
            (Value::Bool(_), AstValue::Boolean(v)) => Ok(Value::Bool(*v)),
            (Value::OptBool(_), AstValue::Boolean(v)) => Ok(Value::OptBool(Some(*v))),
            (Value::OptBool(_), AstValue::Null) => Ok(Value::OptBool(None)),
            _ => Err(ValueError::LiteralNotSupported.into()),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a + b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a + b))),
            (OptI64(Some(a)), OptI64(Some(b))) => Ok(OptI64(Some(a + b))),
            (F64(a), F64(b)) => Ok(F64(a + b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a + b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::AddOnNonNumeric.into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a - b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a - b))),
            (F64(a), F64(b)) => Ok(F64(a - b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a - b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a * b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a * b))),
            (F64(a), F64(b)) => Ok(F64(a * b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a * b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::MultiplyOnNonNumeric.into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a / b)),
            (I64(a), OptI64(Some(b))) | (OptI64(Some(a)), I64(b)) => Ok(OptI64(Some(a / b))),
            (F64(a), F64(b)) => Ok(F64(a / b)),
            (F64(a), OptF64(Some(b))) | (OptF64(Some(a)), F64(b)) => Ok(OptF64(Some(a / b))),
            (OptI64(None), OptI64(_))
            | (OptI64(_), OptI64(None))
            | (OptI64(None), I64(_))
            | (I64(_), OptI64(None)) => Ok(OptI64(None)),
            (OptF64(_), OptF64(None))
            | (OptF64(None), OptF64(_))
            | (F64(_), OptF64(None))
            | (OptF64(None), F64(_)) => Ok(OptF64(None)),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }

    pub fn is_some(&self) -> bool {
        use Value::*;

        !matches!(
            self,
            Empty | OptBool(None) | OptI64(None) | OptF64(None) | OptStr(None)
        )
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(_) | OptI64(_) | F64(_) | OptF64(_) => Ok(self.clone()),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(a) => Ok(I64(-a)),
            OptI64(Some(a)) => Ok(OptI64(Some(-a))),
            F64(a) => Ok(F64(-a)),
            OptF64(Some(a)) => Ok(OptF64(Some(-a))),
            OptI64(None) => Ok(OptI64(None)),
            OptF64(None) => Ok(OptF64(None)),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn eq() {
        assert_eq!(Value::Bool(true), Value::OptBool(Some(true)));
        assert_eq!(Value::OptBool(Some(false)), Value::Bool(false));
        assert_eq!(Value::OptBool(None), Value::OptBool(None));

        assert_eq!(Value::I64(1), Value::OptI64(Some(1)));
        assert_eq!(Value::OptI64(Some(1)), Value::I64(1));
        assert_eq!(Value::OptI64(None), Value::OptI64(None));

        assert_eq!(Value::F64(6.11), Value::OptF64(Some(6.11)));
        assert_eq!(Value::OptF64(Some(6.11)), Value::F64(6.11));
        assert_eq!(Value::OptF64(None), Value::OptF64(None));

        let glue = || "Glue".to_owned();

        assert_eq!(Value::Str(glue()), Value::OptStr(Some(glue())));
        assert_eq!(Value::OptStr(Some(glue())), Value::Str(glue()));
        assert_eq!(Value::OptStr(None), Value::OptStr(None));

        assert_eq!(Value::Empty, Value::Empty);
    }
}
