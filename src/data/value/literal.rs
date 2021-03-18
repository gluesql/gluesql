use {
    super::{error::ValueError, Value},
    crate::{
        data::Literal,
        result::{Error, Result},
    },
    sqlparser::ast::DataType,
    std::{cmp::Ordering, convert::TryFrom},
};

impl PartialEq<Literal<'_>> for Value {
    fn eq(&self, other: &Literal<'_>) -> bool {
        match (self, other) {
            (Value::Bool(l), Literal::Boolean(r)) => l == r,
            (Value::I64(l), Literal::Number(r)) => match r.parse::<i64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64) == r,
                    Err(_) => false,
                },
            },
            (Value::F64(l), Literal::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l == &r,
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => *l == (r as f64),
                    Err(_) => false,
                },
            },
            (Value::Str(l), Literal::Text(r)) => l == r.as_ref(),
            _ => false,
        }
    }
}

impl PartialOrd<Literal<'_>> for Value {
    fn partial_cmp(&self, other: &Literal<'_>) -> Option<Ordering> {
        match (self, other) {
            (Value::I64(l), Literal::Number(r)) => match r.parse::<i64>() {
                Ok(r) => Some(l.cmp(&r)),
                Err(_) => match r.parse::<f64>() {
                    Ok(r) => (*l as f64).partial_cmp(&r),
                    Err(_) => None,
                },
            },
            (Value::F64(l), Literal::Number(r)) => match r.parse::<f64>() {
                Ok(r) => l.partial_cmp(&r),
                Err(_) => match r.parse::<i64>() {
                    Ok(r) => l.partial_cmp(&(r as f64)),
                    Err(_) => None,
                },
            },
            (Value::Str(l), Literal::Text(r)) => Some(l.cmp(r.as_ref())),
            _ => None,
        }
    }
}

impl TryFrom<&Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: &Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            Literal::Boolean(v) => Ok(Value::Bool(*v)),
            Literal::Text(v) => Ok(Value::Str(v.as_ref().to_owned())),
            Literal::Null => Ok(Value::Null),
        }
    }
}

impl TryFrom<Literal<'_>> for Value {
    type Error = Error;

    fn try_from(literal: Literal<'_>) -> Result<Self> {
        match literal {
            Literal::Number(v) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            Literal::Boolean(v) => Ok(Value::Bool(v)),
            Literal::Text(v) => Ok(Value::Str(v.into_owned())),
            Literal::Null => Ok(Value::Null),
        }
    }
}

pub trait TryFromLiteral {
    fn try_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value>;

    fn try_cast_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value>;
}

impl TryFromLiteral for Value {
    fn try_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Int, Literal::Number(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float(_), Literal::Number(v)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::UnreachableNumberParsing.into()),
            (DataType::Text, Literal::Text(v)) => Ok(Value::Str(v.to_string())),
            (DataType::Boolean, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Float(_), Literal::Null)
            | (DataType::Text, Literal::Null) => Ok(Value::Null),
            _ => Err(ValueError::IncompatibleLiteralForDataType {
                data_type: data_type.to_string(),
                literal: format!("{:?}", literal),
            }
            .into()),
        }
    }

    fn try_cast_from_literal(data_type: &DataType, literal: &Literal<'_>) -> Result<Value> {
        match (data_type, literal) {
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Boolean, Literal::Text(v)) | (DataType::Boolean, Literal::Number(v)) => {
                match v.to_uppercase().as_str() {
                    "TRUE" | "1" => Ok(Value::Bool(true)),
                    "FALSE" | "0" => Ok(Value::Bool(false)),
                    _ => Err(ValueError::LiteralCastToBooleanFailed(v.to_string()).into()),
                }
            }
            (DataType::Int, Literal::Text(v)) => v
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::LiteralCastFromTextToIntegerFailed(v.to_string()).into()),
            (DataType::Int, Literal::Number(v)) => v
                .parse::<f64>()
                .map_err(|_| {
                    ValueError::UnreachableLiteralCastFromNumberToInteger(v.to_string()).into()
                })
                .map(|v| Value::I64(v.trunc() as i64)),
            (DataType::Int, Literal::Boolean(v)) => {
                let v = if *v { 1 } else { 0 };

                Ok(Value::I64(v))
            }
            (DataType::Float(_), Literal::Text(v)) | (DataType::Float(_), Literal::Number(v)) => v
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::LiteralCastToFloatFailed(v.to_string()).into()),
            (DataType::Float(_), Literal::Boolean(v)) => {
                let v = if *v { 1.0 } else { 0.0 };

                Ok(Value::F64(v))
            }
            (DataType::Text, Literal::Number(v)) | (DataType::Text, Literal::Text(v)) => {
                Ok(Value::Str(v.to_string()))
            }
            (DataType::Text, Literal::Boolean(v)) => {
                let v = if *v { "TRUE" } else { "FALSE" };

                Ok(Value::Str(v.to_owned()))
            }
            (DataType::Boolean, Literal::Null)
            | (DataType::Int, Literal::Null)
            | (DataType::Float(_), Literal::Null)
            | (DataType::Text, Literal::Null) => Ok(Value::Null),
            _ => Err(ValueError::UnimplementedLiteralCast {
                data_type: data_type.to_string(),
                literal: format!("{:?}", literal),
            }
            .into()),
        }
    }
}
