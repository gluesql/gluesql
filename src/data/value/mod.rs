use {
    crate::result::Result,
    boolinator::Boolinator,
    serde::{Deserialize, Serialize},
    sqlparser::ast::{DataType, Expr, Ident, Value as Literal},
    std::{cmp::Ordering, fmt::Debug},
};

mod error;
mod group_key;
mod literal;
mod unique_key;

pub use error::ValueError;
pub use literal::TryFromLiteral;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Null,
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::I64(l), Value::I64(r)) => l == r,
            (Value::F64(l), Value::F64(r)) => l == r,
            (Value::Str(l), Value::Str(r)) => l == r,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
            (Value::I64(l), Value::I64(r)) => Some(l.cmp(r)),
            (Value::I64(l), Value::F64(r)) => (*l as f64).partial_cmp(r),
            (Value::F64(l), Value::I64(r)) => l.partial_cmp(&(*r as f64)),
            (Value::F64(l), Value::F64(r)) => l.partial_cmp(r),
            (Value::Str(l), Value::Str(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }
}

trait BoolToValue: Sized {
    fn into_value(self, v1: Value, v2: Value) -> Value;
}

impl BoolToValue for bool {
    #[inline]
    fn into_value(self, v1: Value, v2: Value) -> Value {
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

    pub fn is_same_as_data_type(&self, data_type: &DataType) -> bool {
        matches!(
            (data_type, self),
            (DataType::Boolean, Value::Bool(_))
                | (DataType::Int, Value::I64(_))
                | (DataType::Float(_), Value::F64(_))
                | (DataType::Text, Value::Str(_))
                | (DataType::Boolean, Value::Null)
                | (DataType::Int, Value::Null)
                | (DataType::Float(_), Value::Null)
                | (DataType::Text, Value::Null)
        )
    }

    pub fn from_data_type(data_type: &DataType, nullable: bool, literal: &Literal) -> Result<Self> {
        match (data_type, literal) {
            (DataType::Int, Literal::Number(v, false)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Float(_), Literal::Number(v, false)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (DataType::Boolean, Literal::Boolean(v)) => Ok(Value::Bool(*v)),
            (DataType::Text, Literal::SingleQuotedString(v)) => Ok(Value::Str(v.clone())),
            (DataType::Int, Literal::Null)
            | (DataType::Float(_), Literal::Null)
            | (DataType::Boolean, Literal::Null)
            | (DataType::Text, Literal::Null) => {
                nullable.as_result(Value::Null, ValueError::NullValueOnNotNullField.into())
            }
            _ => Err(ValueError::SqlTypeNotSupported.into()),
        }
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Self> {
        match (data_type, self) {
            // Same as
            (DataType::Boolean, Value::Bool(_))
            | (DataType::Int, Value::I64(_))
            | (DataType::Float(_), Value::F64(_))
            | (DataType::Text, Value::Str(_)) => Ok(self.clone()),

            // Null
            (DataType::Boolean, Value::Null)
            | (DataType::Int, Value::Null)
            | (DataType::Float(_), Value::Null)
            | (DataType::Text, Value::Null) => Ok(Value::Null),

            // Boolean
            (DataType::Boolean, Value::Str(value)) => match value.to_uppercase().as_str() {
                "TRUE" => Ok(Value::Bool(true)),
                "FALSE" => Ok(Value::Bool(false)),
                _ => Err(ValueError::ImpossibleCast.into()),
            },
            (DataType::Boolean, Value::I64(value)) => match value {
                1 => Ok(Value::Bool(true)),
                0 => Ok(Value::Bool(false)),
                _ => Err(ValueError::ImpossibleCast.into()),
            },
            (DataType::Boolean, Value::F64(value)) => {
                if value.eq(&1.0) {
                    Ok(Value::Bool(true))
                } else if value.eq(&0.0) {
                    Ok(Value::Bool(false))
                } else {
                    Err(ValueError::ImpossibleCast.into())
                }
            }

            // Integer
            (DataType::Int, Value::Bool(value)) => Ok(Value::I64(if *value { 1 } else { 0 })),
            (DataType::Int, Value::F64(value)) => Ok(Value::I64(value.trunc() as i64)),
            (DataType::Int, Value::Str(value)) => value
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| ValueError::ImpossibleCast.into()),

            // Float
            (DataType::Float(_), Value::Bool(value)) => {
                Ok(Value::F64(if *value { 1.0 } else { 0.0 }))
            }
            (DataType::Float(_), Value::I64(value)) => Ok(Value::F64((*value as f64).trunc())),
            (DataType::Float(_), Value::Str(value)) => value
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| ValueError::ImpossibleCast.into()),

            // Text
            (DataType::Text, Value::Bool(value)) => Ok(Value::Str(
                (if *value { "TRUE" } else { "FALSE" }).to_string(),
            )),
            (DataType::Text, Value::I64(value)) => Ok(Value::Str(value.to_string())),
            (DataType::Text, Value::F64(value)) => Ok(Value::Str(value.to_string())),

            _ => Err(ValueError::UnimplementedCast.into()),
        }
    }

    pub fn clone_by(&self, literal: &Literal) -> Result<Self> {
        match (self, literal) {
            (Value::I64(_), Literal::Number(v, false)) => v
                .parse()
                .map(Value::I64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::F64(_), Literal::Number(v, false)) => v
                .parse()
                .map(Value::F64)
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (Value::Str(_), Literal::SingleQuotedString(v))
            | (Value::Null, Literal::SingleQuotedString(v)) => Ok(Value::Str(v.clone())),
            (Value::Bool(_), Literal::Boolean(v)) | (Value::Null, Literal::Boolean(v)) => {
                Ok(Value::Bool(*v))
            }
            (Value::Null, Literal::Number(v, false)) => v
                .parse::<i64>()
                .map_or_else(|_| v.parse::<f64>().map(Value::F64), |v| Ok(Value::I64(v)))
                .map_err(|_| ValueError::FailedToParseNumber.into()),
            (_, Literal::Null) => Ok(Value::Null),
            _ => Err(ValueError::LiteralNotSupported.into()),
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a + b)),
            (F64(a), F64(b)) => Ok(F64(a + b)),
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(ValueError::AddOnNonNumeric.into()),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a - b)),
            (F64(a), F64(b)) => Ok(F64(a - b)),
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(ValueError::SubtractOnNonNumeric.into()),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a * b)),
            (F64(a), F64(b)) => Ok(F64(a * b)),
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(ValueError::MultiplyOnNonNumeric.into()),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        use Value::*;

        match (self, other) {
            (I64(a), I64(b)) => Ok(I64(a / b)),
            (F64(a), F64(b)) => Ok(F64(a / b)),
            (Null, _) | (_, Null) => Ok(Null),
            _ => Err(ValueError::DivideOnNonNumeric.into()),
        }
    }

    pub fn is_some(&self) -> bool {
        use Value::*;

        !matches!(self, Null)
    }

    pub fn unary_plus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(_) | F64(_) => Ok(self.clone()),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryPlusOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Value> {
        use Value::*;

        match self {
            I64(a) => Ok(I64(-a)),
            F64(a) => Ok(F64(-a)),
            Null => Ok(Null),
            _ => Err(ValueError::UnaryMinusOnNonNumeric.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value::*;

    #[test]
    fn eq() {
        assert_ne!(Null, Null);
        assert_eq!(Bool(true), Bool(true));
        assert_eq!(I64(1), I64(1));
        assert_eq!(F64(6.11), F64(6.11));
        assert_eq!(Str("Glue".to_owned()), Str("Glue".to_owned()));
    }

    #[test]
    fn cast() {
        use sqlparser::ast::DataType::*;

        macro_rules! cast {
            ($input: expr => $data_type: expr, $expected: expr) => {
                let found = $input.cast(&$data_type).unwrap();

                match ($expected, found) {
                    (Null, Null) => {}
                    (expected, found) => {
                        assert_eq!(expected, found);
                    }
                }
            };
        }

        // Same as
        cast!(Bool(true)            => Boolean      , Bool(true));
        cast!(Str("a".to_owned())   => Text         , Str("a".to_owned()));
        cast!(I64(1)                => Int          , I64(1));
        cast!(F64(1.0)              => Float(None)  , F64(1.0));

        // Boolean
        cast!(Str("TRUE".to_owned())    => Boolean, Bool(true));
        cast!(Str("FALSE".to_owned())   => Boolean, Bool(false));
        cast!(I64(1)                    => Boolean, Bool(true));
        cast!(I64(0)                    => Boolean, Bool(false));
        cast!(F64(1.0)                  => Boolean, Bool(true));
        cast!(F64(0.0)                  => Boolean, Bool(false));
        cast!(Null                      => Boolean, Null);

        // Integer
        cast!(Bool(true)            => Int, I64(1));
        cast!(Bool(false)           => Int, I64(0));
        cast!(F64(1.1)              => Int, I64(1));
        cast!(Str("11".to_owned())  => Int, I64(11));
        cast!(Null                  => Int, Null);

        // Float
        cast!(Bool(true)            => Float(None), F64(1.0));
        cast!(Bool(false)           => Float(None), F64(0.0));
        cast!(I64(1)                => Float(None), F64(1.0));
        cast!(Str("11".to_owned())  => Float(None), F64(11.0));
        cast!(Null                  => Float(None), Null);

        // Text
        cast!(Bool(true)    => Text, Str("TRUE".to_owned()));
        cast!(Bool(false)   => Text, Str("FALSE".to_owned()));
        cast!(I64(11)       => Text, Str("11".to_owned()));
        cast!(F64(1.0)      => Text, Str("1".to_owned()));
        cast!(Null          => Text, Null);
    }
}
