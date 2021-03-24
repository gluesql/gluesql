use {
    super::Literal,
    crate::result::Result,
    into::TryInto,
    serde::{Deserialize, Serialize},
    sqlparser::ast::{DataType, Expr},
    std::{cmp::Ordering, convert::TryFrom, fmt::Debug},
};

mod error;
mod group_key;
mod into;
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
        let literal = Literal::try_from(expr)?;
        let value = Value::try_from_literal(&data_type, &literal)?;

        value.validate_null(nullable)?;

        Ok(value)
    }

    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        let valid = matches!(
            (data_type, self),
            (DataType::Boolean, Value::Bool(_))
                | (DataType::Int, Value::I64(_))
                | (DataType::Float(_), Value::F64(_))
                | (DataType::Text, Value::Str(_))
                | (DataType::Boolean, Value::Null)
                | (DataType::Int, Value::Null)
                | (DataType::Float(_), Value::Null)
                | (DataType::Text, Value::Null)
        );

        if !valid {
            return Err(ValueError::IncompatibleDataType {
                data_type: data_type.to_string(),
                value: format!("{:?}", self),
            }
            .into());
        }

        Ok(())
    }

    pub fn validate_null(&self, nullable: bool) -> Result<()> {
        if !nullable && matches!(self, Value::Null) {
            return Err(ValueError::NullValueOnNotNullField.into());
        }

        Ok(())
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Self> {
        match (data_type, self) {
            (DataType::Boolean, Value::Bool(_))
            | (DataType::Int, Value::I64(_))
            | (DataType::Float(_), Value::F64(_))
            | (DataType::Text, Value::Str(_)) => Ok(self.clone()),
            (_, Value::Null) => Ok(Value::Null),

            (DataType::Boolean, value) => value.try_into().map(Value::Bool),
            (DataType::Int, value) => value.try_into().map(Value::I64),
            (DataType::Float(_), value) => value.try_into().map(Value::F64),
            (DataType::Text, value) => value.try_into().map(Value::Str),

            _ => Err(ValueError::UnimplementedCast.into()),
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
