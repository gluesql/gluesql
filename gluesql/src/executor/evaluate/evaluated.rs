// ...existing code...

use std::convert::TryInto;
use crate::data::{Value, Literal};
use crate::result::{Result, Error};

// ...existing code...

impl<'a> TryInto<f64> for Evaluated<'a> {
    type Error = Error;

    fn try_into(self) -> Result<f64> {
        match self {
            Evaluated::Literal(literal) => match literal {
                Literal::Number(n) => n.parse::<f64>().map_err(|e| Error::Parse(e.to_string())),
                Literal::Text(s) => s.parse::<f64>().map_err(|e| Error::Parse(e.to_string())),
                _ => Err(Error::InvalidType("Cannot convert to f64".to_owned())),
            },
            Evaluated::Value(value) => match value {
                Value::F64(f) => Ok(f),
                Value::I64(i) => Ok(i as f64),
                Value::Str(s) => s.parse::<f64>().map_err(|e| Error::Parse(e.to_string())),
                _ => Err(Error::InvalidType("Cannot convert to f64".to_owned())),
            },
        }
    }
}

impl<'a> TryInto<i64> for Evaluated<'a> {
    type Error = Error;

    fn try_into(self) -> Result<i64> {
        match self {
            Evaluated::Literal(literal) => match literal {
                Literal::Number(n) => n.parse::<i64>().map_err(|e| Error::Parse(e.to_string())),
                Literal::Text(s) => s.parse::<i64>().map_err(|e| Error::Parse(e.to_string())),
                _ => Err(Error::InvalidType("Cannot convert to i64".to_owned())),
            },
            Evaluated::Value(value) => match value {
                Value::I64(i) => Ok(i),
                Value::F64(f) => {
                    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                        Ok(f as i64)
                    } else {
                        Err(Error::InvalidType("Cannot convert float to i64".to_owned()))
                    }
                }
                Value::Str(s) => s.parse::<i64>().map_err(|e| Error::Parse(e.to_string())),
                _ => Err(Error::InvalidType("Cannot convert to i64".to_owned())),
            },
            Evaluated::Value(value) => match value {
                Value::Str(s) => Ok(s.into_owned()),
                Value::I64(i) => Ok(i.to_string()),
                Value::F64(f) => Ok(f.to_string()),
                Value::Bool(b) => Ok(b.to_string()),
                Value::Null => Ok("NULL".to_owned()),
                // ...handle other Value variants if any...
            },
        }
    }
}

// ...existing code...
            },
        }
    }
}

impl<'a> TryInto<String> for Evaluated<'a> {
    type Error = Error;

    fn try_into(self) -> Result<String> {
        match self {
            Evaluated::Literal(literal) => match literal {
                Literal::Text(s) => Ok(s),
                Literal::Number(n) => Ok(n),
                Literal::Boolean(b) => Ok(b.to_string()),
                Literal::Null => Ok("NULL".to_owned()),
                // ...handle other Literal variants if any...

