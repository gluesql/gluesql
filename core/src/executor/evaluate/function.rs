use {
    super::{EvaluateError, Evaluated},
    crate::{ast::TrimWhereField, data::Value, result::Result},
    std::cmp::{max, min},
    uuid::Uuid,
};

macro_rules! eval_to_str {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::Str(value) => value,
            Value::Null => {
                return Ok(Value::Null);
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresStringValue($name).into());
            }
        }
    };
}

macro_rules! eval_to_int {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::I64(num) => num,
            Value::Null => {
                return Ok(Value::Null);
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresIntegerValue($name).into());
            }
        }
    };
}

macro_rules! eval_to_float {
    ($name: expr, $evaluated: expr) => {
        match $evaluated.try_into()? {
            Value::I64(v) => v as f64,
            Value::F64(v) => v,
            Value::Null => {
                return Ok(Value::Null);
            }
            _ => {
                return Err(EvaluateError::FunctionRequiresFloatValue($name).into());
            }
        }
    };
}

// --- text ---

pub fn concat(exprs: Vec<Evaluated<'_>>) -> Result<Value> {
    exprs
        .into_iter()
        .map(|expr| expr.try_into())
        .filter(|value| !matches!(value, Ok(Value::Null)))
        .try_fold(Value::Str("".to_owned()), |left, right| {
            Ok(left.concat(&right?))
        })
}

pub fn lower(name: String, expr: Evaluated<'_>) -> Result<Value> {
    Ok(Value::Str(eval_to_str!(name, expr).to_lowercase()))
}

pub fn upper(name: String, expr: Evaluated<'_>) -> Result<Value> {
    Ok(Value::Str(eval_to_str!(name, expr).to_uppercase()))
}

pub fn left_or_right(name: String, expr: Evaluated<'_>, size: Evaluated<'_>) -> Result<Value> {
    let string = eval_to_str!(name, expr);
    let size = match size.try_into()? {
        Value::I64(number) => usize::try_from(number)
            .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()))?,
        Value::Null => {
            return Ok(Value::Null);
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    let converted = if name == "LEFT" {
        string.get(..size).map(|v| v.to_string()).unwrap_or(string)
    } else {
        let start_pos = if size > string.len() {
            0
        } else {
            string.len() - size
        };

        string
            .get(start_pos..)
            .map(|value| value.to_string())
            .unwrap_or(string)
    };

    Ok(Value::Str(converted))
}

pub fn lpad_or_rpad(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
    fill: Option<Evaluated<'_>>,
) -> Result<Value> {
    let string = eval_to_str!(name, expr);
    let size = match size.try_into()? {
        Value::I64(number) => usize::try_from(number)
            .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()))?,
        Value::Null => {
            return Ok(Value::Null);
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresIntegerValue(name).into());
        }
    };

    let fill = match fill {
        Some(expr) => eval_to_str!(name, expr),
        None => " ".to_owned(),
    };

    let result = if size > string.len() {
        let padding_size = size - string.len();
        let repeat_count = padding_size / fill.len();
        let plus_count = padding_size % fill.len();
        let fill = fill.repeat(repeat_count) + &fill[0..plus_count];

        if name == "LPAD" {
            fill + &string
        } else {
            string + &fill
        }
    } else {
        string[0..size].to_string()
    };

    Ok(Value::Str(result))
}

pub fn trim(
    name: String,
    expr: Evaluated<'_>,
    filter_chars: Option<Evaluated<'_>>,
    trim_where_field: &Option<TrimWhereField>,
) -> Result<Value> {
    let expr_str = eval_to_str!(name, expr);
    let expr_str = expr_str.as_str();
    let filter_chars = match filter_chars {
        Some(expr) => eval_to_str!(name, expr).chars().collect::<Vec<_>>(),
        None => vec![' '],
    };

    let value = match trim_where_field {
        Some(TrimWhereField::Both) => expr_str.trim_matches(&filter_chars[..]),
        Some(TrimWhereField::Leading) => expr_str.trim_start_matches(&filter_chars[..]),
        Some(TrimWhereField::Trailing) => expr_str.trim_end_matches(&filter_chars[..]),
        None => expr_str.trim(),
    };

    Ok(Value::Str(value.to_owned()))
}

pub fn ltrim(name: String, expr: Evaluated<'_>, chars: Option<Evaluated<'_>>) -> Result<Value> {
    let expr = eval_to_str!(name, expr);
    let chars = match chars {
        Some(chars) => eval_to_str!(name, chars).chars().collect::<Vec<char>>(),
        None => vec![' '],
    };

    let value = expr.trim_start_matches(chars.as_slice()).to_string();
    Ok(Value::Str(value))
}

pub fn rtrim(name: String, expr: Evaluated<'_>, chars: Option<Evaluated<'_>>) -> Result<Value> {
    let expr = eval_to_str!(name, expr);
    let chars = match chars {
        Some(chars) => eval_to_str!(name, chars).chars().collect::<Vec<char>>(),
        None => vec![' '],
    };

    let value = expr.trim_end_matches(chars.as_slice()).to_string();
    Ok(Value::Str(value))
}

pub fn reverse(name: String, expr: Evaluated<'_>) -> Result<Value> {
    let value = eval_to_str!(name, expr).chars().rev().collect::<String>();

    Ok(Value::Str(value))
}

pub fn repeat(name: String, expr: Evaluated<'_>, num: Evaluated<'_>) -> Result<Value> {
    let expr = eval_to_str!(name, expr);
    let num = eval_to_int!(name, num) as usize;
    let value = expr.repeat(num);

    Ok(Value::Str(value))
}

pub fn substr(
    name: String,
    expr: Evaluated<'_>,
    start: Evaluated<'_>,
    count: Option<Evaluated<'_>>,
) -> Result<Value> {
    let string = eval_to_str!(name, expr);
    let start = eval_to_int!(name, start) - 1;
    let count = match count {
        Some(v) => eval_to_int!(name, v),
        None => string.len() as i64,
    };

    let end = if count < 0 {
        return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
    } else {
        min(max(start + count, 0) as usize, string.len())
    };

    let start = min(max(start, 0) as usize, string.len());
    let string = String::from(&string[start..end]);
    Ok(Value::Str(string))
}

// --- float ---

pub fn abs(name: String, n: Evaluated<'_>) -> Result<Value> {
    match n.try_into()? {
        Value::I8(v) => Ok(Value::I8(v.abs())),
        Value::I64(v) => Ok(Value::I64(v.abs())),
        Value::Decimal(v) => Ok(Value::Decimal(v.abs())),
        Value::F64(v) => Ok(Value::F64(v.abs())),
        Value::Null => Ok(Value::Null),
        _ => Err(EvaluateError::FunctionRequiresFloatValue(name).into()),
    }
}

pub fn ifnull(expr: Evaluated<'_>, then: Evaluated<'_>) -> Result<Value> {
    Ok(match expr {
        Evaluated::Value(v) => match v.is_null() {
            true => then.try_into()?,
            false => v.into_owned(),
        },
        Evaluated::Literal(l) => match l {
            Literal::Null => then.try_into()?,
            _ => l.try_into_value()?,
        },
    })
    */
}

pub fn sign(name: String, n: Evaluated<'_>) -> Result<Value> {
    let x = eval_to_float!(name, n);
    if x == 0.0 {
        return Ok(Value::I8(0));
    }
    Ok(Value::I8(x.signum() as i8))
}

pub fn sqrt(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).sqrt()))
}

pub fn power(name: String, expr: Evaluated<'_>, power: Evaluated<'_>) -> Result<Value> {
    let expr = eval_to_float!(name, expr);
    let power = eval_to_float!(name, power);

    Ok(Value::F64(expr.powf(power)))
}

pub fn ceil(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).ceil()))
}

pub fn round(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).round()))
}

pub fn floor(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).floor()))
}

pub fn radians(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).to_radians()))
}

pub fn degrees(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).to_degrees()))
}

pub fn exp(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).exp()))
}

pub fn log(name: String, antilog: Evaluated<'_>, base: Evaluated<'_>) -> Result<Value> {
    let antilog = eval_to_float!(name, antilog);
    let base = eval_to_float!(name, base);

    Ok(Value::F64(antilog.log(base)))
}

pub fn ln(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).ln()))
}

pub fn log2(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).log2()))
}

pub fn log10(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).log10()))
}

pub fn sin(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).sin()))
}

pub fn cos(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).cos()))
}

pub fn tan(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).tan()))
}

pub fn asin(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).asin()))
}

pub fn acos(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).acos()))
}

pub fn atan(name: String, n: Evaluated<'_>) -> Result<Value> {
    Ok(Value::F64(eval_to_float!(name, n).atan()))
}

// --- integer ---

pub fn div(name: String, dividend: Evaluated<'_>, divisor: Evaluated<'_>) -> Result<Value> {
    let dividend = match dividend.try_into()? {
        Value::F64(number) => number,
        Value::I64(number) => number as f64,
        Value::Null => {
            return Ok(Value::Null);
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(name).into());
        }
    };

    let divisor = match divisor.try_into()? {
        Value::F64(number) => match number {
            x if x == 0.0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
            _ => number,
        },
        Value::I64(number) => match number {
            0 => return Err(EvaluateError::DivisorShouldNotBeZero.into()),
            _ => number as f64,
        },
        Value::Null => {
            return Ok(Value::Null);
        }
        _ => {
            return Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(name).into());
        }
    };

    Ok(Value::I64((dividend / divisor) as i64))
}

pub fn gcd(name: String, left: Evaluated<'_>, right: Evaluated<'_>) -> Result<Value> {
    let left = eval_to_int!(name, left);
    let right = eval_to_int!(name, right);

    Ok(Value::I64(gcd_i64(left, right)))
}

pub fn lcm(name: String, left: Evaluated<'_>, right: Evaluated<'_>) -> Result<Value> {
    let left = eval_to_int!(name, left);
    let right = eval_to_int!(name, right);

    fn lcm(a: i64, b: i64) -> i64 {
        a * b / gcd_i64(a, b)
    }

    Ok(Value::I64(lcm(left, right)))
}

fn gcd_i64(a: i64, b: i64) -> i64 {
    if b == 0 {
        a
    } else {
        gcd_i64(b, a % b)
    }
}

// --- etc ---

pub fn unwrap(name: String, expr: Evaluated<'_>, selector: Evaluated<'_>) -> Result<Value> {
    if expr.is_null() {
        return Ok(Value::Null);
    }

    let value = match expr {
        Evaluated::Value(value) => value,
        _ => {
            return Err(EvaluateError::FunctionRequiresMapValue(name).into());
        }
    };

    let selector = eval_to_str!(name, selector);
    value.selector(&selector)
}

pub fn generate_uuid() -> Value {
    Value::Uuid(Uuid::new_v4().as_u128())
}
