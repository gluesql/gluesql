use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::DateTimeField,
        data::{Key, Point, Value, ValueError},
        result::{Error, Result},
    },
    chrono::{Datelike, Duration, Months},
    md5::{Digest, Md5},
    rand::{rngs::StdRng, Rng, SeedableRng},
    std::ops::ControlFlow::{self as StdControlFlow, Break, Continue},
    uuid::Uuid,
};

type ControlFlow<T> = std::ops::ControlFlow<BreakCase, T>;

pub enum BreakCase {
    Null,
    Err(Error),
}

trait ContinueOrBreak<T> {
    fn continue_or_break(self, err: Error) -> ControlFlow<T>;
}

impl<T> ContinueOrBreak<T> for Option<T> {
    fn continue_or_break(self, err: Error) -> ControlFlow<T> {
        match self {
            Some(v) => Continue(v),
            None => Break(BreakCase::Err(err)),
        }
    }
}

trait BreakIfNull<T> {
    fn break_if_null(self) -> ControlFlow<T>;
}

impl<'a> BreakIfNull<Evaluated<'a>> for Result<Evaluated<'a>> {
    fn break_if_null(self) -> ControlFlow<Evaluated<'a>> {
        match self {
            Err(err) => Break(BreakCase::Err(err)),
            Ok(value) if value.is_null() => Break(BreakCase::Null),
            Ok(value) => Continue(value),
        }
    }
}

impl BreakIfNull<Value> for Result<Value> {
    fn break_if_null(self) -> ControlFlow<Value> {
        match self {
            Err(err) => Break(BreakCase::Err(err)),
            Ok(value) if value.is_null() => Break(BreakCase::Null),
            Ok(value) => Continue(value),
        }
    }
}

trait ControlFlowMap<T, U, F> {
    fn map(self, f: F) -> ControlFlow<U>
    where
        F: FnOnce(T) -> U;
}

impl<T, U, F> ControlFlowMap<T, U, F> for ControlFlow<T> {
    fn map(self, f: F) -> ControlFlow<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Continue(v) => Continue(f(v)),
            Break(v) => Break(v),
        }
    }
}

trait ControlFlowMapErr<T, F> {
    fn map_err(self, f: F) -> ControlFlow<T>
    where
        F: FnOnce(Error) -> Error;
}

impl<T, F> ControlFlowMapErr<T, F> for ControlFlow<T> {
    fn map_err(self, f: F) -> ControlFlow<T>
    where
        F: FnOnce(Error) -> Error,
    {
        match self {
            Continue(v) => Continue(v),
            Break(BreakCase::Null) => Break(BreakCase::Null),
            Break(BreakCase::Err(err)) => Break(BreakCase::Err(f(err))),
        }
    }
}

pub trait IntoControlFlow<T> {
    fn into_control_flow(self) -> ControlFlow<T>;
}

impl<T> IntoControlFlow<T> for Result<T> {
    fn into_control_flow(self) -> ControlFlow<T> {
        match self {
            Err(err) => Break(BreakCase::Err(err)),
            Ok(value) => Continue(value),
        }
    }
}

fn eval_to_str(name: &str, evaluated: Evaluated<'_>) -> ControlFlow<String> {
    match evaluated.try_into().break_if_null()? {
        Value::Str(value) => Continue(value),
        _ => Break(BreakCase::Err(
            EvaluateError::FunctionRequiresStringValue(name.to_owned()).into(),
        )),
    }
}

fn eval_to_int(name: &str, evaluated: Evaluated<'_>) -> ControlFlow<i64> {
    match evaluated.try_into().break_if_null()? {
        Value::I64(num) => Continue(num),
        _ => Break(BreakCase::Err(
            EvaluateError::FunctionRequiresIntegerValue(name.to_owned()).into(),
        )),
    }
}

fn eval_to_float(name: &str, evaluated: Evaluated<'_>) -> ControlFlow<f64> {
    match evaluated.try_into().break_if_null()? {
        Value::I64(v) => Continue(v as f64),
        Value::F32(v) => Continue(v.into()),
        Value::F64(v) => Continue(v),
        _ => Break(BreakCase::Err(
            EvaluateError::FunctionRequiresFloatValue(name.to_owned()).into(),
        )),
    }
}

fn eval_to_point(name: &str, evaluated: Evaluated<'_>) -> ControlFlow<Point> {
    match evaluated.try_into().break_if_null()? {
        Value::Point(v) => Continue(v),
        _ => Break(BreakCase::Err(
            EvaluateError::FunctionRequiresPointValue(name.to_owned()).into(),
        )),
    }
}

// --- text ---
pub fn concat(exprs: Vec<Evaluated<'_>>) -> ControlFlow<Evaluated> {
    let value = exprs
        .into_iter()
        .try_fold(None, |left: Option<Evaluated>, right| match left {
            None => Continue(Some(right)),
            Some(left) => left.concat(right).break_if_null().map(Some),
        })?;

    value.continue_or_break(ValueError::EmptyArgNotAllowedInConcat.into())
}

pub fn concat_ws<'a>(
    name: String,
    separator: Evaluated<'a>,
    exprs: Vec<Evaluated<'a>>,
) -> ControlFlow<Evaluated<'a>> {
    let separator = eval_to_str(&name, separator)?;

    let result = exprs
        .into_iter()
        .map(Value::try_from)
        .filter(|value| !matches!(value, Ok(Value::Null)))
        .map(|value| Ok(String::from(value?)))
        .collect::<Result<Vec<_>>>()
        .into_control_flow()?
        .join(&separator);

    Continue(Evaluated::Value(Value::Str(result)))
}

pub fn lower(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'_>> {
    eval_to_str(&name, expr)
        .map(|value| value.to_lowercase())
        .map(Value::Str)
        .map(Evaluated::Value)
}

pub fn initcap(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'_>> {
    let string = eval_to_str(&name, expr)?
        .chars()
        .scan(true, |state, c| {
            let c = if *state {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            };
            *state = !c.is_alphanumeric();
            Some(c)
        })
        .collect();

    Continue(Evaluated::Value(Value::Str(string)))
}

pub fn upper(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'_>> {
    eval_to_str(&name, expr)
        .map(|value| value.to_uppercase())
        .map(Value::Str)
        .map(Evaluated::Value)
}

pub fn left_or_right<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let string = eval_to_str(&name, expr)?;
    let size = eval_to_int(&name, size)
        .map(usize::try_from)?
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;

    let converted = if name == "LEFT" {
        string.get(..size).map(|v| v.to_owned()).unwrap_or(string)
    } else {
        let start_pos = if size > string.len() {
            0
        } else {
            string.len() - size
        };

        string
            .get(start_pos..)
            .map(|value| value.to_owned())
            .unwrap_or(string)
    };

    Continue(Evaluated::Value(Value::Str(converted)))
}

pub fn lpad_or_rpad<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
    fill: Option<Evaluated<'_>>,
) -> ControlFlow<Evaluated<'a>> {
    let string = eval_to_str(&name, expr)?;
    let size = eval_to_int(&name, size)
        .map(usize::try_from)?
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;

    let fill = match fill {
        Some(expr) => eval_to_str(&name, expr)?,
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
        string[0..size].to_owned()
    };

    Continue(Evaluated::Value(Value::Str(result)))
}

pub fn reverse(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'_>> {
    let value = eval_to_str(&name, expr)?.chars().rev().collect::<String>();

    Continue(Evaluated::Value(Value::Str(value)))
}

pub fn repeat<'a>(
    name: String,
    expr: Evaluated<'_>,
    num: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = eval_to_str(&name, expr)?;
    let num = eval_to_int(&name, num)? as usize;
    let value = expr.repeat(num);

    Continue(Evaluated::Value(Value::Str(value)))
}

pub fn replace<'a>(
    name: String,
    expr: Evaluated<'_>,
    old: Evaluated<'_>,
    new: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = eval_to_str(&name, expr)?;
    let old = eval_to_str(&name, old)?;
    let new = eval_to_str(&name, new)?;
    let value = expr.replace(&old, &new);

    Continue(Evaluated::Value(Value::Str(value)))
}

pub fn ascii<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let string = eval_to_str(&name, expr)?;
    let mut iter = string.chars();

    match (iter.next(), iter.next()) {
        (Some(c), None) => {
            if c.is_ascii() {
                Continue(Evaluated::Value(Value::U8(c as u8)))
            } else {
                Err(EvaluateError::NonAsciiCharacterNotAllowed.into()).into_control_flow()
            }
        }
        _ => {
            Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()).into_control_flow()
        }
    }
}

pub fn chr<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let expr = eval_to_int(&name, expr)?;

    match expr {
        0..=255 => {
            let expr = expr as u8;

            Continue(Evaluated::Value(Value::Str((expr as char).to_string())))
        }
        _ => Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())
            .into_control_flow(),
    }
}

pub fn md5<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let string = eval_to_str(&name, expr)?;
    let mut hasher = Md5::new();
    hasher.update(string.as_bytes());
    let result = hasher.finalize();
    let result = format!("{:x}", result);

    Continue(Evaluated::Value(Value::Str(result)))
}

// --- float ---

pub fn abs<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let value = match n.try_into().break_if_null()? {
        Value::I8(v) => Value::I8(v.abs()),
        Value::I32(v) => Value::I32(v.abs()),
        Value::I64(v) => Value::I64(v.abs()),
        Value::I128(v) => Value::I128(v.abs()),
        Value::Decimal(v) => Value::Decimal(v.abs()),
        Value::F32(v) => Value::F32(v.abs()),
        Value::F64(v) => Value::F64(v.abs()),
        _ => {
            return Err(EvaluateError::FunctionRequiresFloatValue(name).into()).into_control_flow()
        }
    };

    Continue(Evaluated::Value(value))
}

pub fn ifnull<'a>(expr: Evaluated<'a>, then: Evaluated<'a>) -> ControlFlow<Evaluated<'a>> {
    Continue(match expr.is_null() {
        true => then,
        false => expr,
    })
}

pub fn sign(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'_>> {
    let x = eval_to_float(&name, n)?;
    if x == 0.0 {
        return Continue(Evaluated::Value(Value::I8(0)));
    }

    Continue(Evaluated::Value(Value::I8(x.signum() as i8)))
}

pub fn sqrt<'a>(n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    Value::try_from(n)
        .and_then(|v| v.sqrt())
        .into_control_flow()
        .map(Evaluated::Value)
}

pub fn power<'a>(
    name: String,
    expr: Evaluated<'_>,
    power: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = eval_to_float(&name, expr)?;
    let power = eval_to_float(&name, power)?;

    Continue(Evaluated::Value(Value::F64(expr.powf(power))))
}

pub fn ceil<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.ceil())))
}

pub fn rand<'a>(name: String, seed: Option<Evaluated<'_>>) -> ControlFlow<Evaluated<'a>> {
    let seed = if let Some(v) = seed {
        StdRng::seed_from_u64(eval_to_float(&name, v)? as u64).gen()
    } else {
        rand::random()
    };
    Continue(Evaluated::Value(Value::F64(seed)))
}

pub fn round<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.round())))
}

pub fn floor<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.floor())))
}

pub fn radians<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.to_radians())))
}

pub fn degrees<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.to_degrees())))
}

pub fn exp<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.exp())))
}

pub fn log<'a>(
    name: String,
    antilog: Evaluated<'_>,
    base: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let antilog = eval_to_float(&name, antilog)?;
    let base = eval_to_float(&name, base)?;

    Continue(Evaluated::Value(Value::F64(antilog.log(base))))
}

pub fn ln<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.ln())))
}

pub fn log2<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.log2())))
}

pub fn log10<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.log10())))
}

pub fn sin<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.sin())))
}

pub fn cos<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.cos())))
}

pub fn tan<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.tan())))
}

pub fn asin<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.asin())))
}

pub fn acos<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.acos())))
}

pub fn atan<'a>(name: String, n: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    eval_to_float(&name, n).map(|n| Evaluated::Value(Value::F64(n.atan())))
}

// --- integer ---

pub fn div<'a>(
    name: String,
    dividend: Evaluated<'_>,
    divisor: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let dividend = eval_to_float(&name, dividend)
        .map_err(|_| EvaluateError::FunctionRequiresFloatOrIntegerValue(name.clone()).into())?;
    let divisor = eval_to_float(&name, divisor)
        .map_err(|_| EvaluateError::FunctionRequiresFloatOrIntegerValue(name.clone()).into())?;

    if divisor == 0.0 {
        return Err(EvaluateError::DivisorShouldNotBeZero.into()).into_control_flow();
    }

    Continue(Evaluated::Value(Value::I64((dividend / divisor) as i64)))
}

pub fn gcd<'a>(
    name: String,
    left: Evaluated<'_>,
    right: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let left = eval_to_int(&name, left)?;
    let right = eval_to_int(&name, right)?;

    gcd_i64(left, right).map(|gcd| Evaluated::Value(Value::I64(gcd)))
}

pub fn lcm<'a>(
    name: String,
    left: Evaluated<'_>,
    right: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let left = eval_to_int(&name, left)?;
    let right = eval_to_int(&name, right)?;

    fn lcm(a: i64, b: i64) -> ControlFlow<i64> {
        let gcd_val: i128 = gcd_i64(a, b)?.into();

        let a: i128 = a.into();
        let b: i128 = b.into();

        // lcm(a, b) = abs(a * b) / gcd(a, b)   if gcd(a, b) != 0
        // lcm(a, b) = 0                        if gcd(a, b) == 0
        let result = (a * b).abs().checked_div(gcd_val).unwrap_or(0);

        i64::try_from(result)
            .map_err(|_| Error::Value(ValueError::LcmResultOutOfRange))
            .into_control_flow()
    }

    lcm(left, right).map(|lcm| Evaluated::Value(Value::I64(lcm)))
}

fn gcd_i64(a: i64, b: i64) -> ControlFlow<i64> {
    let mut a = a
        .checked_abs()
        .continue_or_break(ValueError::GcdLcmOverflow(a).into())?;
    let mut b = b
        .checked_abs()
        .continue_or_break(ValueError::GcdLcmOverflow(b).into())?;

    while b > 0 {
        (a, b) = (b, a % b);
    }

    Continue(a)
}

// --- list ---
pub fn append<'a>(expr: Evaluated<'_>, value: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let value = value.try_into().break_if_null()?;

    match (expr, value) {
        (Value::List(mut l), v) => {
            l.push(v);
            Continue(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn prepend<'a>(expr: Evaluated<'_>, value: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let value = value.try_into().break_if_null()?;

    match (expr, value) {
        (Value::List(mut l), v) => {
            l.insert(0, v);
            Continue(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn skip<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let size: usize = match size.try_into().break_if_null()? {
        Value::I64(number) => usize::try_from(number)
            .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name).into()),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
    .into_control_flow()?;

    match expr {
        Value::List(l) => {
            let l = l.into_iter().skip(size).collect();
            Continue(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn sort<'a>(expr: Evaluated<'_>, order: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let order = order.try_into().break_if_null()?;

    match expr {
        Value::List(l) => {
            let mut l: Vec<(Key, Value)> = l
                .into_iter()
                .map(|v| match Key::try_from(&v) {
                    Ok(key) => Ok((key, v)),
                    Err(_) => Err(EvaluateError::InvalidSortType.into()),
                })
                .collect::<Result<Vec<(Key, Value)>>>()
                .into_control_flow()?;

            let asc = match order {
                Value::Str(s) => match s.to_uppercase().as_str() {
                    "ASC" => true,
                    "DESC" => false,
                    _ => return Err(EvaluateError::InvalidSortOrder.into()).into_control_flow(),
                },
                _ => return Err(EvaluateError::InvalidSortOrder.into()).into_control_flow(),
            };

            l.sort_by(|a, b| if asc { a.0.cmp(&b.0) } else { b.0.cmp(&a.0) });

            Continue(Evaluated::Value(Value::List(
                l.into_iter().map(|(_, v)| v).collect(),
            )))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn slice<'a>(
    name: String,
    expr: Evaluated<'_>,
    start: Evaluated<'_>,
    length: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let mut start = eval_to_int(&name, start)?;
    let length = eval_to_int(&name, length)
        .map(usize::try_from)?
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;

    match expr {
        Value::List(l) => {
            if start < 0 {
                start += l.len() as i64;
            }
            if start < 0 {
                start = 0;
            }

            let start_usize = start as usize;

            let l = l.into_iter().skip(start_usize).take(length).collect();
            Continue(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn take<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let size = eval_to_int(&name, size)
        .map(usize::try_from)?
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;

    match expr {
        Value::List(l) => {
            let l = l.into_iter().take(size).collect();
            Continue(Evaluated::Value(Value::List(l)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}

pub fn is_empty<'a>(expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let expr = expr.try_into().break_if_null()?;
    let length = match expr {
        Value::List(l) => l.len(),
        Value::Map(m) => m.len(),
        _ => {
            return Err(EvaluateError::MapOrListTypeRequired.into()).into_control_flow();
        }
    };

    Continue(Evaluated::Value(Value::Bool(length == 0)))
}

pub fn values<'a>(expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Map(m) => Ok(Evaluated::Value(Value::List(m.into_values().collect()))),
        _ => Err(EvaluateError::MapTypeRequired.into()),
    }
    .into_control_flow()
}

// --- etc ---

pub fn unwrap<'a>(
    name: String,
    expr: Evaluated<'a>,
    selector: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let value = match expr {
        _ if expr.is_null() => return Continue(expr),
        Evaluated::Value(value) => value,
        _ => {
            return Err(EvaluateError::FunctionRequiresMapValue(name).into()).into_control_flow();
        }
    };
    let selector = eval_to_str(&name, selector)?;

    value
        .selector(&selector)
        .into_control_flow()
        .map(Evaluated::Value)
}

pub fn generate_uuid<'a>() -> Evaluated<'a> {
    Evaluated::Value(Value::Uuid(Uuid::new_v4().as_u128()))
}

pub fn greatest(name: String, exprs: Vec<Evaluated<'_>>) -> Result<Evaluated<'_>> {
    exprs
        .into_iter()
        .try_fold(None, |greatest, expr| -> Result<_> {
            let greatest = match greatest {
                Some(greatest) => greatest,
                None => return Ok(Some(expr)),
            };

            match greatest.evaluate_cmp(&expr) {
                Some(std::cmp::Ordering::Less) => Ok(Some(expr)),
                Some(_) => Ok(Some(greatest)),
                None => Err(EvaluateError::NonComparableArgumentError(name.to_owned()).into()),
            }
        })?
        .ok_or(EvaluateError::FunctionRequiresAtLeastOneArgument(name.to_owned()).into())
}

pub fn format<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Date(expr) => eval_to_str(&name, format)
            .map(|format| chrono::NaiveDate::format(&expr, &format).to_string()),
        Value::Timestamp(expr) => eval_to_str(&name, format)
            .map(|format| chrono::NaiveDateTime::format(&expr, &format).to_string()),
        Value::Time(expr) => eval_to_str(&name, format)
            .map(|format| chrono::NaiveTime::format(&expr, &format).to_string()),
        value => Err(EvaluateError::UnsupportedExprForFormatFunction(value.into()).into())
            .into_control_flow(),
    }
    .map(Value::Str)
    .map(Evaluated::Value)
}

pub fn last_day<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let date = match expr.try_into().break_if_null()? {
        Value::Date(date) => date,
        Value::Timestamp(timestamp) => timestamp.date(),
        _ => {
            return Err(EvaluateError::FunctionRequiresDateOrDateTimeValue(name).into())
                .into_control_flow()
        }
    };

    Continue(Evaluated::Value(Value::Date(
        date + Months::new(1) - Duration::days(date.day() as i64),
    )))
}

pub fn to_date<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Str(expr) => {
            let format = eval_to_str(&name, format)?;

            chrono::NaiveDate::parse_from_str(&expr, &format)
                .map(Value::Date)
                .map(Evaluated::Value)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
    .into_control_flow()
}

pub fn to_timestamp<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Str(expr) => {
            let format = eval_to_str(&name, format)?;

            chrono::NaiveDateTime::parse_from_str(&expr, &format)
                .map(Value::Timestamp)
                .map(Evaluated::Value)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
    .into_control_flow()
}

pub fn add_month<'a>(
    name: String,
    expr: Evaluated<'_>,
    size: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let size = eval_to_int(&name, size)?;
    let expr = eval_to_str(&name, expr)?;
    let expr = chrono::NaiveDate::parse_from_str(&expr, "%Y-%m-%d")
        .map_err(EvaluateError::from)
        .map_err(Error::from)
        .into_control_flow()?;
    let date = {
        let size_as_u32 = size
            .abs()
            .try_into()
            .map_err(|_| ValueError::I64ToU32ConversionFailure(name).into())
            .into_control_flow()?;
        let new_months = chrono::Months::new(size_as_u32);

        if size <= 0 {
            expr.checked_sub_months(new_months)
        } else {
            expr.checked_add_months(new_months)
        }
        .continue_or_break(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into())?
    };
    Continue(Evaluated::Value(Value::Date(date)))
}

pub fn to_time<'a>(
    name: String,
    expr: Evaluated<'_>,
    format: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Str(expr) => {
            let format = eval_to_str(&name, format)?;

            chrono::NaiveTime::parse_from_str(&expr, &format)
                .map(Value::Time)
                .map(Evaluated::Value)
                .map_err(|err| {
                    let err: EvaluateError = err.into();
                    err.into()
                })
        }
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
    .into_control_flow()
}

pub fn position<'a>(
    from_expr: Evaluated<'_>,
    sub_expr: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let from: Value = from_expr.try_into().break_if_null()?;
    let sub = sub_expr.try_into().break_if_null()?;

    from.position(&sub)
        .map(Evaluated::Value)
        .into_control_flow()
}

pub fn find_idx<'a>(
    name: String,
    from: Evaluated<'a>,
    sub: Evaluated<'a>,
    start: Option<Evaluated<'a>>,
) -> ControlFlow<Evaluated<'a>> {
    let from_expr = eval_to_str(&name, from)?;
    let sub_expr = eval_to_str(&name, sub)?;

    match start {
        Some(start) => {
            let start = eval_to_int(&name, start)?;
            Value::find_idx(
                &Value::Str(from_expr),
                &Value::Str(sub_expr),
                &Value::I64(start),
            )
        }
        None => Value::position(&Value::Str(from_expr), &Value::Str(sub_expr)),
    }
    .map(Evaluated::Value)
    .into_control_flow()
}

pub fn extract<'a>(field: &DateTimeField, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    Value::try_from(expr)
        .and_then(|v| v.extract(field))
        .map(Evaluated::Value)
        .into_control_flow()
}

pub fn point<'a>(name: String, x: Evaluated<'_>, y: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    let x = eval_to_float(&name, x)?;
    let y = eval_to_float(&name, y)?;

    Continue(Evaluated::Value(Value::Point(Point::new(x, y))))
}

pub fn get_x<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Point(v) => Ok(Evaluated::Value(Value::F64(v.x))),
        _ => Err(EvaluateError::FunctionRequiresPointValue(name).into()),
    }
    .into_control_flow()
}

pub fn get_y<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Point(v) => Ok(Evaluated::Value(Value::F64(v.y))),
        _ => Err(EvaluateError::FunctionRequiresPointValue(name).into()),
    }
    .into_control_flow()
}

pub fn calc_distance<'a>(
    name: String,
    x: Evaluated<'_>,
    y: Evaluated<'_>,
) -> ControlFlow<Evaluated<'a>> {
    let x = eval_to_point(&name, x)?;
    let y = eval_to_point(&name, y)?;

    Continue(Evaluated::Value(Value::F64(Point::calc_distance(&x, &y))))
}

pub fn length<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Str(expr) => Ok(Evaluated::Value(Value::U64(expr.chars().count() as u64))),
        Value::List(expr) => Ok(Evaluated::Value(Value::U64(expr.len() as u64))),
        Value::Map(expr) => Ok(Evaluated::Value(Value::U64(expr.len() as u64))),
        _ => Err(EvaluateError::FunctionRequiresStrOrListOrMapValue(name).into()),
    }
    .into_control_flow()
}

pub fn coalesce<'a>(exprs: Vec<Evaluated<'_>>) -> Result<Evaluated<'a>> {
    if exprs.is_empty() {
        return Err((EvaluateError::FunctionRequiresMoreArguments {
            function_name: "COALESCE".to_owned(),
            required_minimum: 1,
            found: exprs.len(),
        })
        .into());
    }

    let control_flow = exprs.into_iter().map(|expr| expr.try_into()).try_for_each(
        |item: Result<Value>| match item {
            Ok(value) if value.is_null() => StdControlFlow::Continue(()),
            Ok(value) => StdControlFlow::Break(Ok(value)),
            Err(err) => StdControlFlow::Break(Err(err)),
        },
    );

    match control_flow {
        StdControlFlow::Break(Ok(value)) => Ok(Evaluated::Value(value)),
        StdControlFlow::Break(Err(err)) => Err(err),
        StdControlFlow::Continue(()) => Ok(Evaluated::Value(Value::Null)),
    }
}

pub fn entries<'a>(name: String, expr: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match expr.try_into().break_if_null()? {
        Value::Map(expr) => {
            let entries = expr
                .into_iter()
                .map(|(k, v)| Value::List(vec![Value::Str(k), v]))
                .collect::<Vec<_>>();
            Ok(Evaluated::Value(Value::List(entries)))
        }
        _ => Err(EvaluateError::FunctionRequiresMapValue(name).into()),
    }
    .into_control_flow()
}

pub fn splice<'a>(
    name: String,
    list_data: Evaluated<'_>,
    begin_index: Evaluated<'_>,
    end_index: Evaluated<'_>,
    values: Option<Evaluated<'_>>,
) -> ControlFlow<Evaluated<'a>> {
    let list_data = match list_data.try_into().break_if_null()? {
        Value::List(list) => list,
        _ => {
            return Err(EvaluateError::ListTypeRequired.into()).into_control_flow();
        }
    };

    let begin_index = eval_to_int(&name, begin_index)?.max(0);
    let begin_index = usize::try_from(begin_index)
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;
    let end_index = eval_to_int(&name, end_index)?.max(0);
    let end_index = usize::try_from(end_index)
        .map_err(|_| EvaluateError::FunctionRequiresUSizeValue(name.clone()).into())
        .into_control_flow()?;

    let (left, right) = {
        let mut list_iter = list_data.into_iter();
        let left: Vec<_> = list_iter.by_ref().take(begin_index).collect();
        let right: Vec<_> = list_iter.skip(end_index - begin_index).collect();
        (left, right)
    };

    let center = match values {
        Some(values) => match values.try_into().break_if_null()? {
            Value::List(list) => list,
            _ => return Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
        },
        None => vec![],
    };

    let result = {
        let mut result = vec![];
        result.extend(left);
        result.extend(center);
        result.extend(right);
        result
    };

    Continue(Evaluated::Value(Value::List(result)))
}

pub fn dedup<'a>(list: Evaluated<'_>) -> ControlFlow<Evaluated<'a>> {
    match list.try_into().break_if_null()? {
        Value::List(mut list) => {
            list.dedup();
            Continue(Evaluated::Value(Value::List(list)))
        }
        _ => Err(EvaluateError::ListTypeRequired.into()).into_control_flow(),
    }
}
