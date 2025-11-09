use {
    super::{error::EvaluateError, function, literal::Literal},
    crate::{
        ast::{DataType, TrimWhereField},
        data::{Key, Value, ValueError, value::BTreeMapJsonExt},
        result::{Error, Result},
    },
    std::{borrow::Cow, collections::BTreeMap, convert::TryFrom, ops::Range},
    utils::Tribool,
    uuid::Uuid,
};

mod binary_op;
mod cmp;
mod eq;
mod like;
mod literal;
mod unary_op;

pub(crate) use literal::literal_to_value;

#[derive(Clone, Debug, PartialEq)]
pub enum Evaluated<'a> {
    Literal(Literal<'a>),
    StrSlice {
        source: Cow<'a, str>,
        range: Range<usize>,
    },
    Value(Value),
}

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<Value> {
        match e {
            Evaluated::Literal(v) => Value::try_from(v),
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Ok(Value::Str(s[r].to_owned())),
            Evaluated::Value(v) => Ok(v),
        }
    }
}

impl TryFrom<Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<Self> {
        Self::try_from(&evaluated)
    }
}

impl TryFrom<&Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::Literal(l) => Value::try_from(l)?.try_into(),
            Evaluated::StrSlice { source, range } => Ok(Key::Str(source[range.clone()].to_owned())),
            Evaluated::Value(v) => v.try_into(),
        }
    }
}

impl TryFrom<Evaluated<'_>> for bool {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<bool> {
        match e {
            Evaluated::Value(Value::Bool(v)) => Ok(v),
            Evaluated::Literal(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{v:?}")).into())
            }
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::BooleanTypeRequired(source[range].to_owned()).into())
            }
            Evaluated::Value(v) => Err(EvaluateError::BooleanTypeRequired(format!("{v:?}")).into()),
        }
    }
}

impl TryFrom<Evaluated<'_>> for BTreeMap<String, Value> {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<BTreeMap<String, Value>> {
        match evaluated {
            Evaluated::Literal(Literal::Text(v)) => BTreeMap::parse_json_object(v.as_ref()),
            Evaluated::Literal(v) => {
                Err(EvaluateError::TextLiteralRequired(format!("{v:?}")).into())
            }
            Evaluated::Value(Value::Str(v)) => BTreeMap::parse_json_object(v.as_str()),
            Evaluated::Value(Value::Map(v)) => Ok(v),
            Evaluated::Value(v) => Err(EvaluateError::MapOrStringValueRequired(v.into()).into()),
            Evaluated::StrSlice { source, range } => BTreeMap::parse_json_object(&source[range]),
        }
    }
}

pub fn exceptional_int_val_to_eval<'a>(name: String, v: &Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::Value(Value::Null)),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
}

impl From<Tribool> for Evaluated<'_> {
    fn from(x: Tribool) -> Self {
        Evaluated::Value(Value::from(x))
    }
}

impl<'a> Evaluated<'a> {
    pub fn arrow<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        let selector = Value::try_from(other.clone())?;

        if selector.is_null() {
            return Ok(Evaluated::Value(Value::Null));
        }

        let value_result = if let Evaluated::Value(base) = self {
            function::select_arrow_value(base, &selector)
        } else {
            let base = Value::try_from(self.clone())?;
            function::select_arrow_value(&base, &selector)
        };

        value_result.map(Evaluated::Value)
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(literal) => literal::try_cast_literal_to_value(data_type, &literal),
            Evaluated::Value(value) => value.cast(data_type),
            Evaluated::StrSlice { source, range } => {
                Value::Str(source[range].to_owned()).cast(data_type)
            }
        }
        .map(Evaluated::Value)
    }

    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => Evaluated::Literal(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::Value((Value::try_from(l)?).concat(r))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::Value(l.concat(Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::Value(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value((Value::try_from(l)?).concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(Value::try_from(r)?))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(r))
            }
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => {
                Evaluated::Value(Value::Str(a[ar].to_owned()).concat(Value::Str(b[br].to_owned())))
            }
        };

        Ok(evaluated)
    }

    pub fn ltrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_start_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_start_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_start_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        let pivot = matched_vec
            .iter()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let start = match pivot {
            Some(idx) => match idx {
                0 => 0,
                _ => matched_vec[idx - 1].0 + 1,
            },
            _ => matched_vec[matched_vec.len() - 1].0 + 1,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start + start..range.end,
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Evaluated::Value(v) if v.is_null())
    }

    pub fn rtrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_end_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_end_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_end_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }

        let pivot = matched_vec
            .iter()
            .rev()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| *vec_idx == sliced_expr.len() - slice_idx - 1)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let end = match pivot {
            Some(idx) => match idx {
                0 => range.end,
                _ => matched_vec[matched_vec.len() - idx].0,
            },
            _ => matched_vec[0].0,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start..end,
        })
    }

    pub fn substr(
        self,
        name: String,
        start: Evaluated<'a>,
        count: Option<Evaluated<'a>>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let start = {
            let value = start.try_into()?;
            match value {
                Value::I64(num) => num,
                _ => return exceptional_int_val_to_eval(name, &value),
            }
        } - 1;

        let count = match count {
            Some(eval) => {
                let value = eval.try_into()?;
                match value {
                    Value::I64(num) => num,
                    _ => return exceptional_int_val_to_eval(name, &value),
                }
            }
            None => source.len() as i64,
        };

        let end = if count < 0 {
            return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
        } else {
            (range.start as i64 + start + count).clamp(0, source.len() as i64) as usize
        };

        let start = (start + range.start as i64).clamp(0, source.len() as i64) as usize;

        Ok(Evaluated::StrSlice {
            source,
            range: start..end,
        })
    }

    pub fn trim(
        self,
        name: String,
        filter_chars: Option<Evaluated<'_>>,
        trim_where_field: Option<&TrimWhereField>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match filter_chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();
        //filter_chars => ['x','y','z']
        //"x".trim_matches(filter_chars[..]) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //filter_chars => ['x','y','z']
        //"tuv".trim_matches(filter_chars[..]) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //filter_chars => ['x','y','z']
        //"txu".trim_matches(filter_chars[..]) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        match trim_where_field {
            Some(TrimWhereField::Both) => {
                //filter_chars => ['x','y','z']
                //"xyzbyxlxyz  ".trim_matches(filter_chars[..]) => "byxlxyz  "
                if matched_vec[0].0 == 0
                    && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let start = match pivot {
                        Some(idx) => matched_vec[idx - 1].0 + 1,
                        _ => matched_vec[matched_vec.len() - 1].0 + 1,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start + start..range.end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"  xyzblankxyzxx".trim_matches(filter_chars[..]) => "  xyzblank"
                if matched_vec[0].0 != 0
                    && matched_vec[matched_vec.len() - 1].0 == sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .rev()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| {
                            *vec_idx == sliced_expr.len() - slice_idx - 1
                        })
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let end = match pivot {
                        Some(idx) => matched_vec[matched_vec.len() - idx].0,
                        _ => matched_vec[0].0,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start..end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"xxbyz".trim_matches(filter_chars[..]) => "b"
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next()
                    .unwrap_or(0);

                let trim_range = matched_vec[pivot - 1].0..(matched_vec[pivot].0 + range.start);

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + trim_range.start + 1..trim_range.end,
                })
            }
            Some(TrimWhereField::Leading) => {
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let start = match pivot {
                    Some(idx) => match idx {
                        0 => 0,
                        _ => matched_vec[idx - 1].0 + 1,
                    },
                    _ => matched_vec[matched_vec.len() - 1].0 + 1,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + start..range.end,
                })
            }
            Some(TrimWhereField::Trailing) => {
                let pivot = matched_vec
                    .iter()
                    .rev()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| {
                        *vec_idx == sliced_expr.len() - slice_idx - 1
                    })
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let end = match pivot {
                    Some(idx) => match idx {
                        0 => range.end,
                        _ => matched_vec[matched_vec.len() - idx].0,
                    },
                    _ => matched_vec[0].0,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start..end,
                })
            }
            None => {
                let start = source
                    .chars()
                    .skip(range.start)
                    .enumerate()
                    .find(|(_, c)| !c.is_whitespace())
                    .map_or(0, |(idx, _)| idx + range.start);

                let end = source.len()
                    - source
                        .chars()
                        .rev()
                        .skip(source.len() - range.end)
                        .enumerate()
                        .find(|(_, c)| !c.is_whitespace())
                        .map_or(0, |(idx, _)| source.len() - (range.end - idx));

                Ok(Evaluated::StrSlice {
                    source,
                    range: start..end,
                })
            }
        }
    }

    pub fn try_into_value(self, data_type: &DataType, nullable: bool) -> Result<Value> {
        let value = match self {
            Evaluated::Literal(v) => literal_to_value(data_type, &v)?,
            Evaluated::Value(Value::Bytea(bytes)) if data_type == &DataType::Uuid => {
                Uuid::from_slice(&bytes)
                    .map_err(|_| ValueError::FailedToParseUUID(hex::encode(bytes)))
                    .map(|uuid| Value::Uuid(uuid.as_u128()))?
            }
            Evaluated::Value(value) => value,
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Value::Str(s[r].to_owned()),
        };

        value.validate_null(nullable)?;

        Ok(value)
    }
}
