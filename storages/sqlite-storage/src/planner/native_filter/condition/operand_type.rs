//! Resolves literals and checks logical type compatibility for native comparisons.

use gluesql_core::{
    ast::{BinaryOperator, DataType, Literal},
    data::{BigDecimalExt, Value},
    plan::ColumnDefPlan,
};

#[derive(Clone)]
pub(super) enum Operand<'a> {
    Column(&'a ColumnDefPlan),
    // Literals can adopt a peer type; typed Values retain theirs.
    Literal(Literal),
    Value(Value),
}

#[derive(Debug, PartialEq)]
pub(super) enum ResolvedOperand<'a> {
    Column(&'a ColumnDefPlan),
    Value(Value),
}

impl<'a> Operand<'a> {
    pub(super) fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Column(column) => Some(column.data_type.clone()),
            // NULL has no logical type.
            Self::Value(value) => value.get_type(),
            Self::Literal(_) => None,
        }
    }

    pub(super) fn resolve(self, peer_type: Option<&DataType>) -> Option<ResolvedOperand<'a>> {
        Some(match self {
            Self::Column(column) => ResolvedOperand::Column(column),
            Self::Literal(literal) => ResolvedOperand::Value(literal_to_value(literal, peer_type)?),
            Self::Value(value) => ResolvedOperand::Value(value),
        })
    }
}

impl ResolvedOperand<'_> {
    pub(super) fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Column(column) => Some(column.data_type.clone()),
            Self::Value(value) => value.get_type(),
        }
    }

    pub(super) fn supports_comparison(&self, op: &BinaryOperator) -> bool {
        match self.data_type() {
            None | Some(DataType::Text | DataType::Boolean | DataType::Bytea) => true,
            Some(DataType::Float32 | DataType::Float) => {
                // Value float equality uses an epsilon; SQLite equality is exact.
                !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq)
            }
            Some(data_type) => integer_bounds(&data_type).is_some(),
        }
    }

    pub(super) fn coerce_rhs(&self, right: Self) -> Option<Self> {
        // Match GlueSQL's lhs-based comparison: a rhs column's whole range must fit
        // the lhs type, while a bound value only needs its own value to fit.
        let (Some(left_type), Some(right_type)) = (self.data_type(), right.data_type()) else {
            return Some(right);
        };

        if left_type == right_type {
            return Some(right);
        }

        let (left_min, left_max) = integer_bounds(&left_type)?;
        let (right_min, right_max) = integer_bounds(&right_type)?;

        match right {
            Self::Column(column) => {
                if left_min > right_min || right_max > left_max {
                    return None;
                }

                Some(Self::Column(column))
            }
            Self::Value(value) => convert_integer_value(&value, &left_type).map(Self::Value),
        }
    }
}

fn literal_to_value(literal: Literal, peer_type: Option<&DataType>) -> Option<Value> {
    Some(match literal {
        Literal::QuotedString(text)
            if peer_type.is_none_or(|data_type| *data_type == DataType::Text) =>
        {
            Value::Str(text)
        }
        // TEXT affinity alone does not provide GlueSQL's date/UUID conversions.
        Literal::QuotedString(_) => return None,
        Literal::Number(number) => match peer_type.unwrap_or(&DataType::Int) {
            DataType::Int8 => Value::I8(number.to_i8()?),
            DataType::Int16 => Value::I16(number.to_i16()?),
            DataType::Int32 => Value::I32(number.to_i32()?),
            DataType::Int => Value::I64(number.to_i64()?),
            DataType::Uint8 => Value::U8(number.to_u8()?),
            DataType::Uint16 => Value::U16(number.to_u16()?),
            DataType::Uint32 => Value::U32(number.to_u32()?),
            // Round FLOAT32 literals to f32 first. The codec then widens that
            // value to f64, matching the representation of stored FLOAT32 rows.
            DataType::Float32 => Value::F32(number.to_f32()?),
            DataType::Float => Value::F64(number.to_f64()?),
            _ => return None,
        },
    })
}

fn integer_bounds(data_type: &DataType) -> Option<(i128, i128)> {
    // i128 is only the calculation domain for bounds. Accepted columns are stored as
    // SQLite INTEGER; INT128/UINT64/UINT128 use TEXT and are intentionally absent.
    Some(match data_type {
        DataType::Int8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        DataType::Int16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        DataType::Int32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        DataType::Int => (i128::from(i64::MIN), i128::from(i64::MAX)),
        DataType::Uint8 => (0, i128::from(u8::MAX)),
        DataType::Uint16 => (0, i128::from(u16::MAX)),
        DataType::Uint32 => (0, i128::from(u32::MAX)),
        _ => return None,
    })
}

fn convert_integer_value(value: &Value, data_type: &DataType) -> Option<Value> {
    let value = match value {
        Value::I8(value) => i128::from(*value),
        Value::I16(value) => i128::from(*value),
        Value::I32(value) => i128::from(*value),
        Value::I64(value) => i128::from(*value),
        Value::U8(value) => i128::from(*value),
        Value::U16(value) => i128::from(*value),
        Value::U32(value) => i128::from(*value),
        _ => return None,
    };
    Some(match data_type {
        DataType::Int8 => Value::I8(value.try_into().ok()?),
        DataType::Int16 => Value::I16(value.try_into().ok()?),
        DataType::Int32 => Value::I32(value.try_into().ok()?),
        DataType::Int => Value::I64(value.try_into().ok()?),
        DataType::Uint8 => Value::U8(value.try_into().ok()?),
        DataType::Uint16 => Value::U16(value.try_into().ok()?),
        DataType::Uint32 => Value::U32(value.try_into().ok()?),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{Operand, ResolvedOperand},
        gluesql_core::{
            ast::{BinaryOperator, DataType, Literal},
            data::{Schema, Value},
            plan::ColumnDefPlan,
        },
    };

    #[test]
    fn resolve() {
        let schema = Schema::from_ddl("CREATE TABLE q (n INT8)").unwrap();
        let columns = schema.column_defs.unwrap();
        let column = ColumnDefPlan::from(columns[0].clone());

        let actual = Operand::Column(&column).resolve(Some(&DataType::Int));
        let expected = Some(ResolvedOperand::Column(&column));
        assert_eq!(actual, expected);

        // Only a literal acquires the peer type during resolution.
        let literal = Operand::Literal(Literal::Number(12.into()));
        let actual = literal.resolve(Some(&DataType::Int8));
        let expected = Some(ResolvedOperand::Value(Value::I8(12)));
        assert_eq!(actual, expected);

        let literal = Operand::Literal(Literal::Number(128.into()));
        let actual = literal.resolve(Some(&DataType::Int8));
        let expected = None;
        assert_eq!(actual, expected);

        // Typed values, including NULL, pass through without conversion.
        for value in [Value::I64(12), Value::Null] {
            let actual = Operand::Value(value.clone()).resolve(Some(&DataType::Int8));
            let expected = Some(ResolvedOperand::Value(value));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn supports_comparison() {
        for value in [
            Value::Null,
            Value::Str("apple".to_owned()),
            Value::Bool(true),
            Value::Bytea(vec![0, 255]),
        ] {
            for op in [
                BinaryOperator::Eq,
                BinaryOperator::NotEq,
                BinaryOperator::Lt,
                BinaryOperator::LtEq,
                BinaryOperator::Gt,
                BinaryOperator::GtEq,
            ] {
                let actual = ResolvedOperand::Value(value.clone()).supports_comparison(&op);
                let expected = true;
                assert_eq!(actual, expected, "{value:?} {op:?}");
            }
        }

        // Floating-point ordering is supported; equality is not.
        for value in [Value::F32(0.1), Value::F64(0.1)] {
            for (op, expected) in [
                (BinaryOperator::Eq, false),
                (BinaryOperator::NotEq, false),
                (BinaryOperator::Lt, true),
                (BinaryOperator::LtEq, true),
                (BinaryOperator::Gt, true),
                (BinaryOperator::GtEq, true),
            ] {
                let actual = ResolvedOperand::Value(value.clone()).supports_comparison(&op);
                assert_eq!(actual, expected, "{value:?} {op:?}");
            }
        }

        // Other types must have an INTEGER-backed range.
        for (value, expected) in [
            (Value::I8(12), true),
            (Value::I16(12), true),
            (Value::I32(12), true),
            (Value::I64(12), true),
            (Value::U8(12), true),
            (Value::U16(12), true),
            (Value::U32(12), true),
            (Value::I128(12), false),
            (Value::U64(12), false),
            (Value::U128(12), false),
        ] {
            for op in [
                BinaryOperator::Eq,
                BinaryOperator::NotEq,
                BinaryOperator::Lt,
                BinaryOperator::LtEq,
                BinaryOperator::Gt,
                BinaryOperator::GtEq,
            ] {
                let actual = ResolvedOperand::Value(value.clone()).supports_comparison(&op);
                assert_eq!(actual, expected, "{value:?} {op:?}");
            }
        }
    }

    #[test]
    fn coerce_rhs() {
        // No type on either side: keep the rhs as it is.
        for (left, right, expected) in [
            (Value::Null, Value::I8(12), Value::I8(12)),
            (Value::I8(0), Value::Null, Value::Null),
        ] {
            let actual = ResolvedOperand::Value(left).coerce_rhs(ResolvedOperand::Value(right));
            let expected = Some(ResolvedOperand::Value(expected));
            assert_eq!(actual, expected);
        }

        let actual =
            ResolvedOperand::Value(Value::I8(0)).coerce_rhs(ResolvedOperand::Value(Value::I8(12)));
        let expected = Some(ResolvedOperand::Value(Value::I8(12)));
        assert_eq!(actual, expected);

        // Different types must both be INTEGER-backed before checking their ranges.
        for (left, right) in [
            (Value::F64(0.0), Value::I8(12)),
            (Value::I8(0), Value::F64(12.0)),
        ] {
            let actual = ResolvedOperand::Value(left).coerce_rhs(ResolvedOperand::Value(right));
            let expected = None;
            assert_eq!(actual, expected);
        }

        // A column keeps its identity; its entire declared range must fit the lhs.
        let schema = Schema::from_ddl("CREATE TABLE q (n INT8, wide INT, unsigned UINT8)").unwrap();
        let columns = schema.column_defs.unwrap();
        let narrow = ColumnDefPlan::from(columns[0].clone());
        let wide = ColumnDefPlan::from(columns[1].clone());
        let unsigned = ColumnDefPlan::from(columns[2].clone());

        let actual = ResolvedOperand::Column(&wide).coerce_rhs(ResolvedOperand::Column(&narrow));
        let expected = Some(ResolvedOperand::Column(&narrow));
        assert_eq!(actual, expected);

        // Reject a rhs with a lower minimum or a higher maximum.
        for (left, right) in [(&unsigned, &narrow), (&narrow, &unsigned), (&narrow, &wide)] {
            let actual = ResolvedOperand::Column(left).coerce_rhs(ResolvedOperand::Column(right));
            let expected = None;
            assert_eq!(actual, expected, "{} <- {}", left.name, right.name);
        }

        // A known value can narrow when that value fits, unlike a wider column.
        let actual =
            ResolvedOperand::Column(&narrow).coerce_rhs(ResolvedOperand::Value(Value::I64(12)));
        let expected = Some(ResolvedOperand::Value(Value::I8(12)));
        assert_eq!(actual, expected);

        let actual =
            ResolvedOperand::Column(&narrow).coerce_rhs(ResolvedOperand::Value(Value::I64(128)));
        let expected = None;
        assert_eq!(actual, expected);
    }

    #[test]
    fn literal_to_value() {
        for peer_type in [None, Some(DataType::Text)] {
            let literal = Literal::QuotedString("apple".to_owned());
            let actual = super::literal_to_value(literal, peer_type.as_ref());
            let expected = Some(Value::Str("apple".to_owned()));
            assert_eq!(actual, expected);
        }

        let literal = Literal::QuotedString("12".to_owned());
        let actual = super::literal_to_value(literal, Some(&DataType::Int));
        let expected = None;
        assert_eq!(actual, expected);

        // Numeric literals default to INT when neither operand supplies a type.
        let actual = super::literal_to_value(Literal::Number(12.into()), None);
        let expected = Some(Value::I64(12));
        assert_eq!(actual, expected);

        // Follow the supported target types, with rejected inputs beside each type.
        for (data_type, number, expected) in [
            (DataType::Int8, "12", Some(Value::I8(12))),
            (DataType::Int8, "128", None),
            (DataType::Int16, "12", Some(Value::I16(12))),
            (DataType::Int32, "12", Some(Value::I32(12))),
            (DataType::Int, "12", Some(Value::I64(12))),
            (DataType::Int, "1.0", None),
            (DataType::Uint8, "12", Some(Value::U8(12))),
            (DataType::Uint8, "-1", None),
            (DataType::Uint16, "12", Some(Value::U16(12))),
            (DataType::Uint32, "12", Some(Value::U32(12))),
            (DataType::Float32, "12", Some(Value::F32(12.0))),
            (DataType::Float32, "0.1", Some(Value::F32(0.1))),
            (DataType::Float, "12", Some(Value::F64(12.0))),
            (DataType::Float, "0.1", Some(Value::F64(0.1))),
            (DataType::Boolean, "1", None),
        ] {
            let literal = Literal::Number(number.parse().unwrap());
            let actual = super::literal_to_value(literal, Some(&data_type));
            assert_eq!(actual, expected, "{number} -> {data_type:?}");
        }
    }

    #[test]
    fn convert_integer_value() {
        // The source must be one of the INTEGER-backed value variants.
        for value in [
            Value::F64(12.0),
            Value::I128(12),
            Value::U64(12),
            Value::U128(12),
        ] {
            let actual = super::convert_integer_value(&value, &DataType::Int);
            let expected = None;
            assert_eq!(actual, expected, "{value:?}");
        }

        // Convert to the requested type without wrapping or truncating.
        for (data_type, value, expected) in [
            (DataType::Int8, Value::U32(12), Some(Value::I8(12))),
            (DataType::Int8, Value::I64(128), None),
            (DataType::Int16, Value::I8(12), Some(Value::I16(12))),
            (DataType::Int32, Value::I16(12), Some(Value::I32(12))),
            (DataType::Int32, Value::U32(u32::MAX), None),
            (DataType::Int, Value::I32(12), Some(Value::I64(12))),
            (DataType::Uint8, Value::I64(12), Some(Value::U8(12))),
            (DataType::Uint8, Value::I8(-1), None),
            (DataType::Uint16, Value::U8(12), Some(Value::U16(12))),
            (DataType::Uint32, Value::U16(12), Some(Value::U32(12))),
            (DataType::Float, Value::I8(12), None),
        ] {
            let actual = super::convert_integer_value(&value, &data_type);
            assert_eq!(actual, expected, "{value:?} -> {data_type:?}");
        }
    }
}
