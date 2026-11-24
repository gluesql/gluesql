//! Plans scalar operands in a native `WHERE` condition.
//!
//! [`plan_operand`] looks up column references using the table and column
//! aliases visible in the query. The matching column definition supplies its
//! type and physical name. [`OperandPlan::Unmatched`] lets a caller try another
//! interpretation, such as a condition inside `IS NULL`.
//!
//! Bound values carry their own types, and [scalar functions](function) supply
//! their return types. A literal can take its comparison peer's type. In
//! `qty > 10` with `qty` declared `INT8`, it becomes `Value::I8(10)`.
//! The planner checks the operator for both operands, then checks whether the
//! right operand fits the left operand's type.

mod function;

use {
    self::function::{ResolvedFunction, plan_function_call},
    gluesql_core::{
        ast::{BinaryOperator, DataType, Literal, UnaryOperator},
        data::{BigDecimalExt, Value},
        plan::{ColumnDefPlan, ExprPlan},
    },
};

#[derive(Clone, Debug, PartialEq)]
pub(super) enum OperandPlan<'a> {
    // Only a literal can acquire its comparison peer's type.
    Literal(Literal),
    Resolved(ResolvedOperand<'a>),
    Unmatched,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum ResolvedOperand<'a> {
    Column(&'a ColumnDefPlan),
    Value(Value),
    Function(ResolvedFunction<'a>),
}

pub(super) fn plan_operand<'a>(
    expr: &ExprPlan,
    table_alias: &str,
    columns: &'a [ColumnDefPlan],
    column_aliases: &[String],
) -> OperandPlan<'a> {
    match expr {
        ExprPlan::Nested(expr) => plan_operand(expr, table_alias, columns, column_aliases),
        ExprPlan::CompoundIdentifier { alias, .. } if alias != table_alias => {
            OperandPlan::Unmatched
        }
        ExprPlan::Identifier(name) | ExprPlan::CompoundIdentifier { ident: name, .. } => {
            let Some(column) = columns.iter().enumerate().find_map(|(index, column)| {
                let visible_name = column_aliases.get(index).unwrap_or(&column.name);
                (visible_name == name).then_some(column)
            }) else {
                return OperandPlan::Unmatched;
            };

            OperandPlan::Resolved(ResolvedOperand::Column(column))
        }
        ExprPlan::Literal(literal) => OperandPlan::Literal(literal.clone()),
        ExprPlan::Value(value) => OperandPlan::Resolved(ResolvedOperand::Value(value.clone())),
        ExprPlan::UnaryOp {
            op: op @ (UnaryOperator::Plus | UnaryOperator::Minus),
            expr,
        } => {
            let OperandPlan::Literal(Literal::Number(number)) =
                plan_operand(expr, table_alias, columns, column_aliases)
            else {
                return OperandPlan::Unmatched;
            };

            let number = if matches!(op, UnaryOperator::Minus) {
                -number
            } else {
                number
            };
            OperandPlan::Literal(Literal::Number(number))
        }
        ExprPlan::Function(function) => {
            let Some(function) = plan_function_call(function, table_alias, columns, column_aliases)
            else {
                return OperandPlan::Unmatched;
            };

            OperandPlan::Resolved(ResolvedOperand::Function(function))
        }
        _ => OperandPlan::Unmatched,
    }
}

impl<'a> OperandPlan<'a> {
    pub(super) fn with_literal_type_from(self, peer: &Self) -> Option<ResolvedOperand<'a>> {
        match peer {
            Self::Literal(_) => self.with_literal_type(None),
            Self::Resolved(operand) => self.with_literal_type(operand.data_type().as_ref()),
            Self::Unmatched => None,
        }
    }

    pub(super) fn with_literal_type(
        self,
        peer_type: Option<&DataType>,
    ) -> Option<ResolvedOperand<'a>> {
        match self {
            Self::Literal(literal) => {
                literal_to_value(literal, peer_type).map(ResolvedOperand::Value)
            }
            Self::Resolved(operand) => Some(operand),
            Self::Unmatched => None,
        }
    }
}

pub(super) fn sqlite_allows_operator(
    left: &ResolvedOperand<'_>,
    op: &BinaryOperator,
    right: &ResolvedOperand<'_>,
) -> bool {
    [left, right]
        .into_iter()
        .all(|operand| match operand.data_type() {
            Some(DataType::Text) if matches!(operand, ResolvedOperand::Function(_)) => {
                // SQLite orders TEXT by the file encoding, while GlueSQL orders UTF-8.
                matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq)
            }
            None | Some(DataType::Text | DataType::Boolean | DataType::Bytea) => true,
            Some(DataType::Float32 | DataType::Float) => {
                // Value float equality uses an epsilon; SQLite equality is exact.
                !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq)
            }
            Some(data_type) => integer_bounds(&data_type).is_some(),
        })
}

impl ResolvedOperand<'_> {
    pub(super) fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Column(column) => Some(column.data_type.clone()),
            Self::Value(value) => value.get_type(),
            Self::Function(function) => function.return_type.clone(),
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

        match (self, right) {
            // A function can return a numeric literal, which GlueSQL compares at its peer's width.
            (Self::Function(_), _) | (_, Self::Function(_)) => None,
            (_, Self::Column(column)) => {
                if left_min > right_min || right_max > left_max {
                    return None;
                }

                Some(Self::Column(column))
            }
            (_, Self::Value(value)) => convert_integer_value(&value, &left_type).map(Self::Value),
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
        super::{super::tests::expr, OperandPlan, ResolvedOperand},
        gluesql_core::{
            ast::{
                BinaryOperator::{Eq, Gt, GtEq, Lt, LtEq, NotEq},
                DataType, Literal,
            },
            data::{Schema, Value},
            plan::ColumnDefPlan,
            translate::IntoParamLiteral,
        },
    };

    #[test]
    fn plan_operand() {
        let schema = Schema::from_ddl("CREATE TABLE Item (n INT8, label TEXT)").unwrap();
        let columns = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect::<Vec<_>>();

        // Nested, unqualified and source-qualified names resolve to the same column.
        for sql in ["((n))", "n", "q.n"] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &columns, &[]);
            let expected = OperandPlan::Resolved(ResolvedOperand::Column(&columns[0]));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["unknown", "q.unknown", "outer_row.n"] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &columns, &[]);
            assert_eq!(actual, OperandPlan::Unmatched, "{sql}");
        }

        // A positional alias replaces the visible name, not the physical column.
        let aliases = ["renamed".to_owned()];
        for (sql, column) in [
            ("renamed", &columns[0]),
            ("q.renamed", &columns[0]),
            ("label", &columns[1]),
        ] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &columns, &aliases);
            let expected = OperandPlan::Resolved(ResolvedOperand::Column(column));
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = super::plan_operand(&expr("n", &[]), "q", &columns, &aliases);
        assert_eq!(actual, OperandPlan::Unmatched);

        // Literals remain syntax until a peer type is available.
        for (sql, literal) in [
            ("12", Literal::Number(12.into())),
            ("'apple'", Literal::QuotedString("apple".to_owned())),
        ] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &[], &[]);
            let expected = OperandPlan::Literal(literal);
            assert_eq!(actual, expected, "{sql}");
        }

        let params = [12_i64.into_param_literal()];
        let actual = super::plan_operand(&expr("$1", &params), "q", &[], &[]);
        let expected = OperandPlan::Resolved(ResolvedOperand::Value(Value::I64(12)));
        assert_eq!(actual, expected);

        // Unary signs normalize numeric literals only.
        for (sql, number) in [("+12", 12), ("-12", -12)] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &[], &[]);
            let expected = OperandPlan::Literal(Literal::Number(number.into()));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["-renamed", "-$1", "-'apple'", "-unknown"] {
            let actual = super::plan_operand(&expr(sql, &params), "q", &columns, &aliases);
            assert_eq!(actual, OperandPlan::Unmatched, "{sql}");
        }

        let sql = "COALESCE(NULLIF(q.renamed, 7), IFNULL(NULL, q.renamed))";
        let OperandPlan::Resolved(ResolvedOperand::Function(function)) =
            super::plan_operand(&expr(sql, &[]), "q", &columns, &aliases)
        else {
            panic!("expected a function using the aliased column");
        };
        assert_eq!(function.return_type, Some(DataType::Int8));

        for sql in [
            "renamed + 1",
            "LENGTH(label)",
            "CAST(renamed AS INT)",
            "(SELECT 1)",
        ] {
            let actual = super::plan_operand(&expr(sql, &[]), "q", &columns, &aliases);
            assert_eq!(actual, OperandPlan::Unmatched, "{sql}");
        }
    }

    #[test]
    fn with_literal_type_from() {
        let literal = OperandPlan::Literal(Literal::Number(12.into()));

        let peer = OperandPlan::Literal(Literal::Number(7.into()));
        let actual = literal.clone().with_literal_type_from(&peer);
        let expected = Some(ResolvedOperand::Value(Value::I64(12)));
        assert_eq!(actual, expected);

        let peer = OperandPlan::Resolved(ResolvedOperand::Value(Value::I8(7)));
        let actual = literal.clone().with_literal_type_from(&peer);
        let expected = Some(ResolvedOperand::Value(Value::I8(12)));
        assert_eq!(actual, expected);

        let peer = OperandPlan::Resolved(ResolvedOperand::Value(Value::Null));
        let actual = literal.clone().with_literal_type_from(&peer);
        let expected = Some(ResolvedOperand::Value(Value::I64(12)));
        assert_eq!(actual, expected);

        let actual = literal.with_literal_type_from(&OperandPlan::Unmatched);
        assert_eq!(actual, None);
    }

    #[test]
    fn with_literal_type() {
        let schema = Schema::from_ddl("CREATE TABLE q (n INT8)").unwrap();
        let columns = schema.column_defs.unwrap();
        let column = ColumnDefPlan::from(columns[0].clone());

        let actual = OperandPlan::Resolved(ResolvedOperand::Column(&column))
            .with_literal_type(Some(&DataType::Int));
        let expected = Some(ResolvedOperand::Column(&column));
        assert_eq!(actual, expected);

        // Only a literal acquires the peer type during resolution.
        let literal = OperandPlan::Literal(Literal::Number(12.into()));
        let actual = literal.with_literal_type(Some(&DataType::Int8));
        let expected = Some(ResolvedOperand::Value(Value::I8(12)));
        assert_eq!(actual, expected);

        let literal = OperandPlan::Literal(Literal::Number(128.into()));
        let actual = literal.with_literal_type(Some(&DataType::Int8));
        let expected = None;
        assert_eq!(actual, expected);

        // Typed values, including NULL, pass through without conversion.
        for value in [Value::I64(12), Value::Null] {
            let actual = OperandPlan::Resolved(ResolvedOperand::Value(value.clone()))
                .with_literal_type(Some(&DataType::Int8));
            let expected = Some(ResolvedOperand::Value(value));
            assert_eq!(actual, expected);
        }

        assert_eq!(OperandPlan::Unmatched.with_literal_type(None), None);
    }

    #[test]
    fn sqlite_allows_operator() {
        let OperandPlan::Resolved(text_function) =
            super::plan_operand(&expr("IFNULL('ÿ', '')", &[]), "q", &[], &[])
        else {
            panic!("expected a resolved text function");
        };
        let text = ResolvedOperand::Value(Value::Str(String::new()));

        assert!(super::sqlite_allows_operator(&text_function, &Eq, &text));
        assert!(super::sqlite_allows_operator(&text_function, &NotEq, &text));
        assert!(!super::sqlite_allows_operator(&text_function, &Lt, &text));
        assert!(!super::sqlite_allows_operator(&text, &Lt, &text_function));

        for value in [
            Value::Null,
            Value::Str("apple".to_owned()),
            Value::Bool(true),
            Value::Bytea(vec![0, 255]),
        ] {
            let operand = ResolvedOperand::Value(value);
            for op in [Eq, NotEq, Lt, LtEq, Gt, GtEq] {
                let actual = super::sqlite_allows_operator(&operand, &op, &operand);
                let expected = true;
                assert_eq!(actual, expected, "{operand:?} {op:?}");
            }
        }

        // Floating-point ordering is supported; equality is not.
        for value in [Value::F32(0.1), Value::F64(0.1)] {
            let operand = ResolvedOperand::Value(value);
            for (op, expected) in [
                (Eq, false),
                (NotEq, false),
                (Lt, true),
                (LtEq, true),
                (Gt, true),
                (GtEq, true),
            ] {
                let actual = super::sqlite_allows_operator(&operand, &op, &operand);
                assert_eq!(actual, expected, "{operand:?} {op:?}");
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
            let operand = ResolvedOperand::Value(value);
            for op in [Eq, NotEq, Lt, LtEq, Gt, GtEq] {
                let actual = super::sqlite_allows_operator(&operand, &op, &operand);
                assert_eq!(actual, expected, "{operand:?} {op:?}");
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

        let schema = Schema::from_ddl("CREATE TABLE q (n INT8, wide INT, unsigned UINT8)").unwrap();
        let columns = schema.column_defs.unwrap();
        let narrow = ColumnDefPlan::from(columns[0].clone());
        let wide = ColumnDefPlan::from(columns[1].clone());
        let unsigned = ColumnDefPlan::from(columns[2].clone());

        // When wide is NULL, GlueSQL compares the literal 1000 at the peer's width.
        let expr = expr("IFNULL(wide, 1000)", &[]);
        let OperandPlan::Resolved(if_null) =
            super::plan_operand(&expr, "q", std::slice::from_ref(&wide), &[])
        else {
            panic!("expected a resolved integer function");
        };

        let actual = if_null.coerce_rhs(ResolvedOperand::Column(&narrow));
        assert_eq!(actual, None);

        let actual = ResolvedOperand::Column(&narrow).coerce_rhs(if_null);
        assert_eq!(actual, None);

        // A rhs column keeps its identity when its full range fits the lhs.
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
