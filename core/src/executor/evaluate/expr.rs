use {
    super::{EvaluateError, Evaluated},
    crate::{
        ast::{AstLiteral, BinaryOperator, DataType, UnaryOperator},
        data::{Literal, Value},
        result::Result,
    },
    std::{borrow::Cow, cmp::Ordering},
};

pub fn literal(ast_literal: &AstLiteral) -> Result<Evaluated<'_>> {
    Literal::try_from(ast_literal).map(Evaluated::Literal)
}

pub fn typed_string<'a>(data_type: &'a DataType, value: Cow<'a, str>) -> Result<Evaluated<'a>> {
    let literal = Literal::Text(value);

    Value::try_from_literal(data_type, &literal).map(Evaluated::Value)
}

pub fn binary_op<'a>(
    op: &BinaryOperator,
    l: Evaluated<'a>,
    r: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    macro_rules! cmp {
        ($expr: expr) => {
            if $expr.is_none() {
                Ok(Evaluated::Value(Value::Null))
            } else {
                Ok(Evaluated::Value(Value::Bool($expr.unwrap())))
            }
        };
    }

    macro_rules! cond {
        (l $op: tt r) => {{
            let l: Option<bool> = l.try_into()?;
            let r: Option<bool> = r.try_into()?;

            match (l, r) {
                (Some(l), Some(r)) => Ok(Evaluated::Value(Value::Bool(l $op r))),
                _ => return Ok(Evaluated::Value(Value::Null))
            }
        }};
    }

    match op {
        BinaryOperator::Plus => l.add(&r),
        BinaryOperator::Minus => l.subtract(&r),
        BinaryOperator::Multiply => l.multiply(&r),
        BinaryOperator::Divide => l.divide(&r),
        BinaryOperator::Modulo => l.modulo(&r),
        BinaryOperator::StringConcat => l.concat(r),
        BinaryOperator::Eq => cmp!(l.evaluate_eq(&r)?),
        BinaryOperator::NotEq => cmp!(l.evaluate_eq(&r)?.map(|eq| !eq)),
        BinaryOperator::Lt => cmp!(l.evaluate_cmp(&r)?.map(|ord| ord == Ordering::Less)),
        BinaryOperator::LtEq => cmp!(l
            .evaluate_cmp(&r)?
            .map(|ord| ord == Ordering::Less || ord == Ordering::Equal)),
        BinaryOperator::Gt => cmp!(l.evaluate_cmp(&r)?.map(|ord| ord == Ordering::Greater)),
        BinaryOperator::GtEq => cmp!(l
            .evaluate_cmp(&r)?
            .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)),
        BinaryOperator::And => cond!(l && r),
        BinaryOperator::Or => cond!(l || r),
        BinaryOperator::Xor => cond!(l ^ r),
        BinaryOperator::BitwiseAnd => l.bitwise_and(&r),
        BinaryOperator::BitwiseShiftLeft => l.bitwise_shift_left(&r),
        BinaryOperator::BitwiseShiftRight => l.bitwise_shift_right(&r),
    }
}

pub fn unary_op<'a>(op: &UnaryOperator, v: Evaluated<'a>) -> Result<Evaluated<'a>> {
    match op {
        UnaryOperator::Plus => v.unary_plus(),
        UnaryOperator::Minus => v.unary_minus(),
        UnaryOperator::Not => v.try_into().map(|v: Option<bool>| match v {
            Some(v) => Evaluated::Value(Value::Bool(!v)),
            None => Evaluated::Value(Value::Null),
        }),
        UnaryOperator::Factorial => v.unary_factorial(),
        UnaryOperator::BitwiseNot => v.unary_bitwise_not(),
    }
}

pub fn between<'a>(
    target: Evaluated<'a>,
    negated: bool,
    low: Evaluated<'a>,
    high: Evaluated<'a>,
) -> Result<Evaluated<'a>> {
    let v = low.evaluate_cmp(&target)? != Some(Ordering::Greater)
        && target.evaluate_cmp(&high)? != Some(Ordering::Greater);
    let v = negated ^ v;

    Ok(Evaluated::Value(Value::Bool(v)))
}

pub fn array_index<'a>(obj: Evaluated<'a>, indexes: Vec<Evaluated<'a>>) -> Result<Evaluated<'a>> {
    let value = match obj {
        Evaluated::Value(value) => value,
        _ => return Err(EvaluateError::MapOrListTypeRequired.into()),
    };
    let indexes = indexes
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>>>()?;
    value.selector_by_index(&indexes).map(Evaluated::Value)
}

#[cfg(test)]
mod test_binary_op {
    use super::*;

    #[test]
    fn test_binary_op() {
        let l = Evaluated::Value(Value::I16(1));
        let r = Evaluated::Value(Value::I16(2));
        let op = BinaryOperator::Plus;
        let result = binary_op(&op, l, r).unwrap();
        assert_eq!(result, Evaluated::Value(Value::I16(3)));

        // All binary operations with a NULL value should return NULL
        for op in &[
            BinaryOperator::Plus,
            BinaryOperator::Minus,
            BinaryOperator::Multiply,
            BinaryOperator::Divide,
            BinaryOperator::Modulo,
            BinaryOperator::Eq,
            BinaryOperator::NotEq,
            BinaryOperator::Lt,
            BinaryOperator::LtEq,
            BinaryOperator::Gt,
            BinaryOperator::GtEq,
        ] {
            let l = Evaluated::Value(Value::I16(1));
            let r = Evaluated::Value(Value::Null);
            let result = binary_op(op, l, r).unwrap();
            assert_eq!(
                result,
                Evaluated::Value(Value::Null),
                "When l is not NULL and r is NULL, the result of operation '{op}' should be NULL"
            );

            let l = Evaluated::Value(Value::Null);
            let r = Evaluated::Value(Value::I16(1));
            let result = binary_op(op, l, r).unwrap();
            assert_eq!(
                result,
                Evaluated::Value(Value::Null),
                "When l is NULL and r is not NULL, the result of operation '{op}' should be NULL"
            );

            let l = Evaluated::Value(Value::Null);
            let r = Evaluated::Value(Value::Null);
            let result = binary_op(op, l, r).unwrap();
            assert_eq!(result, Evaluated::Value(Value::Null));
        }
    }

    #[test]
    fn test_unary_op(){
        let v = Evaluated::Value(Value::I16(1));
        let op = UnaryOperator::Minus;
        let result = unary_op(&op, v).unwrap();
        assert_eq!(result, Evaluated::Value(Value::I16(-1)));

        let v = Evaluated::Value(Value::Bool(true));
        let op = UnaryOperator::Not;
        let result = unary_op(&op, v).unwrap();
        assert_eq!(result, Evaluated::Value(Value::Bool(false)));

        let v = Evaluated::Value(Value::Bool(false));
        let op = UnaryOperator::Not;
        let result = unary_op(&op, v).unwrap();
        assert_eq!(result, Evaluated::Value(Value::Bool(true)));

        let v = Evaluated::Value(Value::I16(1));
        let op = UnaryOperator::BitwiseNot;
        let result = unary_op(&op, v).unwrap();
        assert_eq!(result, Evaluated::Value(Value::I16(-2)));

        // We check the unary operation on a None value
        let v = Evaluated::Value(Value::Null);
        let op = UnaryOperator::Not;
        let result = unary_op(&op, v).unwrap();
        assert_eq!(result, Evaluated::Value(Value::Null));

    }

    #[test]
    fn test_array_index(){
        // Test case where a MapOrListTypeRequired error is returned
        let obj = Evaluated::Literal(Literal::Text("test".into()));
        let indexes = vec![Evaluated::Value(Value::I16(1))];
        assert_eq!(
            Err(EvaluateError::MapOrListTypeRequired.into()),
            array_index(obj, indexes)
        );
    }
}
