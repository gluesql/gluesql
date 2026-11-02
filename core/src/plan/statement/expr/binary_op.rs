use {
    super::{ExprPlan, fmt_expr},
    crate::{
        ast::{BinaryOperator, ToSql},
        plan::explain::ExplainContext,
    },
};

pub(super) fn fmt(
    left: &ExprPlan,
    op: &BinaryOperator,
    right: &ExprPlan,
    context: &mut ExplainContext,
    output: &mut String,
) {
    fmt_expr(left, context, output);
    output.push(' ');
    output.push_str(&op.to_sql());
    output.push(' ');
    fmt_expr(right, context, output);
}

#[cfg(test)]
mod tests {
    use {
        super::{BinaryOperator, ExprPlan},
        crate::{
            parse_sql::parse_expr,
            plan::explain::{Explain, ExplainContext},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(op: BinaryOperator, expected: &str) {
        let actual = ExprPlan::BinaryOp {
            left: Box::new(ExprPlan::Identifier("lhs".to_owned())),
            op,
            right: Box::new(ExprPlan::Identifier("rhs".to_owned())),
        };
        let explained = actual.explain(&mut ExplainContext::default());
        assert_eq!(explained, expected);

        let parsed = parse_expr(&explained).expect(&explained);
        let translated = translate_expr(&parsed, NO_PARAMS).expect(&explained);
        assert_eq!(ExprPlan::from(translated), actual);
    }

    #[test]
    fn explain() {
        test(BinaryOperator::Plus, "lhs + rhs");
        test(BinaryOperator::Minus, "lhs - rhs");
        test(BinaryOperator::Multiply, "lhs * rhs");
        test(BinaryOperator::Divide, "lhs / rhs");
        test(BinaryOperator::Modulo, "lhs % rhs");
        test(BinaryOperator::StringConcat, "lhs || rhs");
        test(BinaryOperator::Gt, "lhs > rhs");
        test(BinaryOperator::Lt, "lhs < rhs");
        test(BinaryOperator::GtEq, "lhs >= rhs");
        test(BinaryOperator::LtEq, "lhs <= rhs");
        test(BinaryOperator::Eq, "lhs = rhs");
        test(BinaryOperator::NotEq, "lhs <> rhs");
        test(BinaryOperator::And, "lhs AND rhs");
        test(BinaryOperator::Or, "lhs OR rhs");
        test(BinaryOperator::Xor, "lhs XOR rhs");
        test(BinaryOperator::BitwiseAnd, "lhs & rhs");
        test(BinaryOperator::BitwiseShiftLeft, "lhs << rhs");
        test(BinaryOperator::BitwiseShiftRight, "lhs >> rhs");
        test(BinaryOperator::Arrow, "lhs -> rhs");
    }
}
