use {
    super::{ExprPlan, fmt_expr},
    crate::{
        ast::{ToSql, UnaryOperator},
        plan::explain::ExplainContext,
    },
};

pub(super) fn fmt(
    op: &UnaryOperator,
    expr: &ExprPlan,
    context: &mut ExplainContext,
    output: &mut String,
) {
    if op == &UnaryOperator::Factorial {
        fmt_expr(expr, context, output);
        output.push_str(&op.to_sql());
    } else {
        output.push_str(&op.to_sql());
        fmt_expr(expr, context, output);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{ExprPlan, UnaryOperator},
        crate::{
            parse_sql::parse_expr,
            plan::explain::{Explain, ExplainContext},
            translate::{NO_PARAMS, translate_expr},
        },
    };

    fn test(op: UnaryOperator, expected: &str) {
        let actual = ExprPlan::UnaryOp {
            op,
            expr: Box::new(ExprPlan::Identifier("operand".to_owned())),
        };
        let explained = actual.explain(&mut ExplainContext::default());
        assert_eq!(explained, expected);

        let parsed = parse_expr(&explained).expect(&explained);
        let translated = translate_expr(&parsed, NO_PARAMS).expect(&explained);
        assert_eq!(ExprPlan::from(translated), actual);
    }

    #[test]
    fn explain() {
        test(UnaryOperator::Plus, "+operand");
        test(UnaryOperator::Minus, "-operand");
        test(UnaryOperator::Not, "NOT operand");
        test(UnaryOperator::Factorial, "operand!");
        test(UnaryOperator::BitwiseNot, "~operand");
    }
}
