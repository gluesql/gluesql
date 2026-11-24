//! Plans scalar function calls used as operands in a native `WHERE` condition.
//!
//! [`plan_function_call`] plans each argument with the table and column aliases
//! visible to the caller. [`ResolvedFunction`] records the SQL name and typed
//! arguments for rendering the call, and the return type for the enclosing
//! expression.
//!
//! `IFNULL` and `COALESCE` take their return type from their typed arguments,
//! which must agree. `NULLIF(a, b)` checks `a = b` using the filter's comparison
//! rules and keeps the type of `a` because it can return `a`. In
//! `NULLIF(qty, 0) > 10`, the outer comparison uses that return type to bind `10`.

use {
    super::{ResolvedOperand, plan_operand, sqlite_allows_operator},
    gluesql_core::{
        ast::{BinaryOperator, DataType},
        plan::{ColumnDefPlan, ExprPlan, FunctionPlan},
    },
};

#[derive(Clone, Debug, PartialEq)]
pub(in super::super) struct ResolvedFunction<'a> {
    pub(in super::super) name: &'static str,
    pub(in super::super) args: Vec<ResolvedOperand<'a>>,
    /// `None` represents an untyped `NULL`, as in `COALESCE(NULL, NULL)`.
    pub(super) return_type: Option<DataType>,
}

pub(super) fn plan_function_call<'a>(
    function: &FunctionPlan,
    table_alias: &str,
    columns: &'a [ColumnDefPlan],
    column_aliases: &[String],
) -> Option<ResolvedFunction<'a>> {
    match function {
        FunctionPlan::IfNull { expr, then } => {
            plan_first_non_null("IFNULL", [expr, then], table_alias, columns, column_aliases)
        }
        // GlueSQL also accepts a single argument, which SQLite rejects.
        FunctionPlan::Coalesce(exprs) if exprs.len() >= 2 => {
            plan_first_non_null("COALESCE", exprs, table_alias, columns, column_aliases)
        }
        FunctionPlan::NullIf { expr1, expr2 } => {
            // The first argument can be returned unchanged, so resolve it at its own type.
            let value = plan_operand(expr1, table_alias, columns, column_aliases)
                .with_literal_type(None)?;
            let return_type = value.data_type();
            let other = plan_operand(expr2, table_alias, columns, column_aliases)
                .with_literal_type(return_type.as_ref())?;

            if !sqlite_allows_operator(&value, &BinaryOperator::Eq, &other) {
                return None;
            }

            let other = value.coerce_rhs(other)?;
            Some(ResolvedFunction {
                name: "NULLIF",
                args: vec![value, other],
                return_type,
            })
        }
        _ => None,
    }
}

fn plan_first_non_null<'a, 'expr>(
    name: &'static str,
    exprs: impl IntoIterator<Item = &'expr ExprPlan>,
    table_alias: &str,
    columns: &'a [ColumnDefPlan],
    column_aliases: &[String],
) -> Option<ResolvedFunction<'a>> {
    let mut args = Vec::new();
    let mut return_type = None;

    for expr in exprs {
        let arg =
            plan_operand(expr, table_alias, columns, column_aliases).with_literal_type(None)?;
        if let Some(arg_type) = arg.data_type() {
            if return_type
                .as_ref()
                .is_some_and(|known_type| *known_type != arg_type)
            {
                return None;
            }

            return_type = Some(arg_type);
        }

        args.push(arg);
    }

    Some(ResolvedFunction {
        name,
        args,
        return_type,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{ResolvedFunction, ResolvedOperand, plan_function_call},
        crate::planner::native_filter::condition::tests::expr,
        gluesql_core::{
            ast::DataType,
            data::{Schema, Value},
            plan::{ColumnDefPlan, ExprPlan},
        },
    };

    fn plan_call<'a>(sql: &str, columns: &'a [ColumnDefPlan]) -> Option<ResolvedFunction<'a>> {
        let ExprPlan::Function(call) = expr(sql, &[]) else {
            panic!("expected a function: {sql}");
        };
        plan_function_call(&call, "q", columns, &[])
    }

    #[test]
    fn if_null() {
        let actual = plan_call("IFNULL(NULL, 7)", &[]);
        let expected = Some(ResolvedFunction {
            name: "IFNULL",
            args: vec![
                ResolvedOperand::Value(Value::Null),
                ResolvedOperand::Value(Value::I64(7)),
            ],
            return_type: Some(DataType::Int),
        });
        assert_eq!(actual, expected);

        let schema = Schema::from_ddl("CREATE TABLE q (n INT8, f FLOAT32)").unwrap();
        let columns: Vec<_> = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect();

        // A numeric literal keeps its standalone type regardless of the other argument.
        for sql in ["IFNULL(n, 7)", "IFNULL(0.5, f)", "IFNULL(f, 0.5)"] {
            let actual = plan_call(sql, &columns);
            assert_eq!(actual, None, "{sql}");
        }

        let actual = plan_call("IFNULL(outer_row.n, n)", &columns);
        assert_eq!(actual, None);
    }

    #[test]
    fn coalesce() {
        let actual = plan_call("COALESCE()", &[]);
        assert_eq!(actual, None);

        let actual = plan_call("COALESCE(1)", &[]);
        assert_eq!(actual, None);

        let actual = plan_call("COALESCE(NULL, 1, NULL, 2)", &[]);
        let expected = Some(ResolvedFunction {
            name: "COALESCE",
            args: vec![
                ResolvedOperand::Value(Value::Null),
                ResolvedOperand::Value(Value::I64(1)),
                ResolvedOperand::Value(Value::Null),
                ResolvedOperand::Value(Value::I64(2)),
            ],
            return_type: Some(DataType::Int),
        });
        assert_eq!(actual, expected);

        let actual = plan_call("COALESCE(NULL, NULL)", &[]).unwrap().return_type;
        assert_eq!(actual, None);

        let actual = plan_call("COALESCE(NULL, 1, 'text')", &[]);
        assert_eq!(actual, None);

        let actual = plan_call("COALESCE(1, 2, 1 / 0)", &[]);
        assert_eq!(actual, None);
    }

    #[test]
    fn null_if() {
        let schema = Schema::from_ddl("CREATE TABLE q (n INT8, f FLOAT32)").unwrap();
        let columns: Vec<_> = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect();

        let actual = plan_call("NULLIF(n, 7)", &columns);
        let expected = Some(ResolvedFunction {
            name: "NULLIF",
            args: vec![
                ResolvedOperand::Column(&columns[0]),
                ResolvedOperand::Value(Value::I8(7)),
            ],
            return_type: Some(DataType::Int8),
        });
        assert_eq!(actual, expected);

        // The first argument can also be returned. Keep its standalone type.
        let actual = plan_call("NULLIF(1000, n)", &columns);
        let expected = Some(ResolvedFunction {
            name: "NULLIF",
            args: vec![
                ResolvedOperand::Value(Value::I64(1000)),
                ResolvedOperand::Column(&columns[0]),
            ],
            return_type: Some(DataType::Int),
        });
        assert_eq!(actual, expected);

        let actual = plan_call("NULLIF(NULL, n)", &columns).unwrap().return_type;
        assert_eq!(actual, None);

        for sql in [
            "NULLIF(n + 1, n)",
            "NULLIF(0.5, f)",
            "NULLIF(n, n + 1)",
            "NULLIF(n, 1000)",
            "NULLIF(f, f)",
            "NULLIF(n, TRUE)",
        ] {
            let actual = plan_call(sql, &columns);
            assert_eq!(actual, None, "{sql}");
        }
    }
}
