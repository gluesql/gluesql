//! Splits a single-table `WHERE` clause between `SQLite` and the common executor.
//!
//! [`ConditionPlanner::push_down_where_terms`] considers each top-level `AND`
//! term independently. [`ConditionPlanner::build_condition_sql`] translates a
//! whole term, including `OR` and `NOT`, when `SQLite` can evaluate it with
//! `GlueSQL` semantics. If a term stays in the common filter, the planner
//! discards any parameters it collected for that term.
//!
//! [`operand::plan_operand`] identifies the columns, values and functions in
//! each condition. [`ConditionPlanner::render_operand`] writes physical column
//! names and binds values to numbered placeholders, visiting function arguments
//! from left to right.
//!
//! [`ConditionPlan`] records whether the filter stays unchanged or moves to
//! `SQLite` in full or in part. Both delegated cases carry SQL and parameters in
//! placeholder order. Partial delegation also carries the remaining expression.

mod operand;

use {
    self::operand::{OperandPlan, ResolvedOperand, plan_operand, sqlite_allows_operator},
    crate::schema::qualified_column,
    gluesql_core::{
        ast::{BinaryOperator, DataType, ToSql, UnaryOperator},
        data::Value,
        plan::{ColumnDefPlan, ExprPlan},
    },
};

#[derive(Debug, PartialEq)]
pub(super) enum ConditionPlan {
    Unchanged,
    Full {
        sql: String,
        params: Vec<Value>,
    },
    Partial {
        sql: String,
        params: Vec<Value>,
        remaining: ExprPlan,
    },
}

pub(super) fn plan(
    expr: &ExprPlan,
    table_name: &str,
    table_alias: &str,
    columns: &[ColumnDefPlan],
    column_aliases: &[String],
) -> ConditionPlan {
    let mut planner = ConditionPlanner::new(table_name, table_alias, columns, column_aliases);
    let remaining = planner.push_down_where_terms(expr);

    if planner.sql_conditions.is_empty() {
        return ConditionPlan::Unchanged;
    }

    let sql = planner.sql_conditions.join(" AND ");
    let params = planner.params;
    match remaining {
        None => ConditionPlan::Full { sql, params },
        Some(remaining) => ConditionPlan::Partial {
            sql,
            params,
            remaining,
        },
    }
}

struct ConditionPlanner<'a> {
    table_name: &'a str,
    table_alias: &'a str,
    columns: &'a [ColumnDefPlan],
    column_aliases: &'a [String],
    sql_conditions: Vec<String>,
    params: Vec<Value>,
}

impl<'a> ConditionPlanner<'a> {
    fn new(
        table_name: &'a str,
        table_alias: &'a str,
        columns: &'a [ColumnDefPlan],
        column_aliases: &'a [String],
    ) -> Self {
        Self {
            table_name,
            table_alias,
            columns,
            column_aliases,
            sql_conditions: Vec::new(),
            params: Vec::new(),
        }
    }

    /// Collects native conditions and their parameters, returning the filter for the common executor.
    fn push_down_where_terms(&mut self, expr: &ExprPlan) -> Option<ExprPlan> {
        match expr {
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = self.push_down_where_terms(left);
                let right = self.push_down_where_terms(right);

                match (left, right) {
                    (Some(left), Some(right)) => Some(ExprPlan::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::And,
                        right: Box::new(right),
                    }),
                    (Some(remaining), None) | (None, Some(remaining)) => Some(remaining),
                    (None, None) => None,
                }
            }
            ExprPlan::Nested(expr) => self
                .push_down_where_terms(expr)
                .map(|remaining| ExprPlan::Nested(Box::new(remaining))),
            // OR/NOT and other expressions move as a whole. Extracting an AND from
            // inside one of them could discard rows that satisfy the original condition.
            _ => {
                let param_count = self.params.len();
                let Some(sql) = self.build_condition_sql(expr) else {
                    // Translation may fail after binding a child; discard those partial bindings.
                    self.params.truncate(param_count);
                    return Some(expr.clone());
                };

                self.sql_conditions.push(sql);
                None
            }
        }
    }

    fn build_condition_sql(&mut self, expr: &ExprPlan) -> Option<String> {
        match expr {
            // Composite SQL already includes the parentheses needed for precedence.
            ExprPlan::Nested(expr) => self.build_condition_sql(expr),
            ExprPlan::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Some(format!("(NOT {})", self.build_condition_sql(expr)?)),
            ExprPlan::BinaryOp {
                left,
                op: op @ (BinaryOperator::And | BinaryOperator::Or),
                right,
            } => {
                let left = self.build_condition_sql(left)?;
                let right = self.build_condition_sql(right)?;

                Some(format!("({left} {} {right})", op.to_sql()))
            }
            ExprPlan::BinaryOp {
                left,
                op:
                    op @ (BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq),
                right,
            } => self.build_comparison_sql(left, op, right),
            ExprPlan::IsNull(inner) | ExprPlan::IsNotNull(inner) => {
                // Null checks also work on types whose comparisons cannot be delegated.
                let operand =
                    plan_operand(inner, self.table_alias, self.columns, self.column_aliases);
                let sql = match operand {
                    OperandPlan::Unmatched => self.build_condition_sql(inner)?,
                    operand => self.render_operand(operand.with_literal_type(None)?)?,
                };

                let op = if matches!(expr, ExprPlan::IsNull(_)) {
                    "IS NULL"
                } else {
                    "IS NOT NULL"
                };

                Some(format!("({sql} {op})"))
            }
            ExprPlan::InList {
                expr,
                list,
                negated,
            } => self.build_in_list_sql(expr, list, *negated),
            ExprPlan::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // Keep the common evaluator's operand direction: low <= target and
                // target <= high. Typed-value conversions can depend on the lhs type.
                let low = self.build_comparison_sql(low, &BinaryOperator::LtEq, expr)?;
                let high = self.build_comparison_sql(expr, &BinaryOperator::LtEq, high)?;

                let between = format!("({low} AND {high})");
                Some(if *negated {
                    format!("(NOT {between})")
                } else {
                    between
                })
            }
            _ => {
                // SQLite accepts numbers/text as truthy; GlueSQL requires BOOLEAN or NULL.
                let operand =
                    plan_operand(expr, self.table_alias, self.columns, self.column_aliases)
                        .with_literal_type(None)?;
                if operand
                    .data_type()
                    .is_some_and(|data_type| data_type != DataType::Boolean)
                {
                    return None;
                }

                self.render_operand(operand)
            }
        }
    }

    fn build_comparison_sql(
        &mut self,
        left: &ExprPlan,
        op: &BinaryOperator,
        right: &ExprPlan,
    ) -> Option<String> {
        let left = plan_operand(left, self.table_alias, self.columns, self.column_aliases);
        if matches!(left, OperandPlan::Unmatched) {
            return None;
        }

        let right = plan_operand(right, self.table_alias, self.columns, self.column_aliases);
        let left = left.with_literal_type_from(&right)?;
        let right = right.with_literal_type(left.data_type().as_ref())?;

        if !sqlite_allows_operator(&left, op, &right) {
            return None;
        }

        let right = left.coerce_rhs(right)?;

        let left_sql = self.render_operand(left)?;
        let right_sql = self.render_operand(right)?;
        Some(format!("({left_sql} {} {right_sql})", op.to_sql()))
    }

    fn build_in_list_sql(
        &mut self,
        expr: &ExprPlan,
        list: &[ExprPlan],
        negated: bool,
    ) -> Option<String> {
        let target = plan_operand(expr, self.table_alias, self.columns, self.column_aliases);
        if matches!(target, OperandPlan::Unmatched) {
            return None;
        }

        let mut resolved_target = None;
        let mut items = Vec::new();

        for item in list {
            let item = plan_operand(item, self.table_alias, self.columns, self.column_aliases);
            if matches!(item, OperandPlan::Unmatched) {
                return None;
            }

            let target = target.clone().with_literal_type_from(&item)?;
            let item = item.with_literal_type(target.data_type().as_ref())?;

            if !sqlite_allows_operator(&target, &BinaryOperator::Eq, &item) {
                return None;
            }

            let item = target.coerce_rhs(item)?;

            // SQL IN binds the target once, so every item must resolve it identically.
            if resolved_target
                .as_ref()
                .is_some_and(|previous| previous != &target)
            {
                return None;
            }

            resolved_target = Some(target);
            items.push(item);
        }

        let target_sql = self.render_operand(resolved_target?)?;
        let items_sql = items
            .into_iter()
            .map(|item| self.render_operand(item))
            .collect::<Option<Vec<_>>>()?;

        let op = if negated { "NOT IN" } else { "IN" };

        Some(format!("({target_sql} {op} ({}))", items_sql.join(", ")))
    }

    fn render_operand(&mut self, operand: ResolvedOperand<'_>) -> Option<String> {
        match operand {
            ResolvedOperand::Column(column) => {
                Some(qualified_column(self.table_name, &column.name))
            }
            ResolvedOperand::Value(value) => {
                // The codec rejects NaN, so leave its evaluation on the common path.
                if matches!(&value, Value::F32(v) if v.is_nan())
                    || matches!(&value, Value::F64(v) if v.is_nan())
                {
                    return None;
                }

                self.params.push(value);
                Some(format!("?{}", self.params.len()))
            }
            ResolvedOperand::Function(function) => {
                let args = function
                    .args
                    .into_iter()
                    .map(|arg| self.render_operand(arg))
                    .collect::<Option<Vec<_>>>()?;
                Some(format!("{}({})", function.name, args.join(", ")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            ConditionPlan, ConditionPlanner,
            operand::{OperandPlan, ResolvedOperand, plan_operand},
            plan as plan_condition,
        },
        gluesql_core::{
            data::{Schema, Value},
            parse_sql::parse_expr,
            plan::{ColumnDefPlan, ExprPlan},
            translate::{IntoParamLiteral, ParamLiteral, translate_expr},
        },
    };

    pub(super) fn expr(sql: &str, params: &[ParamLiteral]) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), params)
            .expect(sql)
            .into()
    }

    fn plan(ddl: &str, sql: &str) -> ConditionPlan {
        let schema = Schema::from_ddl(ddl).unwrap();
        let columns = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect::<Vec<_>>();

        plan_condition(
            &expr(sql, &[]),
            &schema.table_name,
            &schema.table_name,
            &columns,
            &[],
        )
    }

    fn sql_and_params(
        ddl: &str,
        sql: &str,
        params: &[ParamLiteral],
    ) -> Option<(String, Vec<Value>)> {
        let schema = Schema::from_ddl(ddl).unwrap();
        let columns = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect::<Vec<_>>();
        let mut planner =
            ConditionPlanner::new(&schema.table_name, &schema.table_name, &columns, &[]);
        let sql = planner.build_condition_sql(&expr(sql, params))?;
        Some((sql, planner.params))
    }

    #[test]
    fn push_down_where_terms() {
        let ddl = "CREATE TABLE Item (id INT, qty INT)";

        // AND: both sides remain, with no native conditions.
        let actual = plan(ddl, "qty + 1 > 2 AND id + 1 < 10");
        let expected = ConditionPlan::Unchanged;
        assert_eq!(actual, expected);

        // AND: rebuild both residual sides after extracting their native terms.
        let ddl = "CREATE TABLE Item (id INT, name TEXT, qty INT)";
        let sql = "
            (qty > 10 AND LOWER(name) = 'apple')
            AND (LENGTH(name) > 3 AND id < 20)
        ";
        let actual = plan(ddl, sql);
        let expected = ConditionPlan::Partial {
            sql: r#"("Item"."qty" > ?1) AND ("Item"."id" < ?2)"#.to_owned(),
            params: vec![Value::I64(10), Value::I64(20)],
            remaining: expr("(LOWER(name) = 'apple') AND (LENGTH(name) > 3)", &[]),
        };
        assert_eq!(actual, expected);

        // AND: retain only the left or right residual.
        for sql in [
            "LOWER(name) = 'apple' AND qty > 10",
            "qty > 10 AND LOWER(name) = 'apple'",
        ] {
            let actual = plan(ddl, sql);
            let expected = ConditionPlan::Partial {
                sql: r#"("Item"."qty" > ?1)"#.to_owned(),
                params: vec![Value::I64(10)],
                remaining: expr("LOWER(name) = 'apple'", &[]),
            };
            assert_eq!(actual, expected, "{sql}");
        }

        // AND and Nested: wrappers disappear when no residual remains.
        for sql in ["qty > 10 AND id < 20", "((qty > 10 AND id < 20))"] {
            let actual = plan(ddl, sql);
            let expected = ConditionPlan::Full {
                sql: r#"("Item"."qty" > ?1) AND ("Item"."id" < ?2)"#.to_owned(),
                params: vec![Value::I64(10), Value::I64(20)],
            };
            assert_eq!(actual, expected, "{sql}");
        }

        // Nested preserves the wrapper around the remaining expression.
        let actual = plan(ddl, "(qty > 10 AND LOWER(name) = 'apple')");
        let expected = ConditionPlan::Partial {
            sql: r#"("Item"."qty" > ?1)"#.to_owned(),
            params: vec![Value::I64(10)],
            remaining: expr("(LOWER(name) = 'apple')", &[]),
        };
        assert_eq!(actual, expected);

        // Other expressions: a supported OR/NOT group moves as a whole.
        let sql = "
            (qty > 10 OR qty < 0)
            AND NOT (id = 5)
        ";
        let actual = plan(ddl, sql);
        let expected = ConditionPlan::Full {
            sql: r#"(("Item"."qty" > ?1) OR ("Item"."qty" < ?2)) AND (NOT ("Item"."id" = ?3))"#
                .to_owned(),
            params: vec![Value::I64(10), Value::I64(0), Value::I64(5)],
        };
        assert_eq!(actual, expected);

        // A failed OR/NOT group remains whole, including any supported inner terms.
        for sql in [
            "qty > 10 OR LOWER(name) = 'apple'",
            "NOT (qty > 10 AND LOWER(name) = 'apple')",
        ] {
            let actual = plan(ddl, sql);
            let expected = ConditionPlan::Unchanged;
            assert_eq!(actual, expected, "{sql}");
        }

        // Keep the function's parameters and reuse the failed OR group's position.
        let sql = "
            NULLIF(qty, 0) > 10
            AND (id = 99 OR LOWER(name) = 'apple')
            AND id < 20
        ";
        let actual = plan(ddl, sql);
        let expected = ConditionPlan::Partial {
            sql: r#"(NULLIF("Item"."qty", ?1) > ?2) AND ("Item"."id" < ?3)"#.to_owned(),
            params: vec![Value::I64(0), Value::I64(10), Value::I64(20)],
            remaining: expr("(id = 99 OR LOWER(name) = 'apple')", &[]),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn build_condition_sql() {
        let ddl = "CREATE TABLE q (n INT8, enabled BOOLEAN, doc MAP, f FLOAT32, label TEXT)";

        // Nested syntax does not add another wrapper to the emitted condition.
        let actual = sql_and_params(ddl, "((n > 10))", &[]);
        let expected = Some((r#"("q"."n" > ?1)"#.to_owned(), vec![Value::I8(10)]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "NOT (enabled AND FALSE)", &[]);
        let expected = Some((
            r#"(NOT ("q"."enabled" AND ?1))"#.to_owned(),
            vec![Value::Bool(false)],
        ));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "NOT (LENGTH(label) > 3)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // Within one SQL condition, both sides of AND or OR must translate.
        let actual = sql_and_params(ddl, "-10 < n AND n <= +12", &[]);
        let expected = Some((
            r#"((?1 < "q"."n") AND ("q"."n" <= ?2))"#.to_owned(),
            vec![Value::I8(-10), Value::I8(12)],
        ));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "enabled OR n >= 10", &[]);
        let expected = Some((
            r#"("q"."enabled" OR ("q"."n" >= ?1))"#.to_owned(),
            vec![Value::I8(10)],
        ));
        assert_eq!(actual, expected);

        for sql in ["LENGTH(label) > 3 OR n > 1", "n > 1 AND LENGTH(label) > 3"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // IS NULL accepts scalar operands without comparison support, such as MAP.
        for (sql, native_sql, params) in [
            ("doc IS NULL", r#"("q"."doc" IS NULL)"#, vec![]),
            ("doc IS NOT NULL", r#"("q"."doc" IS NOT NULL)"#, vec![]),
            ("1 IS NOT NULL", "(?1 IS NOT NULL)", vec![Value::I64(1)]),
        ] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), params));
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = sql_and_params(ddl, "0.1 IS NULL", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        for sql in ["IFNULL($1, f) IS NULL", "IFNULL(f, $1) IS NULL"] {
            let actual = sql_and_params(ddl, sql, &[f32::NAN.into_param_literal()]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // A composite operand of IS NULL must itself translate as a condition.
        let actual = sql_and_params(ddl, "(n > 10) IS NULL", &[]);
        let expected = Some((
            r#"(("q"."n" > ?1) IS NULL)"#.to_owned(),
            vec![Value::I8(10)],
        ));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "LENGTH(label) IS NULL", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // BETWEEN builds low <= target and target <= high, then applies NOT.
        let actual = sql_and_params(ddl, "f BETWEEN 0.1 AND 0.5", &[]);
        let expected = Some((
            r#"((?1 <= "q"."f") AND ("q"."f" <= ?2))"#.to_owned(),
            vec![Value::F32(0.1), Value::F32(0.5)],
        ));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "n NOT BETWEEN 1 AND n", &[]);
        let expected = Some((
            r#"(NOT ((?1 <= "q"."n") AND ("q"."n" <= "q"."n")))"#.to_owned(),
            vec![Value::I8(1)],
        ));
        assert_eq!(actual, expected);

        for sql in ["n BETWEEN 1000 AND 2000", "n BETWEEN 1 AND 1000"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // A bare condition must resolve to BOOLEAN or NULL.
        for (sql, native_sql, params) in [
            ("enabled", r#""q"."enabled""#, vec![]),
            ("TRUE", "?1", vec![Value::Bool(true)]),
            ("NULL", "?1", vec![Value::Null]),
            (
                "IFNULL(enabled, FALSE)",
                r#"IFNULL("q"."enabled", ?1)"#,
                vec![Value::Bool(false)],
            ),
        ] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), params));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["LENGTH(label)", "0.1", "n", "'yes'", "label LIKE 'a%'"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }
    }

    #[test]
    fn build_comparison_sql() {
        let ddl = "
            CREATE TABLE q (
                n INT8, u UINT8, wide INT, f FLOAT32, d FLOAT,
                label TEXT, enabled BOOLEAN, bytes BYTEA, doc MAP
            )
        ";

        // Both sides must be scalar operands before type resolution starts.
        for sql in [
            "-n > 1",
            "n + 1 > 10",
            "CAST(n AS INT) > 1",
            "n > (SELECT 1)",
        ] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // Literals acquire their peer's type in either direction.
        for (sql, native_sql) in [
            ("12 = n", r#"(?1 = "q"."n")"#),
            ("n = 12", r#"("q"."n" = ?1)"#),
        ] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), vec![Value::I8(12)]));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["1000 > n", "n < 1000", "n = 1.0", "n = '1'", "u > -1"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = sql_and_params(ddl, "1 < 2", &[]);
        let expected = Some(("(?1 < ?2)".to_owned(), vec![Value::I64(1), Value::I64(2)]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "'a' = 'b'", &[]);
        let expected = Some((
            "(?1 = ?2)".to_owned(),
            vec![Value::Str("a".to_owned()), Value::Str("b".to_owned())],
        ));
        assert_eq!(actual, expected);

        // Supported operators are emitted with the resolved parameter values.
        for (column, literal, value) in [
            ("n", "12", Value::I8(12)),
            ("label", "'a''b'", Value::Str("a'b".to_owned())),
            ("enabled", "TRUE", Value::Bool(true)),
            ("bytes", "X'00FF'", Value::Bytea(vec![0, 255])),
        ] {
            for op in ["=", "<>", "<", "<=", ">", ">="] {
                let sql = format!("q.{column} {op} {literal}");
                let actual = sql_and_params(ddl, &sql, &[]);
                let expected = Some((format!(r#"("q"."{column}" {op} ?1)"#), vec![value.clone()]));
                assert_eq!(actual, expected, "{sql}");
            }
        }

        for op in ["<", "<=", ">", ">="] {
            for (column, value) in [("f", Value::F32(0.1)), ("d", Value::F64(0.1))] {
                let sql = format!("{column} {op} 0.1");
                let actual = sql_and_params(ddl, &sql, &[]);
                let expected = Some((format!(r#"("q"."{column}" {op} ?1)"#), vec![value]));
                assert_eq!(actual, expected, "{sql}");
            }
        }

        for sql in ["d = 0.1", "f <> 0.1", "doc = doc", "NULL = doc"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // NULL and equal types need no rhs conversion.
        let actual = sql_and_params(ddl, "n = NULL", &[]);
        let expected = Some((r#"("q"."n" = ?1)"#.to_owned(), vec![Value::Null]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "label = label", &[]);
        let expected = Some((r#"("q"."label" = "q"."label")"#.to_owned(), vec![]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "f < $1", &[0.1_f32.into_param_literal()]);
        let expected = Some((r#"("q"."f" < ?1)"#.to_owned(), vec![Value::F32(0.1)]));
        assert_eq!(actual, expected);

        for sql in ["n = TRUE", "f < d", "n < f"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = sql_and_params(ddl, "n = $1", &[1.5_f64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "label = $1", &[1_i64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        // A rhs column must fit the lhs's whole range; operand order matters.
        let actual = sql_and_params(ddl, "$1 > n", &[1000_i64.into_param_literal()]);
        let expected = Some((r#"(?1 > "q"."n")"#.to_owned(), vec![Value::I64(1000)]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "$1 > wide", &[12_i8.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "wide > n", &[]);
        let expected = Some((r#"("q"."wide" > "q"."n")"#.to_owned(), vec![]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "n < wide", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // A known rhs value only needs that value to fit before binding.
        let actual = sql_and_params(ddl, "n < $1", &[12_i64.into_param_literal()]);
        let expected = Some((r#"("q"."n" < ?1)"#.to_owned(), vec![Value::I8(12)]));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "n < $1", &[1000_i64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        // Translation keeps infinity but abandons a comparison when binding fails.
        let actual = sql_and_params(ddl, "d < $1", &[f64::INFINITY.into_param_literal()]);
        let expected = Some((
            r#"("q"."d" < ?1)"#.to_owned(),
            vec![Value::F64(f64::INFINITY)],
        ));
        assert_eq!(actual, expected);

        for (sql, param) in [
            ("$1 < f", f32::NAN.into_param_literal()),
            ("f < $1", f32::NAN.into_param_literal()),
            ("d < $1", f64::NAN.into_param_literal()),
        ] {
            let actual = sql_and_params(ddl, sql, &[param]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }
    }

    #[test]
    fn build_in_list_sql() {
        let ddl = "CREATE TABLE q (n INT8, small INT16, label TEXT, f FLOAT32)";

        let actual = sql_and_params(ddl, "(n + 1) IN (1)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // Empty lists stay on the common path to preserve NULL behavior.
        for negated in [false, true] {
            let expr = ExprPlan::InList {
                expr: Box::new(ExprPlan::Value(Value::Null)),
                list: vec![],
                negated,
            };
            let mut planner = ConditionPlanner::new("q", "q", &[], &[]);
            let actual = planner
                .build_condition_sql(&expr)
                .map(|sql| (sql, planner.params));
            let expected = None;
            assert_eq!(actual, expected);
        }

        // Every item must be a scalar, and target/item resolution must succeed.
        for sql in [
            "n IN (n + 1)",
            "1000 IN (n)",
            "n IN (1, 'two')",
            "n IN (1, 1000)",
        ] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // IN requires equality support on both sides, then rhs conversion.
        for sql in ["f IN (0.1, 0.2)", "n IN (f)", "n IN (small)"] {
            let actual = sql_and_params(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = sql_and_params(ddl, "n IN ($1)", &[12_i64.into_param_literal()]);
        let expected = Some((r#"("q"."n" IN (?1))"#.to_owned(), vec![Value::I8(12)]));
        assert_eq!(actual, expected);

        // The target must resolve to one value/type for the whole list.
        let actual = sql_and_params(ddl, "1 IN (n, small)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "1 IN (n, n)", &[]);
        let expected = Some((
            r#"(?1 IN ("q"."n", "q"."n"))"#.to_owned(),
            vec![Value::I8(1)],
        ));
        assert_eq!(actual, expected);

        // Bind the target once, then items in list order; apply negation last.
        let actual = sql_and_params(ddl, "n IN (1, NULL, n)", &[]);
        let expected = Some((
            r#"("q"."n" IN (?1, ?2, "q"."n"))"#.to_owned(),
            vec![Value::I8(1), Value::Null],
        ));
        assert_eq!(actual, expected);

        let actual = sql_and_params(ddl, "label NOT IN ('KR', 'JP')", &[]);
        let expected = Some((
            r#"("q"."label" NOT IN (?1, ?2))"#.to_owned(),
            vec![Value::Str("KR".to_owned()), Value::Str("JP".to_owned())],
        ));
        assert_eq!(actual, expected);

        let ddl = "CREATE TABLE q (name TEXT, label TEXT)";
        let sql = "IFNULL(name, IFNULL(label, $1)) IN ('서울', '부산')";
        let actual = sql_and_params(ddl, sql, &["미정".into_param_literal()]);
        let expected = Some((
            r#"(IFNULL("q"."name", IFNULL("q"."label", ?1)) IN (?2, ?3))"#.to_owned(),
            vec![
                Value::Str("미정".to_owned()),
                Value::Str("서울".to_owned()),
                Value::Str("부산".to_owned()),
            ],
        ));
        assert_eq!(actual, expected);
    }

    #[test]
    fn render_operand() {
        let schema = Schema::from_ddl(r#"CREATE TABLE "q""1" ("a""b" INT)"#).unwrap();
        let columns = schema.column_defs.unwrap();
        let column = ColumnDefPlan::from(columns[0].clone());
        let mut planner = ConditionPlanner::new(&schema.table_name, "q", &[], &[]);

        // A column is quoted SQL syntax and does not consume a parameter position.
        let actual = planner.render_operand(ResolvedOperand::Column(&column));
        let expected = Some(r#""q""1"."a""b""#.to_owned());
        assert_eq!(actual, expected);
        assert_eq!(planner.params, Vec::<Value>::new());

        for value in [Value::F32(f32::NAN), Value::F64(f64::NAN)] {
            let actual = planner.render_operand(ResolvedOperand::Value(value));
            let expected = None;
            assert_eq!(actual, expected);
            assert_eq!(planner.params, Vec::<Value>::new());
        }

        // Values are appended in order, even when the same value appears again.
        for (value, placeholder) in [
            (Value::I64(10), "?1"),
            (Value::I64(10), "?2"),
            (Value::Null, "?3"),
        ] {
            let actual = planner.render_operand(ResolvedOperand::Value(value));
            let expected = Some(placeholder.to_owned());
            assert_eq!(actual, expected);
        }

        let expected = vec![Value::I64(10), Value::I64(10), Value::Null];
        assert_eq!(planner.params, expected);
    }

    #[test]
    fn render_operand_for_nested_functions() {
        let sql = "COALESCE(NULLIF(7, 3), IFNULL(NULL, 9), 12)";
        let OperandPlan::Resolved(operand) = plan_operand(&expr(sql, &[]), "q", &[], &[]) else {
            panic!("expected a function operand");
        };

        let mut planner = ConditionPlanner::new("q", "q", &[], &[]);
        let actual = planner.render_operand(operand);
        let expected = Some("COALESCE(NULLIF(?1, ?2), IFNULL(?3, ?4), ?5)".to_owned());
        assert_eq!(actual, expected);

        let expected = vec![
            Value::I64(7),
            Value::I64(3),
            Value::Null,
            Value::I64(9),
            Value::I64(12),
        ];
        assert_eq!(planner.params, expected);
    }
}
