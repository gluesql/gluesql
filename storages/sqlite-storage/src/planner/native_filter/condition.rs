//! Extracts supported WHERE conditions into native SQL, bindings, and a remaining expression.

mod operand_type;

use {
    self::operand_type::{Operand, ResolvedOperand},
    crate::schema::quote,
    gluesql_core::{
        ast::{BinaryOperator, DataType, Literal, ToSql, UnaryOperator},
        data::Value,
        plan::{ColumnDefPlan, ExprPlan},
    },
};

#[derive(Debug, PartialEq)]
pub(super) struct PlannedCondition {
    pub(super) sql: String,
    pub(super) params: Vec<Value>,
    pub(super) remaining: Option<ExprPlan>,
}

pub(super) fn plan(
    expr: &ExprPlan,
    alias: &str,
    columns: &[ColumnDefPlan],
    column_aliases: &[String],
) -> Option<PlannedCondition> {
    let mut planner = ConditionPlanner::new(alias, columns, column_aliases);
    let remaining = planner.extract(expr);

    if planner.sql_conditions.is_empty() {
        return None;
    }

    Some(PlannedCondition {
        sql: planner.sql_conditions.join(" AND "),
        params: planner.params,
        remaining,
    })
}

struct ConditionPlanner<'a> {
    alias: &'a str,
    columns: &'a [ColumnDefPlan],
    aliases: &'a [String],
    sql_conditions: Vec<String>,
    params: Vec<Value>,
}

impl<'a> ConditionPlanner<'a> {
    fn new(alias: &'a str, columns: &'a [ColumnDefPlan], aliases: &'a [String]) -> Self {
        Self {
            alias,
            columns,
            aliases,
            sql_conditions: Vec::new(),
            params: Vec::new(),
        }
    }

    fn extract(&mut self, expr: &ExprPlan) -> Option<ExprPlan> {
        match expr {
            ExprPlan::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = self.extract(left);
                let right = self.extract(right);

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
                .extract(expr)
                .map(|remaining| ExprPlan::Nested(Box::new(remaining))),
            // OR/NOT and other expressions move as a whole. Extracting an AND from
            // inside one of them could discard rows that satisfy the original condition.
            _ => {
                let param_count = self.params.len();
                let Some(sql) = self.translate(expr) else {
                    // Translation may fail after binding a child; discard those partial bindings.
                    self.params.truncate(param_count);
                    return Some(expr.clone());
                };

                self.sql_conditions.push(sql);
                None
            }
        }
    }

    fn translate(&mut self, expr: &ExprPlan) -> Option<String> {
        match expr {
            // Composite SQL already includes the parentheses needed for precedence.
            ExprPlan::Nested(expr) => self.translate(expr),
            ExprPlan::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Some(format!("(NOT {})", self.translate(expr)?)),
            ExprPlan::BinaryOp {
                left,
                op: op @ (BinaryOperator::And | BinaryOperator::Or),
                right,
            } => {
                let left = self.translate(left)?;
                let right = self.translate(right)?;

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
            } => self.comparison(left, op, right),
            ExprPlan::IsNull(inner) | ExprPlan::IsNotNull(inner) => {
                // Null checks also work on types whose comparisons cannot be delegated.
                let sql = if let Some(operand) = self.operand_from_expr(inner) {
                    self.bind(operand.resolve(None)?)?
                } else {
                    self.translate(inner)?
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
            } => self.in_list(expr, list, *negated),
            ExprPlan::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // Keep the common evaluator's operand direction: low <= target and
                // target <= high. Typed-value conversions can depend on the lhs type.
                let low = self.comparison(low, &BinaryOperator::LtEq, expr)?;
                let high = self.comparison(expr, &BinaryOperator::LtEq, high)?;

                let between = format!("({low} AND {high})");
                Some(if *negated {
                    format!("(NOT {between})")
                } else {
                    between
                })
            }
            _ => {
                // SQLite accepts numbers/text as truthy; GlueSQL requires BOOLEAN or NULL.
                let operand = self.operand_from_expr(expr)?.resolve(None)?;
                if operand
                    .data_type()
                    .is_some_and(|data_type| data_type != DataType::Boolean)
                {
                    return None;
                }

                self.bind(operand)
            }
        }
    }

    fn comparison(
        &mut self,
        left: &ExprPlan,
        op: &BinaryOperator,
        right: &ExprPlan,
    ) -> Option<String> {
        let left = self.operand_from_expr(left)?;
        let right = self.operand_from_expr(right)?;

        let left = left.resolve(right.data_type().as_ref())?;
        let right = right.resolve(left.data_type().as_ref())?;

        if !left.supports_comparison(op) || !right.supports_comparison(op) {
            return None;
        }

        let right = left.coerce_rhs(right)?;

        let left = self.bind(left)?;
        let right = self.bind(right)?;

        Some(format!("({left} {} {right})", op.to_sql()))
    }

    fn in_list(&mut self, expr: &ExprPlan, list: &[ExprPlan], negated: bool) -> Option<String> {
        let target = self.operand_from_expr(expr)?;
        let mut resolved_target = None;
        let mut items = Vec::new();

        for item in list {
            let item = self.operand_from_expr(item)?;
            let target = target.clone().resolve(item.data_type().as_ref())?;
            let item = item.resolve(target.data_type().as_ref())?;

            if !target.supports_comparison(&BinaryOperator::Eq)
                || !item.supports_comparison(&BinaryOperator::Eq)
            {
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

        let target = self.bind(resolved_target?)?;
        let items = items
            .into_iter()
            .map(|item| self.bind(item))
            .collect::<Option<Vec<_>>>()?;

        let op = if negated { "NOT IN" } else { "IN" };

        Some(format!("({target} {op} ({}))", items.join(", ")))
    }

    fn operand_from_expr(&self, expr: &ExprPlan) -> Option<Operand<'a>> {
        match expr {
            ExprPlan::Nested(expr) => self.operand_from_expr(expr),
            // References to outer rows cannot be bound by this native query.
            ExprPlan::Identifier(name) => self.column(name).map(Operand::Column),
            ExprPlan::CompoundIdentifier { alias, ident } if alias == self.alias => {
                self.column(ident).map(Operand::Column)
            }
            ExprPlan::Literal(literal) => Some(Operand::Literal(literal.clone())),
            ExprPlan::Value(value) => Some(Operand::Value(value.clone())),
            ExprPlan::UnaryOp {
                op: op @ (UnaryOperator::Plus | UnaryOperator::Minus),
                expr,
            } => {
                let Operand::Literal(Literal::Number(number)) = self.operand_from_expr(expr)?
                else {
                    return None;
                };

                let number = if matches!(op, UnaryOperator::Minus) {
                    -number
                } else {
                    number
                };
                Some(Operand::Literal(Literal::Number(number)))
            }
            _ => None,
        }
    }

    fn column(&self, name: &str) -> Option<&'a ColumnDefPlan> {
        // Resolve logical aliases while retaining the physical name for SQL.
        self.columns.iter().enumerate().find_map(|(index, column)| {
            (self.aliases.get(index).unwrap_or(&column.name) == name).then_some(column)
        })
    }

    fn bind(&mut self, operand: ResolvedOperand<'_>) -> Option<String> {
        match operand {
            ResolvedOperand::Column(column) => Some(quote(&column.name)),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            ConditionPlanner, Operand, PlannedCondition, ResolvedOperand, plan as plan_condition,
        },
        gluesql_core::{
            ast::Literal,
            data::{Schema, Value},
            parse_sql::parse_expr,
            plan::{ColumnDefPlan, ExprPlan},
            translate::{IntoParamLiteral, ParamLiteral, translate_expr},
        },
    };

    fn expr(sql: &str, params: &[ParamLiteral]) -> ExprPlan {
        translate_expr(&parse_expr(sql).expect(sql), params)
            .expect(sql)
            .into()
    }

    fn plan(ddl: &str, sql: &str) -> Option<PlannedCondition> {
        let schema = Schema::from_ddl(ddl).unwrap();
        let columns = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect::<Vec<_>>();

        plan_condition(&expr(sql, &[]), &schema.table_name, &columns, &[])
    }

    fn translate_condition(
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
        let mut planner = ConditionPlanner::new(&schema.table_name, &columns, &[]);
        let sql = planner.translate(&expr(sql, params))?;
        Some((sql, planner.params))
    }

    #[test]
    fn extract() {
        let ddl = "CREATE TABLE Item (id INT, qty INT)";

        // AND: both sides remain, with no native conditions.
        let actual = plan(ddl, "qty + 1 > 2 AND id + 1 < 10");
        let expected = None;
        assert_eq!(actual, expected);

        // AND: rebuild both residual sides after extracting their native terms.
        let ddl = "CREATE TABLE Item (id INT, name TEXT, qty INT)";
        let sql = "
            (qty > 10 AND LOWER(name) = 'apple')
            AND (LENGTH(name) > 3 AND id < 20)
        ";
        let actual = plan(ddl, sql);
        let expected = Some(PlannedCondition {
            sql: r#"("qty" > ?1) AND ("id" < ?2)"#.to_owned(),
            params: vec![Value::I64(10), Value::I64(20)],
            remaining: Some(expr("(LOWER(name) = 'apple') AND (LENGTH(name) > 3)", &[])),
        });
        assert_eq!(actual, expected);

        // AND: retain only the left or right residual.
        for sql in [
            "LOWER(name) = 'apple' AND qty > 10",
            "qty > 10 AND LOWER(name) = 'apple'",
        ] {
            let actual = plan(ddl, sql);
            let expected = Some(PlannedCondition {
                sql: r#"("qty" > ?1)"#.to_owned(),
                params: vec![Value::I64(10)],
                remaining: Some(expr("LOWER(name) = 'apple'", &[])),
            });
            assert_eq!(actual, expected, "{sql}");
        }

        // AND and Nested: wrappers disappear when no residual remains.
        for sql in ["qty > 10 AND id < 20", "((qty > 10 AND id < 20))"] {
            let actual = plan(ddl, sql);
            let expected = Some(PlannedCondition {
                sql: r#"("qty" > ?1) AND ("id" < ?2)"#.to_owned(),
                params: vec![Value::I64(10), Value::I64(20)],
                remaining: None,
            });
            assert_eq!(actual, expected, "{sql}");
        }

        // Nested preserves the wrapper around the remaining expression.
        let actual = plan(ddl, "(qty > 10 AND LOWER(name) = 'apple')");
        let expected = Some(PlannedCondition {
            sql: r#"("qty" > ?1)"#.to_owned(),
            params: vec![Value::I64(10)],
            remaining: Some(expr("(LOWER(name) = 'apple')", &[])),
        });
        assert_eq!(actual, expected);

        // Other expressions: a supported OR/NOT group moves as a whole.
        let sql = "
            (qty > 10 OR qty < 0)
            AND NOT (id = 5)
        ";
        let actual = plan(ddl, sql);
        let expected = Some(PlannedCondition {
            sql: r#"(("qty" > ?1) OR ("qty" < ?2)) AND (NOT ("id" = ?3))"#.to_owned(),
            params: vec![Value::I64(10), Value::I64(0), Value::I64(5)],
            remaining: None,
        });
        assert_eq!(actual, expected);

        // A failed OR/NOT group remains whole, including any supported inner terms.
        for sql in [
            "qty > 10 OR LOWER(name) = 'apple'",
            "NOT (qty > 10 AND LOWER(name) = 'apple')",
        ] {
            let actual = plan(ddl, sql);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // A failed group does not consume parameter positions for later terms.
        let sql = "
            qty > 10
            AND (id = 99 OR LOWER(name) = 'apple')
            AND id < 20
        ";
        let actual = plan(ddl, sql);
        let expected = Some(PlannedCondition {
            sql: r#"("qty" > ?1) AND ("id" < ?2)"#.to_owned(),
            params: vec![Value::I64(10), Value::I64(20)],
            remaining: Some(expr("(id = 99 OR LOWER(name) = 'apple')", &[])),
        });
        assert_eq!(actual, expected);
    }

    #[test]
    fn translate() {
        let ddl = "CREATE TABLE q (n INT8, enabled BOOLEAN, doc MAP, f FLOAT32, label TEXT)";

        // Nested syntax does not add another wrapper to the emitted condition.
        let actual = translate_condition(ddl, "((n > 10))", &[]);
        let expected = Some((r#"("n" > ?1)"#.to_owned(), vec![Value::I8(10)]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "NOT (enabled AND FALSE)", &[]);
        let expected = Some((
            r#"(NOT ("enabled" AND ?1))"#.to_owned(),
            vec![Value::Bool(false)],
        ));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "NOT (LENGTH(label) > 3)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // AND/OR translate both children; splitting AND terms belongs to extract.
        let actual = translate_condition(ddl, "-10 < n AND n <= +12", &[]);
        let expected = Some((
            r#"((?1 < "n") AND ("n" <= ?2))"#.to_owned(),
            vec![Value::I8(-10), Value::I8(12)],
        ));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "enabled OR n >= 10", &[]);
        let expected = Some((
            r#"("enabled" OR ("n" >= ?1))"#.to_owned(),
            vec![Value::I8(10)],
        ));
        assert_eq!(actual, expected);

        for sql in ["LENGTH(label) > 3 OR n > 1", "n > 1 AND LENGTH(label) > 3"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // IS NULL accepts scalar operands without comparison support, such as MAP.
        for (sql, native_sql, params) in [
            ("doc IS NULL", r#"("doc" IS NULL)"#, vec![]),
            ("doc IS NOT NULL", r#"("doc" IS NOT NULL)"#, vec![]),
            ("1 IS NOT NULL", "(?1 IS NOT NULL)", vec![Value::I64(1)]),
        ] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), params));
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = translate_condition(ddl, "0.1 IS NULL", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // A composite operand of IS NULL must itself translate as a condition.
        let actual = translate_condition(ddl, "(n > 10) IS NULL", &[]);
        let expected = Some((r#"(("n" > ?1) IS NULL)"#.to_owned(), vec![Value::I8(10)]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "LENGTH(label) IS NULL", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // BETWEEN builds low <= target and target <= high, then applies NOT.
        let actual = translate_condition(ddl, "f BETWEEN 0.1 AND 0.5", &[]);
        let expected = Some((
            r#"((?1 <= "f") AND ("f" <= ?2))"#.to_owned(),
            vec![Value::F32(0.1), Value::F32(0.5)],
        ));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "n NOT BETWEEN 1 AND n", &[]);
        let expected = Some((
            r#"(NOT ((?1 <= "n") AND ("n" <= "n")))"#.to_owned(),
            vec![Value::I8(1)],
        ));
        assert_eq!(actual, expected);

        for sql in ["n BETWEEN 1000 AND 2000", "n BETWEEN 1 AND 1000"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // A bare condition must resolve to BOOLEAN or NULL.
        for (sql, native_sql, params) in [
            ("enabled", r#""enabled""#, vec![]),
            ("TRUE", "?1", vec![Value::Bool(true)]),
            ("NULL", "?1", vec![Value::Null]),
        ] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), params));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["LENGTH(label)", "0.1", "n", "'yes'", "label LIKE 'a%'"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }
    }

    #[test]
    fn comparison() {
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
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // Literals acquire their peer's type in either direction.
        for (sql, native_sql) in [("12 = n", r#"(?1 = "n")"#), ("n = 12", r#"("n" = ?1)"#)] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = Some((native_sql.to_owned(), vec![Value::I8(12)]));
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["1000 > n", "n < 1000", "n = 1.0", "n = '1'", "u > -1"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = translate_condition(ddl, "1 < 2", &[]);
        let expected = Some(("(?1 < ?2)".to_owned(), vec![Value::I64(1), Value::I64(2)]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "'a' = 'b'", &[]);
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
                let actual = translate_condition(ddl, &sql, &[]);
                let expected = Some((format!(r#"("{column}" {op} ?1)"#), vec![value.clone()]));
                assert_eq!(actual, expected, "{sql}");
            }
        }

        for op in ["<", "<=", ">", ">="] {
            for (column, value) in [("f", Value::F32(0.1)), ("d", Value::F64(0.1))] {
                let sql = format!("{column} {op} 0.1");
                let actual = translate_condition(ddl, &sql, &[]);
                let expected = Some((format!(r#"("{column}" {op} ?1)"#), vec![value]));
                assert_eq!(actual, expected, "{sql}");
            }
        }

        for sql in ["d = 0.1", "f <> 0.1", "doc = doc", "NULL = doc"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // NULL and equal types need no rhs conversion.
        let actual = translate_condition(ddl, "n = NULL", &[]);
        let expected = Some((r#"("n" = ?1)"#.to_owned(), vec![Value::Null]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "label = label", &[]);
        let expected = Some((r#"("label" = "label")"#.to_owned(), vec![]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "f < $1", &[0.1_f32.into_param_literal()]);
        let expected = Some((r#"("f" < ?1)"#.to_owned(), vec![Value::F32(0.1)]));
        assert_eq!(actual, expected);

        for sql in ["n = TRUE", "f < d", "n < f"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = translate_condition(ddl, "n = $1", &[1.5_f64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "label = $1", &[1_i64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        // A rhs column must fit the lhs's whole range; operand order matters.
        let actual = translate_condition(ddl, "$1 > n", &[1000_i64.into_param_literal()]);
        let expected = Some((r#"(?1 > "n")"#.to_owned(), vec![Value::I64(1000)]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "$1 > wide", &[12_i8.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "wide > n", &[]);
        let expected = Some((r#"("wide" > "n")"#.to_owned(), vec![]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "n < wide", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // A known rhs value only needs that value to fit before binding.
        let actual = translate_condition(ddl, "n < $1", &[12_i64.into_param_literal()]);
        let expected = Some((r#"("n" < ?1)"#.to_owned(), vec![Value::I8(12)]));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "n < $1", &[1000_i64.into_param_literal()]);
        let expected = None;
        assert_eq!(actual, expected);
    }

    #[test]
    fn in_list() {
        let ddl = "CREATE TABLE q (n INT8, small INT16, label TEXT, f FLOAT32)";

        let actual = translate_condition(ddl, "(n + 1) IN (1)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        // Empty lists stay on the common path to preserve NULL behavior.
        for negated in [false, true] {
            let expr = ExprPlan::InList {
                expr: Box::new(ExprPlan::Value(Value::Null)),
                list: vec![],
                negated,
            };
            let mut planner = ConditionPlanner::new("q", &[], &[]);
            let actual = planner.translate(&expr).map(|sql| (sql, planner.params));
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
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        // IN requires equality support on both sides, then rhs conversion.
        for sql in ["f IN (0.1, 0.2)", "n IN (f)", "n IN (small)"] {
            let actual = translate_condition(ddl, sql, &[]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = translate_condition(ddl, "n IN ($1)", &[12_i64.into_param_literal()]);
        let expected = Some((r#"("n" IN (?1))"#.to_owned(), vec![Value::I8(12)]));
        assert_eq!(actual, expected);

        // The target must resolve to one value/type for the whole list.
        let actual = translate_condition(ddl, "1 IN (n, small)", &[]);
        let expected = None;
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "1 IN (n, n)", &[]);
        let expected = Some((r#"(?1 IN ("n", "n"))"#.to_owned(), vec![Value::I8(1)]));
        assert_eq!(actual, expected);

        // Bind the target once, then items in list order; apply negation last.
        let actual = translate_condition(ddl, "n IN (1, NULL, n)", &[]);
        let expected = Some((
            r#"("n" IN (?1, ?2, "n"))"#.to_owned(),
            vec![Value::I8(1), Value::Null],
        ));
        assert_eq!(actual, expected);

        let actual = translate_condition(ddl, "label NOT IN ('KR', 'JP')", &[]);
        let expected = Some((
            r#"("label" NOT IN (?1, ?2))"#.to_owned(),
            vec![Value::Str("KR".to_owned()), Value::Str("JP".to_owned())],
        ));
        assert_eq!(actual, expected);
    }

    #[test]
    fn operand_from_expr() {
        let schema = Schema::from_ddl("CREATE TABLE Item (n INT8, label TEXT)").unwrap();
        let columns = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect::<Vec<_>>();
        let planner = ConditionPlanner::new("q", &columns, &[]);

        // Nested, unqualified and source-qualified names resolve to the same column.
        for sql in ["((n))", "n", "q.n"] {
            let Some(Operand::Column(actual)) = planner.operand_from_expr(&expr(sql, &[])) else {
                panic!("expected a column: {sql}");
            };
            let expected = &columns[0];
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["unknown", "q.unknown", "outer_row.n"] {
            let actual = planner.operand_from_expr(&expr(sql, &[]));
            assert!(actual.is_none(), "{sql}");
        }

        // A positional alias replaces the visible name, not the physical column.
        let aliases = ["renamed".to_owned()];
        let planner = ConditionPlanner::new("q", &columns, &aliases);
        for (sql, expected) in [
            ("renamed", &columns[0]),
            ("q.renamed", &columns[0]),
            ("label", &columns[1]),
        ] {
            let Some(Operand::Column(actual)) = planner.operand_from_expr(&expr(sql, &[])) else {
                panic!("expected a column: {sql}");
            };
            assert_eq!(actual, expected, "{sql}");
        }

        let actual = planner.operand_from_expr(&expr("n", &[]));
        assert!(actual.is_none());

        // Literals remain syntax until a peer type is available.
        for (sql, expected) in [
            ("12", Literal::Number(12.into())),
            ("'apple'", Literal::QuotedString("apple".to_owned())),
        ] {
            let Some(Operand::Literal(actual)) = planner.operand_from_expr(&expr(sql, &[])) else {
                panic!("expected a literal: {sql}");
            };
            assert_eq!(actual, expected, "{sql}");
        }

        let params = [12_i64.into_param_literal()];
        let Some(Operand::Value(actual)) = planner.operand_from_expr(&expr("$1", &params)) else {
            panic!("expected a typed value");
        };
        let expected = Value::I64(12);
        assert_eq!(actual, expected);

        // Unary signs normalize numeric literals only.
        for (sql, expected) in [("+12", 12), ("-12", -12)] {
            let Some(Operand::Literal(actual)) = planner.operand_from_expr(&expr(sql, &[])) else {
                panic!("expected a numeric literal: {sql}");
            };
            let expected = Literal::Number(expected.into());
            assert_eq!(actual, expected, "{sql}");
        }

        for sql in ["-renamed", "-$1", "-'apple'", "-unknown"] {
            let actual = planner.operand_from_expr(&expr(sql, &params));
            assert!(actual.is_none(), "{sql}");
        }

        for sql in [
            "renamed + 1",
            "LENGTH(label)",
            "CAST(renamed AS INT)",
            "(SELECT 1)",
        ] {
            let actual = planner.operand_from_expr(&expr(sql, &[]));
            assert!(actual.is_none(), "{sql}");
        }
    }

    #[test]
    fn bind() {
        let schema = Schema::from_ddl(r#"CREATE TABLE q ("a""b" INT)"#).unwrap();
        let columns = schema.column_defs.unwrap();
        let column = ColumnDefPlan::from(columns[0].clone());
        let mut planner = ConditionPlanner::new("q", &[], &[]);

        // A column is quoted SQL syntax and does not consume a parameter position.
        let actual = planner.bind(ResolvedOperand::Column(&column));
        let expected = Some(r#""a""b""#.to_owned());
        assert_eq!(actual, expected);
        assert_eq!(planner.params, Vec::<Value>::new());

        for value in [Value::F32(f32::NAN), Value::F64(f64::NAN)] {
            let actual = planner.bind(ResolvedOperand::Value(value));
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
            let actual = planner.bind(ResolvedOperand::Value(value));
            let expected = Some(placeholder.to_owned());
            assert_eq!(actual, expected);
        }

        let expected = vec![Value::I64(10), Value::I64(10), Value::Null];
        assert_eq!(planner.params, expected);

        // Translation keeps infinity but abandons a comparison when binding fails.
        let ddl = "CREATE TABLE q (f FLOAT32, d FLOAT)";
        let actual = translate_condition(ddl, "d < $1", &[f64::INFINITY.into_param_literal()]);
        let expected = Some((r#"("d" < ?1)"#.to_owned(), vec![Value::F64(f64::INFINITY)]));
        assert_eq!(actual, expected);

        for (sql, param) in [
            ("$1 < f", f32::NAN.into_param_literal()),
            ("f < $1", f32::NAN.into_param_literal()),
            ("d < $1", f64::NAN.into_param_literal()),
        ] {
            let actual = translate_condition(ddl, sql, &[param]);
            let expected = None;
            assert_eq!(actual, expected, "{sql}");
        }
    }
}
