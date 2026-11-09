use {
    super::PlannerError,
    crate::{
        ast::{Expr, Function},
        plan::{ColumnDefPlan, SchemaMapPlan, StatementPlan, StatementPlanKind},
        result::Result,
    },
};

pub fn validate(statement: &StatementPlan) -> Result<()> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(PlannerError::UnplannedSchemaMap.into());
    };

    if let StatementPlanKind::CreateIndex {
        table_name, column, ..
    } = &statement.kind
    {
        let schema = schema_map
            .get(table_name)
            .ok_or_else(|| PlannerError::CreateIndexTableNotFound(table_name.clone()))?;
        let columns = schema.column_defs.as_deref().unwrap_or_default();

        let has_ident = validate_index_expr(columns, &column.expr)?;
        if !has_ident {
            return Err(PlannerError::IndexExprRequiresColumnReference.into());
        }
    }

    Ok(())
}

fn validate_index_expr(columns: &[ColumnDefPlan], expr: &Expr) -> Result<bool> {
    let validate = |expr| validate_index_expr(columns, expr);

    match expr {
        Expr::Identifier(ident) => {
            if columns.iter().any(|column| column.name == *ident) {
                Ok(true)
            } else {
                Err(PlannerError::CreateIndexColumnNotFound(ident.to_owned()).into())
            }
        }
        Expr::Literal(_) | Expr::TypedString { .. } => Ok(false),
        Expr::Nested(expr) | Expr::UnaryOp { expr, .. } => validate(expr),
        Expr::BinaryOp { left, right, .. } => {
            let has_ident_l = validate(left)?;
            let has_ident_r = validate(right)?;

            Ok(has_ident_l || has_ident_r)
        }
        Expr::Function(func) => match func.as_ref() {
            Function::Cast { expr, .. } => validate(expr),
            _ => Err(PlannerError::UnsupportedIndexExpr.into()),
        },
        _ => Err(PlannerError::UnsupportedIndexExpr.into()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{validate, validate_index_expr},
        crate::{
            ast::{ColumnDef, Expr},
            data::Value,
            mock::run,
            parse_sql::{parse, parse_expr},
            plan::StatementPlan,
            planner::{PlannerError, plan_schema_map},
            query_builder::ColumnDefNode,
            result::Error,
            store::Planner,
            translate::{NO_PARAMS, translate, translate_expr},
        },
    };

    fn statement(sql: &str) -> StatementPlan {
        translate(&parse(sql).expect(sql)[0]).expect(sql).into()
    }

    #[test]
    fn unplanned_schema_map() {
        let input = statement("CREATE INDEX idx ON Item (id)");

        let actual = validate(&input);
        let expected = Err(PlannerError::UnplannedSchemaMap.into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn create_index() {
        let storage = run("
            CREATE TABLE Item (id INTEGER);
            CREATE TABLE Documents;
        ");
        let test = |sql: &str, expected, name: &str| {
            let statement = statement(sql);
            let statement = plan_schema_map(&storage, statement).expect(sql);
            let actual = validate(&statement);
            assert_eq!(actual, expected, "{name}: {sql}");
        };

        let actual = "CREATE INDEX idx ON Item (id)";
        let expected = Ok(());
        test(actual, expected, "valid column reference");

        let actual = "CREATE INDEX idx ON Missing (ABS(id))";
        let expected = Err(Error::Planner(PlannerError::CreateIndexTableNotFound(
            "Missing".to_owned(),
        )));
        test(
            actual,
            expected,
            "missing table precedes invalid expression",
        );

        let actual = "CREATE INDEX idx ON Item (missing)";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "missing column");

        let actual = "CREATE INDEX idx ON Item (ABS(id))";
        let expected = Err(Error::Planner(PlannerError::UnsupportedIndexExpr));
        test(
            actual,
            expected,
            "invalid expression precedes missing reference",
        );

        let actual = "CREATE INDEX idx ON Item (100)";
        let expected = Err(Error::Planner(
            PlannerError::IndexExprRequiresColumnReference,
        ));
        test(actual, expected, "constant requires a column reference");

        let actual = "CREATE INDEX idx ON Documents (id)";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "id".to_owned(),
        )));
        test(actual, expected, "schemaless table has no declared columns");

        let actual = "CREATE INDEX idx ON Documents (100)";
        let expected = Err(Error::Planner(
            PlannerError::IndexExprRequiresColumnReference,
        ));
        test(
            actual,
            expected,
            "schemaless constant still requires a reference",
        );

        let actual = "DROP INDEX Missing.idx";
        let expected = Ok(());
        test(actual, expected, "other statements do not require a schema");
    }

    #[test]
    fn index_expr() {
        let columns = [ColumnDef::try_from(ColumnDefNode::from("id INTEGER"))
            .unwrap()
            .into()];
        let test = |sql: &str, expected, name: &str| {
            let expr = translate_expr(&parse_expr(sql).expect(sql), NO_PARAMS).expect(sql);
            let actual = validate_index_expr(&columns, &expr);
            assert_eq!(actual, expected, "{name}: {sql}");
        };

        let actual = "id";
        let expected = Ok(true);
        test(actual, expected, "existing identifier");

        let actual = "missing";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "missing identifier");

        let actual = "1";
        let expected = Ok(false);
        test(actual, expected, "literal");

        let actual = "DATE '2026-11-04'";
        let expected = Ok(false);
        test(actual, expected, "typed string");

        let actual = "(id)";
        let expected = Ok(true);
        test(actual, expected, "nested reference");

        let actual = "(missing)";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "nested invalid reference");

        let actual = "-id";
        let expected = Ok(true);
        test(actual, expected, "unary reference");

        let actual = "-missing";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "unary invalid reference");

        let actual = "id + 1";
        let expected = Ok(true);
        test(actual, expected, "binary reference on the left");

        let actual = "1 + id";
        let expected = Ok(true);
        test(actual, expected, "binary reference on the right");

        let actual = "1 + 2";
        let expected = Ok(false);
        test(actual, expected, "binary constants");

        let actual = "missing + id";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "invalid left operand");

        let actual = "id + missing";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "invalid right operand");

        let actual = "missing + ABS(id)";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "left missing column is reported first");

        let actual = "ABS(id) + missing";
        let expected = Err(Error::Planner(PlannerError::UnsupportedIndexExpr));
        test(
            actual,
            expected,
            "left unsupported expression is reported first",
        );

        let actual = "CAST(id AS TEXT)";
        let expected = Ok(true);
        test(actual, expected, "cast reference");

        let actual = "CAST(missing AS TEXT)";
        let expected = Err(Error::Planner(PlannerError::CreateIndexColumnNotFound(
            "missing".to_owned(),
        )));
        test(actual, expected, "cast invalid reference");

        let actual = "ABS(id)";
        let expected = Err(Error::Planner(PlannerError::UnsupportedIndexExpr));
        test(actual, expected, "non-cast function");

        let actual = "Item.id";
        let expected = Err(Error::Planner(PlannerError::UnsupportedIndexExpr));
        test(actual, expected, "unsupported expression");

        let actual = validate_index_expr(&columns, &Expr::Value(Value::I64(1)));
        let expected = Err(Error::Planner(PlannerError::UnsupportedIndexExpr));
        assert_eq!(actual, expected, "Value is not an index literal");
    }

    #[test]
    fn planner_chain() {
        let storage = run("CREATE TABLE Item (id INTEGER)");
        let sql = "CREATE INDEX idx ON Item (100)";
        let actual = storage.plan(statement(sql));
        let expected = Err(Error::Planner(
            PlannerError::IndexExprRequiresColumnReference,
        ));

        assert_eq!(
            actual, expected,
            "planner validates before execution: {sql}"
        );
    }
}
