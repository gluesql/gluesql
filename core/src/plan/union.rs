use {
    super::PlanError,
    crate::{
        ast::{
            DataType, Expr, Literal, Projection, Select, SelectItem, SetExpr, Statement,
            TableFactor,
        },
        data::{BigDecimalExt, Schema},
        result::Result,
    },
    std::collections::HashMap,
};

type SchemaMap = HashMap<String, Schema>;

/// Validates UNION column type compatibility at plan time for cases where
/// types can be statically determined — literal projections and schema-backed
/// identifier projections.  Complex expressions (arithmetic, functions, …) are
/// left to execute-time validation.
pub fn validate(schema_map: &SchemaMap, statement: &Statement) -> Result<()> {
    let set_expr = match statement {
        Statement::Query(query) => Some(&query.body),
        Statement::Insert { source, .. } => Some(&source.body),
        Statement::CreateTable { source, .. } => source.as_ref().map(|q| &q.body),
        _ => None,
    };

    if let Some(set_expr) = set_expr {
        validate_set_expr(schema_map, set_expr)?;
    }

    Ok(())
}

fn validate_set_expr(schema_map: &SchemaMap, set_expr: &SetExpr) -> Result<()> {
    let SetExpr::Union { left, right, .. } = set_expr else {
        return Ok(());
    };

    // Recurse first so inner UNIONs are checked before the outer one.
    validate_set_expr(schema_map, left)?;
    validate_set_expr(schema_map, right)?;

    let left_types = infer_column_types(schema_map, left);
    let right_types = infer_column_types(schema_map, right);

    if let (Some(left_types), Some(right_types)) = (left_types, right_types) {
        for (index, (lt, rt)) in left_types.iter().zip(right_types.iter()).enumerate() {
            if let (Some(l), Some(r)) = (lt, rt)
                && l != r
            {
                return Err(PlanError::UnionColumnTypeMismatch {
                    index,
                    left: format!("{l}"),
                    right: format!("{r}"),
                }
                .into());
            }
        }
    }

    Ok(())
}

/// Returns `Some(Vec<Option<DataType>>)` when the column types of a set
/// expression can be partially or fully determined at plan time.  Each element
/// is `Some(type)` when the type is known, or `None` when it cannot be
/// inferred statically (e.g. arbitrary expressions).  Returns `None` when no
/// type information is available at all.
fn infer_column_types(schema_map: &SchemaMap, set_expr: &SetExpr) -> Option<Vec<Option<DataType>>> {
    match set_expr {
        SetExpr::Select(select) => infer_select_types(schema_map, select),
        // VALUES types are determined at execute time.
        SetExpr::Values(_) => None,
        // The output type of a UNION equals the left branch's output type.
        SetExpr::Union { left, .. } => infer_column_types(schema_map, left),
    }
}

fn infer_select_types(schema_map: &SchemaMap, select: &Select) -> Option<Vec<Option<DataType>>> {
    let items = match &select.projection {
        Projection::SelectItems(items) => items,
        Projection::SchemalessMap => return None,
    };

    if items.is_empty() {
        return None;
    }

    // Build a (alias-or-name → schema) lookup for the primary FROM relation.
    let table_ctx = match &select.from.relation {
        TableFactor::Table { name, alias, .. } => {
            let key = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
            schema_map.get(name.as_str()).map(|schema| (key, schema))
        }
        _ => None,
    };

    let types = items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, .. } => infer_expr_type(expr, table_ctx),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => None,
        })
        .collect();

    Some(types)
}

fn infer_expr_type(expr: &Expr, table: Option<(&str, &Schema)>) -> Option<DataType> {
    match expr {
        Expr::Literal(lit) => Some(infer_literal_type(lit)),
        Expr::Identifier(col) => {
            let (_, schema) = table?;
            schema
                .column_defs
                .as_ref()?
                .iter()
                .find(|cd| &cd.name == col)
                .map(|cd| cd.data_type.clone())
        }
        Expr::CompoundIdentifier { alias, ident } => {
            let (table_alias, schema) = table?;
            if alias == table_alias {
                schema
                    .column_defs
                    .as_ref()?
                    .iter()
                    .find(|cd| &cd.name == ident)
                    .map(|cd| cd.data_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn infer_literal_type(lit: &Literal) -> DataType {
    match lit {
        Literal::QuotedString(_) => DataType::Text,
        Literal::Number(n) => {
            if n.is_integer_representation() {
                DataType::Int
            } else {
                DataType::Float
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{infer_select_types, validate},
        crate::{
            ast::{Projection, Select, TableFactor, TableWithJoins},
            mock::run,
            plan::{PlanError, fetch_schema_map},
            prelude::{parse, translate},
        },
        futures::executor::block_on,
        std::collections::HashMap,
    };

    fn make_select(projection: Projection) -> Select {
        Select {
            distinct: false,
            projection,
            from: TableWithJoins {
                relation: TableFactor::Series {
                    alias: crate::ast::TableAlias {
                        name: "s".to_owned(),
                        columns: vec![],
                    },
                    size: crate::ast::Expr::Literal(crate::ast::Literal::Number(
                        "1".parse().unwrap(),
                    )),
                },
                joins: vec![],
            },
            selection: None,
            group_by: vec![],
            having: None,
            aggregate_slots: None,
        }
    }

    fn check(storage: &impl crate::store::Store, sql: &str) -> crate::result::Result<()> {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();
        validate(&schema_map, &statement)
    }

    #[test]
    fn schemaless_map_projection_returns_none() {
        let result = infer_select_types(&HashMap::new(), &make_select(Projection::SchemalessMap));
        assert!(
            result.is_none(),
            "SchemalessMap is set by the planner and never appears in the raw AST, so this path can only be exercised by constructing a Select directly",
        );
    }

    #[test]
    fn empty_select_items_returns_none() {
        let result = infer_select_types(
            &HashMap::new(),
            &make_select(Projection::SelectItems(vec![])),
        );
        assert!(
            result.is_none(),
            "an empty SelectItems vec is rejected by the parser, so it must be constructed manually to cover the guard branch",
        );
    }

    #[test]
    fn literal_type_mismatch_is_rejected() {
        let storage = run("");

        assert_eq!(
            check(&storage, "SELECT 1, 'a' UNION SELECT 2, 3"),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 1,
                left: "TEXT".to_owned(),
                right: "INT".to_owned(),
            }
            .into()),
        );
    }

    #[test]
    fn literal_type_match_is_accepted() {
        let storage = run("");
        assert!(check(&storage, "SELECT 1, 2 UNION SELECT 3, 4").is_ok());
    }

    #[test]
    fn schema_backed_type_mismatch_is_rejected() {
        let storage = run("CREATE TABLE T (id INTEGER, name TEXT);
             CREATE TABLE S (id INTEGER, age INTEGER);");

        assert_eq!(
            check(
                &storage,
                "SELECT id, name FROM T UNION SELECT id, age FROM S"
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 1,
                left: "TEXT".to_owned(),
                right: "INT".to_owned(),
            }
            .into()),
        );
    }

    #[test]
    fn schema_backed_type_match_is_accepted() {
        let storage = run("CREATE TABLE T (id INTEGER, name TEXT);
             CREATE TABLE S (id INTEGER, label TEXT);");
        assert!(
            check(
                &storage,
                "SELECT id, name FROM T UNION SELECT id, label FROM S"
            )
            .is_ok()
        );
    }

    #[test]
    fn unknown_expression_type_is_skipped() {
        let storage = run("");
        assert!(
            check(&storage, "SELECT 1 + 1 UNION SELECT 'a'").is_ok(),
            "complex expression type is not statically known, so plan-time validation should skip it",
        );
    }

    #[test]
    fn float_literal_mismatch_is_rejected() {
        let storage = run("");
        assert_eq!(
            check(&storage, "SELECT 1.5 UNION SELECT 'a'"),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 0,
                left: "FLOAT".to_owned(),
                right: "TEXT".to_owned(),
            }
            .into()),
        );
    }

    #[test]
    fn compound_identifier_matching_alias_mismatch_is_rejected() {
        let storage = run("CREATE TABLE T (id INTEGER, label TEXT);
             CREATE TABLE S (id INTEGER, code INTEGER);");
        assert_eq!(
            check(
                &storage,
                "SELECT t.id, t.label FROM T AS t UNION SELECT s.id, s.code FROM S AS s",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 1,
                left: "TEXT".to_owned(),
                right: "INT".to_owned(),
            }
            .into()),
        );
    }

    #[test]
    fn compound_identifier_non_matching_alias_is_skipped() {
        let storage = run("CREATE TABLE T (id INTEGER, label TEXT);
             CREATE TABLE S (id INTEGER, code INTEGER);");
        assert!(
            check(
                &storage,
                "SELECT x.id, x.label FROM T UNION SELECT y.id, y.code FROM S",
            )
            .is_ok(),
            "aliases 'x' / 'y' don't match the table names so types cannot be inferred and the check is skipped",
        );
    }

    #[test]
    fn wildcard_projection_is_skipped() {
        let storage = run("CREATE TABLE T (id INTEGER);
             CREATE TABLE S (id TEXT);");
        assert!(
            check(&storage, "SELECT * FROM T UNION SELECT * FROM S").is_ok(),
            "wildcards expand at execute time, so plan-time check is skipped",
        );
    }

    #[test]
    fn values_right_side_is_skipped() {
        let storage = run("");
        assert!(
            check(&storage, "SELECT 1, 'a' UNION VALUES (2, 3)").is_ok(),
            "VALUES types are determined at execute time, so plan-time check is skipped",
        );
    }

    #[test]
    fn nested_union_type_inferred_from_left_branch() {
        let storage = run("");
        assert_eq!(
            check(&storage, "SELECT 1 UNION SELECT 2 UNION SELECT 'a'"),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 0,
                left: "INT".to_owned(),
                right: "TEXT".to_owned(),
            }
            .into()),
            "parsed as (SELECT 1 UNION SELECT 2) UNION SELECT 'a'; the outer left's type is inferred from its own left branch (INT)",
        );
    }
}
