use {
    super::{
        PlanError,
        expr::try_visit_expr,
        statement::{
            ExprPlan, ProjectionPlan, SelectItemPlan, SelectPlan, SetExprPlan, StatementPlan,
            TableFactorPlan,
        },
    },
    crate::{
        ast::{DataType, Literal},
        data::{BigDecimalExt, Schema},
        result::Result,
    },
    std::collections::HashMap,
};

type SchemaMap = HashMap<String, Schema>;
type ValidateResult = std::result::Result<(), PlanError>;

/// Validates UNION column type compatibility at plan time for cases where
/// types can be statically determined — literal projections and schema-backed
/// identifier projections.  Complex expressions (arithmetic, functions, …) are
/// left to execute-time validation.
pub fn validate(schema_map: &SchemaMap, statement: &StatementPlan) -> Result<()> {
    let set_expr = match statement {
        StatementPlan::Query(query) => Some(&query.body),
        StatementPlan::Insert { source, .. } => Some(&source.body),
        StatementPlan::CreateTable { source, .. } => source.as_ref().map(|q| &q.body),
        _ => None,
    };

    if let Some(set_expr) = set_expr {
        validate_set_expr(schema_map, set_expr)?;
    }

    Ok(())
}

fn validate_set_expr(schema_map: &SchemaMap, set_expr: &SetExprPlan) -> ValidateResult {
    match set_expr {
        SetExprPlan::Select(select) => validate_select(schema_map, select),
        SetExprPlan::Values(_) => Ok(()),
        SetExprPlan::Union { left, right, .. } => {
            // Recurse first so inner UNIONs are checked before the outer one.
            validate_set_expr(schema_map, left)?;
            validate_set_expr(schema_map, right)?;

            let left_types = infer_column_types(schema_map, left);
            let right_types = infer_column_types(schema_map, right);

            if let (Some(left_types), Some(right_types)) = (left_types, right_types) {
                for (index, (lt, rt)) in left_types.iter().zip(right_types.iter()).enumerate() {
                    if let (Some(l), Some(r)) = (lt, rt)
                        && !types_compatible(l, r)
                    {
                        return Err(PlanError::UnionColumnTypeMismatch {
                            index,
                            left: format!("{l}"),
                            right: format!("{r}"),
                        });
                    }
                }
            }

            Ok(())
        }
    }
}

/// Walk a SELECT body, recursing into any derived tables and subquery
/// expressions so that nested UNIONs receive the same type-compatibility check.
fn validate_select(schema_map: &SchemaMap, select: &SelectPlan) -> ValidateResult {
    validate_table_factor(schema_map, &select.from.relation)?;
    for join in &select.from.joins {
        validate_table_factor(schema_map, &join.relation)?;
    }

    if let ProjectionPlan::SelectItems(items) = &select.projection {
        for item in items {
            if let SelectItemPlan::Expr { expr, .. } = item {
                validate_expr(schema_map, expr)?;
            }
        }
    }

    if let Some(selection) = &select.selection {
        validate_expr(schema_map, selection)?;
    }

    for expr in &select.group_by {
        validate_expr(schema_map, expr)?;
    }

    if let Some(having) = &select.having {
        validate_expr(schema_map, having)?;
    }

    Ok(())
}

fn validate_table_factor(schema_map: &SchemaMap, tf: &TableFactorPlan) -> ValidateResult {
    if let TableFactorPlan::Derived { subquery, .. } = tf {
        validate_set_expr(schema_map, &subquery.body)?;
    }
    Ok(())
}

/// Recursively walk an expression, validating any embedded subquery bodies.
fn validate_expr(schema_map: &SchemaMap, expr: &ExprPlan) -> ValidateResult {
    try_visit_expr(expr, &mut |expr| match expr {
        ExprPlan::Subquery(query) => validate_set_expr(schema_map, &query.body),
        ExprPlan::InSubquery { subquery, .. } | ExprPlan::Exists { subquery, .. } => {
            validate_set_expr(schema_map, &subquery.body)
        }
        _ => Ok(()),
    })
}

/// Returns `Some(Vec<Option<DataType>>)` when the column types of a set
/// expression can be partially or fully determined at plan time.  Each element
/// is `Some(type)` when the type is known, or `None` when it cannot be
/// inferred statically (e.g. arbitrary expressions).  Returns `None` when no
/// type information is available at all.
fn infer_column_types(
    schema_map: &SchemaMap,
    set_expr: &SetExprPlan,
) -> Option<Vec<Option<DataType>>> {
    match set_expr {
        SetExprPlan::Select(select) => infer_select_types(schema_map, select),
        // VALUES types are determined at execute time.
        SetExprPlan::Values(_) => None,
        // The output type of a UNION equals the left branch's output type.
        SetExprPlan::Union { left, .. } => infer_column_types(schema_map, left),
    }
}

fn infer_select_types(
    schema_map: &SchemaMap,
    select: &SelectPlan,
) -> Option<Vec<Option<DataType>>> {
    let items = match &select.projection {
        ProjectionPlan::SelectItems(items) => items,
        ProjectionPlan::SchemalessMap => return None,
    };

    if items.is_empty() {
        return None;
    }

    let table_ctx = {
        use crate::plan::statement::TableFactorPlan;
        match &select.from.relation {
            TableFactorPlan::Table { name, alias, .. } => {
                let key = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
                schema_map.get(name.as_str()).map(|schema| (key, schema))
            }
            _ => None,
        }
    };

    let types = items
        .iter()
        .map(|item| match item {
            SelectItemPlan::Expr { expr, .. } => infer_expr_type(expr, table_ctx),
            SelectItemPlan::Wildcard | SelectItemPlan::QualifiedWildcard(_) => None,
        })
        .collect();

    Some(types)
}

fn infer_expr_type(expr: &ExprPlan, table: Option<(&str, &Schema)>) -> Option<DataType> {
    match expr {
        ExprPlan::Literal(lit) => Some(infer_literal_type(lit)),
        ExprPlan::Identifier(col) => {
            let (_, schema) = table?;
            schema
                .column_defs
                .as_ref()?
                .iter()
                .find(|cd| &cd.name == col)
                .map(|cd| cd.data_type.clone())
        }
        ExprPlan::CompoundIdentifier { alias, ident } => {
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

/// Returns true when `left` and `right` may appear as corresponding columns in
/// a UNION.  Identical types always pass.  Any two distinct numeric types are
/// also accepted — matching the implicit promotion behaviour of `PostgreSQL`
/// and `MySQL`.
fn types_compatible(left: &DataType, right: &DataType) -> bool {
    left == right || (is_numeric(left) && is_numeric(right))
}

/// Returns `true` if `ty` is a numeric data type.
fn is_numeric(t: &DataType) -> bool {
    match t {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int
        | DataType::Int128
        | DataType::Uint8
        | DataType::Uint16
        | DataType::Uint32
        | DataType::Uint64
        | DataType::Uint128
        | DataType::Float32
        | DataType::Float
        | DataType::Decimal => true,
        DataType::Boolean
        | DataType::Text
        | DataType::Bytea
        | DataType::Inet
        | DataType::Date
        | DataType::Timestamp
        | DataType::Time
        | DataType::Interval
        | DataType::Uuid
        | DataType::Map
        | DataType::List
        | DataType::Point => false,
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
            mock::run,
            plan::{
                PlanError, SelectPlan, StatementPlan, TableFactorPlan, TableWithJoinsPlan,
                fetch_schema_map,
                statement::{ExprPlan, ProjectionPlan},
            },
            prelude::{parse, translate},
        },
        futures::executor::block_on,
        std::collections::HashMap,
    };

    fn make_select_plan(projection: ProjectionPlan) -> SelectPlan {
        use crate::plan::statement::TableAliasPlan;
        SelectPlan {
            distinct: false,
            projection,
            from: TableWithJoinsPlan {
                relation: TableFactorPlan::Series {
                    alias: TableAliasPlan {
                        name: "s".to_owned(),
                        columns: vec![],
                    },
                    size: ExprPlan::Literal(crate::ast::Literal::Number("1".parse().unwrap())),
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
        let statement: StatementPlan = translate(&parsed).unwrap().into();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();
        validate(&schema_map, &statement)
    }

    #[test]
    fn schemaless_map_projection_returns_none() {
        let result = infer_select_types(
            &HashMap::new(),
            &make_select_plan(ProjectionPlan::SchemalessMap),
        );
        assert!(
            result.is_none(),
            "SchemalessMap is set by the planner and never appears in the raw AST, so this path can only be exercised by constructing a SelectPlan directly",
        );
    }

    #[test]
    fn empty_select_items_returns_none() {
        let result = infer_select_types(
            &HashMap::new(),
            &make_select_plan(ProjectionPlan::SelectItems(vec![])),
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

    #[test]
    fn nested_union_in_derived_table_is_rejected() {
        let storage = run("");
        assert_eq!(
            check(
                &storage,
                "SELECT * FROM (SELECT 1, 'a' UNION SELECT 2, 3) AS t",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 1,
                left: "TEXT".to_owned(),
                right: "INT".to_owned(),
            }
            .into()),
            "a UNION inside a derived table must be validated even though the outer body is a plain Select",
        );
    }

    #[test]
    fn nested_union_in_derived_table_match_is_accepted() {
        let storage = run("");
        assert!(
            check(
                &storage,
                "SELECT * FROM (SELECT 1, 2 UNION SELECT 3, 4) AS t",
            )
            .is_ok(),
        );
    }

    #[test]
    fn nested_union_in_join_derived_table_is_rejected() {
        let storage = run("CREATE TABLE T (id INTEGER);");
        assert_eq!(
            check(
                &storage,
                "SELECT * FROM T JOIN (SELECT 1, 'a' UNION SELECT 2, 3) AS u ON TRUE",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 1,
                left: "TEXT".to_owned(),
                right: "INT".to_owned(),
            }
            .into()),
            "a UNION inside a JOIN's derived table must also be validated",
        );
    }

    #[test]
    fn nested_union_in_in_subquery_is_rejected() {
        let storage = run("CREATE TABLE T (id INTEGER);");
        assert_eq!(
            check(
                &storage,
                "SELECT id FROM T WHERE id IN (SELECT 1 UNION SELECT 'a')",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 0,
                left: "INT".to_owned(),
                right: "TEXT".to_owned(),
            }
            .into()),
            "a UNION inside an IN subquery must be validated",
        );
    }

    #[test]
    fn nested_union_in_exists_subquery_is_rejected() {
        let storage = run("CREATE TABLE T (id INTEGER);");
        assert_eq!(
            check(
                &storage,
                "SELECT id FROM T WHERE EXISTS (SELECT 1 UNION SELECT 'a')",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 0,
                left: "INT".to_owned(),
                right: "TEXT".to_owned(),
            }
            .into()),
            "a UNION inside an EXISTS subquery must be validated",
        );
    }

    #[test]
    fn nested_union_in_case_exists_is_rejected() {
        let storage = run("");
        assert_eq!(
            check(
                &storage,
                "SELECT CASE WHEN EXISTS (SELECT 1 UNION SELECT 'a') THEN 1 ELSE 0 END",
            ),
            Err(PlanError::UnionColumnTypeMismatch {
                index: 0,
                left: "INT".to_owned(),
                right: "TEXT".to_owned(),
            }
            .into()),
            "a UNION inside a CASE WHEN EXISTS must be caught by the expression visitor",
        );
    }

    #[test]
    fn int_float_numeric_promotion_is_accepted() {
        let storage = run("");
        assert!(
            check(&storage, "SELECT 1 UNION SELECT 1.5").is_ok(),
            "INT and FLOAT are in the same numeric family and should be accepted",
        );
        assert!(
            check(&storage, "SELECT 1.5 UNION SELECT 1").is_ok(),
            "FLOAT and INT should also be accepted in reverse order",
        );
    }
}
