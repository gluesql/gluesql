use {
    super::PlanError,
    crate::{
        data::Schema,
        plan::{
            ExprPlan, JoinConstraintPlan, JoinOperatorPlan, ProjectionPlan, QueryPlan,
            SelectItemPlan, SelectPlan, SetExprPlan, StatementPlan, TableAliasPlan,
            TableFactorPlan, expr::try_visit_expr,
        },
        result::Result,
    },
    std::{
        collections::{HashMap, HashSet},
        rc::Rc,
    },
};

type SchemaMap = HashMap<String, Schema>;
type ValidateResult<T = ()> = std::result::Result<T, PlanError>;

#[derive(Clone)]
struct RelationBinding {
    columns: Option<HashSet<String>>,
}

struct Scope {
    relations: Vec<RelationBinding>,
    outer: Option<Rc<Scope>>,
}

impl Scope {
    fn validate_unqualified_column(&self, column_name: &str) -> ValidateResult {
        let mut scope = Some(self);

        while let Some(current) = scope {
            let mut matches = 0;
            let mut has_unknown = false;

            for relation in &current.relations {
                match &relation.columns {
                    Some(columns) if columns.contains(column_name) => matches += 1,
                    Some(_) => {}
                    None => has_unknown = true,
                }
            }

            if matches > 1 {
                return Err(PlanError::ColumnReferenceAmbiguous(column_name.to_owned()));
            }

            if matches == 1 || has_unknown {
                return Ok(());
            }

            scope = current.outer.as_deref();
        }

        Ok(())
    }
}

pub fn validate(schema_map: &SchemaMap, statement: &StatementPlan) -> Result<()> {
    validate_statement(schema_map, statement).map_err(Into::into)
}

fn validate_statement(schema_map: &SchemaMap, statement: &StatementPlan) -> ValidateResult {
    match statement {
        StatementPlan::Query(query) => validate_query(schema_map, query, None).map(|_| ()),
        StatementPlan::Insert { source, .. } => {
            validate_query(schema_map, source, None).map(|_| ())
        }
        StatementPlan::CreateTable { source, .. } => source.as_deref().map_or(Ok(()), |query| {
            validate_query(schema_map, query, None).map(|_| ())
        }),
        StatementPlan::Update {
            table_name,
            assignments,
            selection,
        } => {
            let scope = single_table_scope(schema_map, table_name);
            for assignment in assignments {
                validate_expr(schema_map, &assignment.value, scope.as_ref())?;
            }
            selection.as_ref().map_or(Ok(()), |expr| {
                validate_expr(schema_map, expr, scope.as_ref())
            })
        }
        StatementPlan::Delete {
            table_name,
            selection,
        } => {
            let scope = single_table_scope(schema_map, table_name);
            selection.as_ref().map_or(Ok(()), |expr| {
                validate_expr(schema_map, expr, scope.as_ref())
            })
        }
        _ => Ok(()),
    }
}

fn validate_query(
    schema_map: &SchemaMap,
    query: &QueryPlan,
    outer: Option<Rc<Scope>>,
) -> ValidateResult<Option<Rc<Scope>>> {
    let scope = match &query.body {
        SetExprPlan::Select(select) => validate_select(schema_map, select, outer)?,
        SetExprPlan::Values(values) => {
            for expr in values.0.iter().flatten() {
                validate_expr(schema_map, expr, outer.as_ref())?;
            }
            outer
        }
    };

    for order_by in &query.order_by {
        validate_expr(schema_map, &order_by.expr, scope.as_ref())?;
    }
    if let Some(limit) = &query.limit {
        validate_expr(schema_map, limit, scope.as_ref())?;
    }
    if let Some(offset) = &query.offset {
        validate_expr(schema_map, offset, scope.as_ref())?;
    }

    Ok(scope)
}

fn validate_select(
    schema_map: &SchemaMap,
    select: &SelectPlan,
    outer: Option<Rc<Scope>>,
) -> ValidateResult<Option<Rc<Scope>>> {
    let mut relations = Vec::with_capacity(select.from.joins.len() + 1);
    let mut identifiers = HashSet::new();

    validate_table_factor(schema_map, &select.from.relation, outer.as_ref())?;
    push_relation(
        schema_map,
        &select.from.relation,
        &mut relations,
        &mut identifiers,
    )?;

    for join in &select.from.joins {
        validate_table_factor(schema_map, &join.relation, outer.as_ref())?;
        push_relation(schema_map, &join.relation, &mut relations, &mut identifiers)?;

        let join_scope = Rc::new(Scope {
            relations: relations.clone(),
            outer: outer.as_ref().map(Rc::clone),
        });
        match &join.join_operator {
            JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => {
                validate_expr(schema_map, expr, Some(&join_scope))?;
            }
            JoinOperatorPlan::Inner(JoinConstraintPlan::None)
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => {}
        }
    }

    let scope = Rc::new(Scope { relations, outer });

    if let ProjectionPlan::SelectItems(projection) = &select.projection {
        for item in projection {
            if let SelectItemPlan::Expr { expr, .. } = item {
                validate_expr(schema_map, expr, Some(&scope))?;
            }
        }
    }
    if let Some(selection) = &select.selection {
        validate_expr(schema_map, selection, Some(&scope))?;
    }
    for group_by in &select.group_by {
        validate_expr(schema_map, group_by, Some(&scope))?;
    }
    if let Some(having) = &select.having {
        validate_expr(schema_map, having, Some(&scope))?;
    }

    Ok(Some(scope))
}

fn validate_table_factor(
    schema_map: &SchemaMap,
    table_factor: &TableFactorPlan,
    outer: Option<&Rc<Scope>>,
) -> ValidateResult {
    match table_factor {
        TableFactorPlan::Derived { subquery, .. } => {
            validate_query(schema_map, subquery, outer.cloned()).map(|_| ())
        }
        TableFactorPlan::Series { size, .. } => validate_expr(schema_map, size, outer),
        TableFactorPlan::Table { .. } | TableFactorPlan::Dictionary { .. } => Ok(()),
    }
}

fn push_relation(
    schema_map: &SchemaMap,
    table_factor: &TableFactorPlan,
    relations: &mut Vec<RelationBinding>,
    identifiers: &mut HashSet<String>,
) -> ValidateResult {
    let identifier = table_factor.alias_name().to_owned();
    if !identifiers.insert(identifier.clone()) {
        return Err(PlanError::DuplicateRelationIdentifier(identifier));
    }

    relations.push(RelationBinding {
        columns: relation_columns(schema_map, table_factor),
    });
    Ok(())
}

fn relation_columns(
    schema_map: &SchemaMap,
    table_factor: &TableFactorPlan,
) -> Option<HashSet<String>> {
    let columns = match table_factor {
        TableFactorPlan::Table { name, alias, .. } => {
            let columns = schema_map
                .get(name)?
                .column_defs
                .as_ref()?
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            apply_column_aliases(columns, alias.as_ref())
        }
        TableFactorPlan::Derived { subquery, alias } => {
            let columns = query_output_columns(subquery)?;
            apply_column_aliases(columns, Some(alias))
        }
        TableFactorPlan::Series { alias, .. } => {
            apply_column_aliases(vec!["N".to_owned()], Some(alias))
        }
        TableFactorPlan::Dictionary { .. } => return None,
    };

    Some(columns.into_iter().collect())
}

fn apply_column_aliases(columns: Vec<String>, alias: Option<&TableAliasPlan>) -> Vec<String> {
    let Some(alias) = alias else {
        return columns;
    };

    alias
        .columns
        .iter()
        .cloned()
        .chain(columns.into_iter().skip(alias.columns.len()))
        .collect()
}

fn query_output_columns(query: &QueryPlan) -> Option<Vec<String>> {
    match &query.body {
        SetExprPlan::Select(select) => match &select.projection {
            ProjectionPlan::SelectItems(items) => items
                .iter()
                .map(|item| match item {
                    SelectItemPlan::Expr { label, .. } => Some(label.clone()),
                    SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => None,
                })
                .collect(),
            ProjectionPlan::SchemalessMap => None,
        },
        SetExprPlan::Values(_) => None,
    }
}

fn single_table_scope(schema_map: &SchemaMap, table_name: &str) -> Option<Rc<Scope>> {
    let columns = schema_map
        .get(table_name)?
        .column_defs
        .as_ref()?
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();

    Some(Rc::new(Scope {
        relations: vec![RelationBinding {
            columns: Some(columns),
        }],
        outer: None,
    }))
}

fn validate_expr(
    schema_map: &SchemaMap,
    expr: &ExprPlan,
    scope: Option<&Rc<Scope>>,
) -> ValidateResult {
    try_visit_expr(expr, &mut |expr| match expr {
        ExprPlan::Identifier(ident) => {
            scope.map_or(Ok(()), |scope| scope.validate_unqualified_column(ident))
        }
        ExprPlan::Subquery(subquery)
        | ExprPlan::Exists { subquery, .. }
        | ExprPlan::InSubquery { subquery, .. } => {
            validate_query(schema_map, subquery, scope.cloned()).map(|_| ())
        }
        _ => Ok(()),
    })
}

#[cfg(test)]
mod tests {
    use {
        super::validate,
        crate::{
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{PlanError, StatementPlan, fetch_schema_map},
            translate::translate,
        },
    };

    fn setup_storage() -> MockStorage {
        run("
            CREATE TABLE Users (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE Items (
                id INTEGER,
                quantity INTEGER
            );
        ")
    }

    fn validate_sql(storage: &MockStorage, sql: &str) -> crate::result::Result<()> {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        validate(&schema_map, &statement)
    }

    fn assert_plan_error(storage: &MockStorage, sql: &str, expected: PlanError) {
        assert_eq!(validate_sql(storage, sql), Err(expected.into()), "{sql}");
    }

    fn assert_plan_ok(storage: &MockStorage, sql: &str) {
        assert!(validate_sql(storage, sql).is_ok(), "{sql}");
    }

    #[test]
    fn rejects_unqualified_ambiguity_in_query_expressions() {
        let storage = setup_storage();
        let cases = [
            "SELECT id FROM Users U JOIN Items I ON U.id = I.id",
            "SELECT id + 1 FROM Users U JOIN Items I ON U.id = I.id",
            "SELECT COALESCE(id, 0) FROM Users U JOIN Items I ON U.id = I.id",
            "SELECT U.name FROM Users U JOIN Items I ON U.id = I.id WHERE id > 0",
            "SELECT U.name FROM Users U JOIN Items I ON id = I.id",
            "SELECT U.id FROM Users U JOIN Items I ON U.id = I.id GROUP BY id",
            "SELECT U.id FROM Users U JOIN Items I ON U.id = I.id HAVING id > 0",
            "SELECT U.id FROM Users U JOIN Items I ON U.id = I.id ORDER BY id",
        ];

        for sql in cases {
            assert_plan_error(
                &storage,
                sql,
                PlanError::ColumnReferenceAmbiguous("id".to_owned()),
            );
        }
    }

    #[test]
    fn rejects_duplicate_relation_identifiers() {
        let storage = setup_storage();
        let cases = [
            "SELECT A.id FROM Users A JOIN Items A ON A.id = A.id",
            "SELECT Users.id FROM Users JOIN Users ON Users.id = Users.id",
        ];

        for sql in cases {
            let identifier = if sql.contains("Items A") {
                "A"
            } else {
                "Users"
            };
            assert_plan_error(
                &storage,
                sql,
                PlanError::DuplicateRelationIdentifier(identifier.to_owned()),
            );
        }
    }

    #[test]
    fn allows_unambiguous_and_correlated_references() {
        let storage = setup_storage();
        let cases = [
            "SELECT U.name FROM Users U JOIN Items I ON U.id = I.id",
            "SELECT name FROM Users U JOIN Items I ON U.id = I.id",
            "SELECT U.name FROM Users U WHERE EXISTS (SELECT 1 FROM Items I WHERE I.id = U.id)",
            "SELECT U.name FROM Users U WHERE EXISTS (SELECT 1 FROM Items U WHERE U.id = 1)",
        ];

        for sql in cases {
            assert_plan_ok(&storage, sql);
        }
    }
}
