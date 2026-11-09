use {
    super::PlannerError,
    crate::{
        plan::{
            ExprPlan, ProjectInputPlan, ProjectionPlan, QueryPlan, SchemaMap, SchemaMapPlan,
            SchemaPlan, SelectItemPlan, SourcePlan, StatementPlan, StatementPlanKind,
        },
        result::Result,
    },
    std::rc::Rc,
};

/// Validate user select column should not be ambiguous
pub fn validate(statement: &StatementPlan) -> Result<()> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(PlannerError::UnplannedSchemaMap.into());
    };

    let query = match &statement.kind {
        StatementPlanKind::Query(query) => Some(query),
        StatementPlanKind::Insert { source, .. } => Some(source),
        StatementPlanKind::CreateTable { source, .. } => source.as_deref(),
        _ => None,
    };

    if let Some(query) = query {
        let Some(project) = query.project() else {
            return Ok(());
        };
        let ProjectionPlan::SelectItems(projection) = &project.projection else {
            return Ok(());
        };

        for select_item in projection {
            if let SelectItemPlan::Expr {
                expr: ExprPlan::Identifier(ident),
                ..
            } = select_item
                && let Some(context) = contextualize_query(schema_map, query)
            {
                context.validate_duplicated(ident)?;
            }
        }
    }

    Ok(())
}

enum Context<'a> {
    Data {
        labels: Option<Vec<&'a str>>,
        next: Option<Rc<Context<'a>>>,
    },
    Bridge {
        left: Rc<Context<'a>>,
        right: Rc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    fn new(labels: Option<Vec<&'a str>>, next: Option<Rc<Context<'a>>>) -> Self {
        Self::Data { labels, next }
    }

    fn concat(left: Option<Rc<Context<'a>>>, right: Option<Rc<Context<'a>>>) -> Option<Rc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Rc::new(Self::Bridge { left, right })),
            (context @ Some(_), None) | (None, context @ Some(_)) => context,
            (None, None) => None,
        }
    }

    fn validate_duplicated(&self, column_name: &str) -> Result<()> {
        fn validate(context: &Context, column_name: &str) -> Result<bool> {
            let (left, right) = match context {
                Context::Data { labels, next, .. } => {
                    let current = labels
                        .as_ref()
                        .is_some_and(|labels| labels.contains(&column_name));

                    let next = next
                        .as_ref()
                        .map_or(Ok(false), |next| validate(next, column_name))?;

                    (current, next)
                }
                Context::Bridge { left, right } => {
                    let left = validate(left, column_name)?;
                    let right = validate(right, column_name)?;

                    (left, right)
                }
            };

            if left && right {
                Err(PlannerError::ColumnReferenceAmbiguous(column_name.to_owned()).into())
            } else {
                Ok(left || right)
            }
        }

        validate(self, column_name).map(|_| ())
    }
}

fn get_labels(schema: &SchemaPlan) -> Option<Vec<&str>> {
    schema.column_defs.as_ref().map(|column_defs| {
        column_defs
            .iter()
            .map(|column_def| column_def.name.as_str())
            .collect::<Vec<_>>()
    })
}

fn contextualize_query<'a>(
    schema_map: &'a SchemaMap,
    query: &'a QueryPlan,
) -> Option<Rc<Context<'a>>> {
    query
        .project()
        .and_then(|project| contextualize_project_input(schema_map, &project.input))
}

fn contextualize_project_input<'a>(
    schema_map: &'a SchemaMap,
    input: &'a ProjectInputPlan,
) -> Option<Rc<Context<'a>>> {
    input.joined_sources().into_iter().fold(
        contextualize_source(schema_map, input.base_source()),
        |context, source| Context::concat(context, contextualize_source(schema_map, source)),
    )
}

fn contextualize_source<'a>(
    schema_map: &'a SchemaMap,
    source: &'a SourcePlan,
) -> Option<Rc<Context<'a>>> {
    match source {
        SourcePlan::Table(table) => {
            let schema = schema_map.get(&table.name);
            schema.map(|schema| Rc::from(Context::new(get_labels(schema), None)))
        }
        SourcePlan::Derived(derived) => contextualize_query(schema_map, &derived.query),
        SourcePlan::Series(_) | SourcePlan::Dictionary(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        mock::run,
        planner::{PlannerError, plan_schema_map, validate},
        prelude::{parse, translate},
    };

    #[test]
    fn unplanned_schema_map() {
        let input = translate(&parse("SELECT * FROM Item").unwrap()[0])
            .unwrap()
            .into();

        let actual = validate(&input);
        let expected = Err(PlannerError::UnplannedSchemaMap.into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn validate_test() {
        let storage = run("
            CREATE TABLE Users (
                id INTEGER,
                name TEXT
            );
        ");

        let cases = [
            ("SELECT * FROM (SELECT * FROM Users) AS Sub", Ok(())),
            ("SELECT * FROM SERIES(3)", Ok(())),
            (
                "SELECT id FROM Users A JOIN Users B on A.id = B.id",
                Err(PlannerError::ColumnReferenceAmbiguous("id".to_owned()).into()),
            ),
            (
                "INSERT INTO Users SELECT id FROM Users A JOIN Users B on A.id = B.id",
                Err(PlannerError::ColumnReferenceAmbiguous("id".to_owned()).into()),
            ),
            (
                "CREATE TABLE Ids AS SELECT id FROM Users A JOIN Users B on A.id = B.id",
                Err(PlannerError::ColumnReferenceAmbiguous("id".to_owned()).into()),
            ),
        ];

        for (sql, expected) in cases {
            let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap().into();
            let statement = plan_schema_map(&storage, statement).unwrap();
            let actual = validate(&statement);

            assert_eq!(actual, expected);
        }
    }
}
