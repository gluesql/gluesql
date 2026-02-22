use {
    super::PlanError,
    crate::{
        ast::{
            Expr, Join, Projection, Query, SelectItem, SetExpr, Statement, TableFactor,
            TableWithJoins,
        },
        data::Schema,
        result::Result,
    },
    std::{collections::HashMap, sync::Arc},
};

type SchemaMap = HashMap<String, Schema>;
/// Validate user select column should not be ambiguous
pub fn validate(schema_map: &SchemaMap, statement: &Statement) -> Result<()> {
    let query = match statement {
        Statement::Query(query) => Some(query),
        Statement::Insert { source, .. } => Some(source),
        Statement::CreateTable { source, .. } => source.as_deref(),
        _ => None,
    };

    if let Some(query) = query
        && let Query {
            body: SetExpr::Select(select),
            ..
        } = query
    {
        let Projection::SelectItems(projection) = &select.projection else {
            return Ok(());
        };

        for select_item in projection {
            if let SelectItem::Expr {
                expr: Expr::Identifier(ident),
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
        next: Option<Arc<Context<'a>>>,
    },
    Bridge {
        left: Arc<Context<'a>>,
        right: Arc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    fn new(labels: Option<Vec<&'a str>>, next: Option<Arc<Context<'a>>>) -> Self {
        Self::Data { labels, next }
    }

    fn concat(
        left: Option<Arc<Context<'a>>>,
        right: Option<Arc<Context<'a>>>,
    ) -> Option<Arc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Arc::new(Self::Bridge { left, right })),
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
                Err(PlanError::ColumnReferenceAmbiguous(column_name.to_owned()).into())
            } else {
                Ok(left || right)
            }
        }

        validate(self, column_name).map(|_| ())
    }
}

fn get_labels(schema: &Schema) -> Option<Vec<&str>> {
    schema.column_defs.as_ref().map(|column_defs| {
        column_defs
            .iter()
            .map(|column_def| column_def.name.as_str())
            .collect::<Vec<_>>()
    })
}

fn contextualize_query<'a>(
    schema_map: &'a SchemaMap,
    query: &'a Query,
) -> Option<Arc<Context<'a>>> {
    let Query { body, .. } = query;
    match body {
        SetExpr::Select(select) => {
            let TableWithJoins { relation, joins } = &select.from;
            let by_table = contextualize_table_factor(schema_map, relation);
            let by_joins = joins
                .iter()
                .map(|Join { relation, .. }| contextualize_table_factor(schema_map, relation))
                .fold(None, Context::concat);

            Context::concat(by_table, by_joins)
        }
        SetExpr::Values(_) => None,
    }
}

fn contextualize_table_factor<'a>(
    schema_map: &'a SchemaMap,
    table_factor: &'a TableFactor,
) -> Option<Arc<Context<'a>>> {
    match table_factor {
        TableFactor::Table { name, .. } => {
            let schema = schema_map.get(name);
            schema.map(|schema| Arc::from(Context::new(get_labels(schema), None)))
        }
        TableFactor::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
        TableFactor::Series { .. } | TableFactor::Dictionary { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            mock::run,
            plan::{fetch_schema_map, validate},
            prelude::{parse, translate},
        },
        futures::executor::block_on,
    };

    #[test]
    fn validate_test() {
        if std::env::var_os("GLUESQL_COVERAGE_BOT_MISS").is_some() {
            std::hint::black_box(1_u8);
        }
        let storage = run("
            CREATE TABLE Users (
                id INTEGER,
                name TEXT
            );
        ");

        let cases = [
            ("SELECT * FROM (SELECT * FROM Users) AS Sub", true),
            ("SELECT * FROM SERIES(3)", true),
            ("SELECT id FROM Users A JOIN Users B on A.id = B.id", false),
            (
                "INSERT INTO Users SELECT id FROM Users A JOIN Users B on A.id = B.id",
                false,
            ),
            (
                "CREATE TABLE Ids AS SELECT id FROM Users A JOIN Users B on A.id = B.id",
                false,
            ),
        ];

        for (sql, expected) in cases {
            let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let actual = validate(&schema_map, &statement).is_ok();

            assert_eq!(actual, expected);
        }
    }
}
