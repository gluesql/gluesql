use {
    super::PlanError,
    crate::{
        data::Schema,
        plan::{
            AggregationInputPlan, DistinctInputPlan, DistinctPlan, ExprPlan, FilterInputPlan,
            JoinInputPlan, JoinPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan,
            ProjectInputPlan, ProjectPlan, ProjectionPlan, QueryPlan, SelectItemPlan,
            StatementPlan, TableFactorPlan,
        },
        result::Result,
    },
    std::{collections::HashMap, rc::Rc},
};

type SchemaMap = HashMap<String, Schema>;
/// Validate user select column should not be ambiguous
pub fn validate(schema_map: &SchemaMap, statement: &StatementPlan) -> Result<()> {
    let query = match statement {
        StatementPlan::Query(query) => Some(query),
        StatementPlan::Insert { source, .. } => Some(source),
        StatementPlan::CreateTable { source, .. } => source.as_deref(),
        _ => None,
    };

    if let Some(query) = query {
        let Some(project) = query_project(query) else {
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
    query: &'a QueryPlan,
) -> Option<Rc<Context<'a>>> {
    query_project(query).and_then(|project| contextualize_project_input(schema_map, &project.input))
}

fn offset_project(offset: &OffsetPlan) -> Option<&ProjectPlan> {
    match &offset.input {
        OffsetInputPlan::Project(project) => Some(project),
        OffsetInputPlan::Values(_) | OffsetInputPlan::ValuesOrderBy(_) => None,
        OffsetInputPlan::SelectOrderBy(order_by) => Some(&order_by.input),
        OffsetInputPlan::Distinct(distinct) => Some(distinct_project(distinct)),
    }
}

fn distinct_project(distinct: &DistinctPlan) -> &ProjectPlan {
    match &distinct.input {
        DistinctInputPlan::Project(project) => project,
        DistinctInputPlan::SelectOrderBy(order_by) => &order_by.input,
    }
}

fn query_project(query: &QueryPlan) -> Option<&ProjectPlan> {
    match query {
        QueryPlan::Project(project) => Some(project),
        QueryPlan::Values(_) | QueryPlan::ValuesOrderBy(_) => None,
        QueryPlan::SelectOrderBy(order_by) => Some(&order_by.input),
        QueryPlan::Distinct(distinct) => Some(distinct_project(distinct)),
        QueryPlan::Offset(offset) => offset_project(offset),
        QueryPlan::Limit(LimitPlan { input, .. }) => match input {
            LimitInputPlan::Project(project) => Some(project),
            LimitInputPlan::Values(_) | LimitInputPlan::ValuesOrderBy(_) => None,
            LimitInputPlan::SelectOrderBy(order_by) => Some(&order_by.input),
            LimitInputPlan::Distinct(distinct) => Some(distinct_project(distinct)),
            LimitInputPlan::Offset(offset) => offset_project(offset),
        },
    }
}

fn contextualize_project_input<'a>(
    schema_map: &'a SchemaMap,
    input: &'a ProjectInputPlan,
) -> Option<Rc<Context<'a>>> {
    match input {
        ProjectInputPlan::Relation(relation) => contextualize_table_factor(schema_map, relation),
        ProjectInputPlan::Join(join) => contextualize_join(schema_map, join),
        ProjectInputPlan::Filter(filter) => contextualize_filter_input(schema_map, &filter.input),
        ProjectInputPlan::Aggregation(aggregation) => {
            contextualize_aggregation_input(schema_map, &aggregation.input)
        }
        ProjectInputPlan::Having(having) => {
            contextualize_aggregation_input(schema_map, &having.input.input)
        }
    }
}

fn contextualize_aggregation_input<'a>(
    schema_map: &'a SchemaMap,
    input: &'a AggregationInputPlan,
) -> Option<Rc<Context<'a>>> {
    match input {
        AggregationInputPlan::Relation(relation) => {
            contextualize_table_factor(schema_map, relation)
        }
        AggregationInputPlan::Join(join) => contextualize_join(schema_map, join),
        AggregationInputPlan::Filter(filter) => {
            contextualize_filter_input(schema_map, &filter.input)
        }
    }
}

fn contextualize_filter_input<'a>(
    schema_map: &'a SchemaMap,
    input: &'a FilterInputPlan,
) -> Option<Rc<Context<'a>>> {
    match input {
        FilterInputPlan::Relation(relation) => contextualize_table_factor(schema_map, relation),
        FilterInputPlan::Join(join) => contextualize_join(schema_map, join),
    }
}

fn contextualize_join<'a>(
    schema_map: &'a SchemaMap,
    join: &'a JoinPlan,
) -> Option<Rc<Context<'a>>> {
    let input = match &join.input {
        JoinInputPlan::Relation(relation) => contextualize_table_factor(schema_map, relation),
        JoinInputPlan::Join(join) => contextualize_join(schema_map, join),
    };
    let relation = contextualize_table_factor(schema_map, &join.relation);

    Context::concat(input, relation)
}

fn contextualize_table_factor<'a>(
    schema_map: &'a SchemaMap,
    table_factor: &'a TableFactorPlan,
) -> Option<Rc<Context<'a>>> {
    match table_factor {
        TableFactorPlan::Table { name, .. } => {
            let schema = schema_map.get(name);
            schema.map(|schema| Rc::from(Context::new(get_labels(schema), None)))
        }
        TableFactorPlan::Derived { subquery, .. } => contextualize_query(schema_map, subquery),
        TableFactorPlan::Series { .. } | TableFactorPlan::Dictionary { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        mock::run,
        plan::{fetch_schema_map, validate},
        prelude::{parse, translate},
    };

    #[test]
    fn validate_test() {
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
            let statement = translate(&parsed).unwrap().into();
            let schema_map = fetch_schema_map(&storage, &statement).unwrap();
            let actual = validate(&schema_map, &statement).is_ok();

            assert_eq!(actual, expected);
        }
    }
}
