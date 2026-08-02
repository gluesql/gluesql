use {
    super::PlannerError,
    crate::{
        data::Schema,
        plan::{
            AggregationInputPlan, ExprPlan, FilterInputPlan, HashJoinInputPlan, HashJoinPlan,
            InnerJoinInputPlan, InnerJoinPlan, JoinConditionInputPlan, JoinConditionPlan,
            LeftOuterJoinInputPlan, LeftOuterJoinPlan, NestedLoopJoinInputPlan, NestedLoopJoinPlan,
            ProjectInputPlan, ProjectionPlan, QueryPlan, SelectItemPlan, SourcePlan, StatementPlan,
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
    query
        .project()
        .and_then(|project| contextualize_project_input(schema_map, &project.input))
}

fn contextualize_project_input<'a>(
    schema_map: &'a SchemaMap,
    input: &'a ProjectInputPlan,
) -> Option<Rc<Context<'a>>> {
    match input {
        ProjectInputPlan::Source(relation) => contextualize_source(schema_map, relation),
        ProjectInputPlan::InnerJoin(join) => contextualize_inner_join(schema_map, join),
        ProjectInputPlan::LeftOuterJoin(join) => contextualize_left_outer_join(schema_map, join),
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
        AggregationInputPlan::Source(relation) => contextualize_source(schema_map, relation),
        AggregationInputPlan::InnerJoin(join) => contextualize_inner_join(schema_map, join),
        AggregationInputPlan::LeftOuterJoin(join) => {
            contextualize_left_outer_join(schema_map, join)
        }
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
        FilterInputPlan::Source(relation) => contextualize_source(schema_map, relation),
        FilterInputPlan::InnerJoin(join) => contextualize_inner_join(schema_map, join),
        FilterInputPlan::LeftOuterJoin(join) => contextualize_left_outer_join(schema_map, join),
    }
}

fn contextualize_inner_join<'a>(
    schema_map: &'a SchemaMap,
    join: &'a InnerJoinPlan,
) -> Option<Rc<Context<'a>>> {
    match &join.input {
        InnerJoinInputPlan::NestedLoop(join) => contextualize_nested_loop(schema_map, join),
        InnerJoinInputPlan::Hash(join) => contextualize_hash(schema_map, join),
        InnerJoinInputPlan::Condition(condition) => contextualize_condition(schema_map, condition),
    }
}

fn contextualize_left_outer_join<'a>(
    schema_map: &'a SchemaMap,
    join: &'a LeftOuterJoinPlan,
) -> Option<Rc<Context<'a>>> {
    match &join.input {
        LeftOuterJoinInputPlan::NestedLoop(join) => contextualize_nested_loop(schema_map, join),
        LeftOuterJoinInputPlan::Hash(join) => contextualize_hash(schema_map, join),
        LeftOuterJoinInputPlan::Condition(condition) => {
            contextualize_condition(schema_map, condition)
        }
    }
}

fn contextualize_condition<'a>(
    schema_map: &'a SchemaMap,
    condition: &'a JoinConditionPlan,
) -> Option<Rc<Context<'a>>> {
    match &condition.input {
        JoinConditionInputPlan::NestedLoop(join) => contextualize_nested_loop(schema_map, join),
        JoinConditionInputPlan::Hash(join) => contextualize_hash(schema_map, join),
    }
}

fn contextualize_nested_loop<'a>(
    schema_map: &'a SchemaMap,
    join: &'a NestedLoopJoinPlan,
) -> Option<Rc<Context<'a>>> {
    let input = match &join.input {
        NestedLoopJoinInputPlan::Source(source) => contextualize_source(schema_map, source),
        NestedLoopJoinInputPlan::InnerJoin(join) => contextualize_inner_join(schema_map, join),
        NestedLoopJoinInputPlan::LeftOuterJoin(join) => {
            contextualize_left_outer_join(schema_map, join)
        }
    };
    let right = contextualize_source(schema_map, &join.right);

    Context::concat(input, right)
}

fn contextualize_hash<'a>(
    schema_map: &'a SchemaMap,
    join: &'a HashJoinPlan,
) -> Option<Rc<Context<'a>>> {
    let input = match &join.input {
        HashJoinInputPlan::Source(source) => contextualize_source(schema_map, source),
        HashJoinInputPlan::InnerJoin(join) => contextualize_inner_join(schema_map, join),
        HashJoinInputPlan::LeftOuterJoin(join) => contextualize_left_outer_join(schema_map, join),
    };
    let right = contextualize_source(schema_map, &join.right);

    Context::concat(input, right)
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
        planner::{fetch_schema_map, validate},
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
