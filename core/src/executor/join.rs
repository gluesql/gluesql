use {
    super::fetch::{fetch_relation_columns, fetch_relation_rows},
    crate::{
        data::{Key, Row, Value},
        executor::{context::RowContext, evaluate::evaluate, filter::check_expr},
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan,
            TableFactorPlan,
        },
        result::Result,
        store::GStore,
    },
    itertools::Itertools,
    std::{borrow::Cow, collections::HashMap, sync::Arc},
};

pub struct Join<'a, T: GStore> {
    storage: &'a T,
    join_clauses: &'a [JoinPlan],
    filter_context: Option<Arc<RowContext<'a>>>,
}

type JoinItem<'a> = Arc<RowContext<'a>>;
type Joined<'a> = Box<dyn Iterator<Item = Result<JoinItem<'a>>> + Send + 'a>;
type JoinInput<'a> = Box<dyn Iterator<Item = Result<RowContext<'a>>> + Send + 'a>;

struct LeftOuter<'a> {
    rows: Joined<'a>,
    init: Option<JoinItem<'a>>,
    matched: bool,
}

impl<'a> LeftOuter<'a> {
    fn new(rows: Joined<'a>, init: JoinItem<'a>) -> Self {
        Self {
            rows,
            init: Some(init),
            matched: false,
        }
    }
}

impl<'a> Iterator for LeftOuter<'a> {
    type Item = Result<JoinItem<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next() {
            Some(item) => {
                self.matched = true;
                Some(item)
            }
            None if !self.matched => self.init.take().map(Ok),
            None => None,
        }
    }
}

impl<'a, T: GStore> Join<'a, T> {
    pub fn new(
        storage: &'a T,
        join_clauses: &'a [JoinPlan],
        filter_context: Option<Arc<RowContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            join_clauses,
            filter_context,
        }
    }

    pub fn apply(self, rows: JoinInput<'a>) -> Result<Joined<'a>> {
        let mut rows: Joined = Box::new(rows.map(|row| row.map(Arc::new)));

        for join_clause in self.join_clauses {
            rows = join(
                self.storage,
                self.filter_context.as_ref().map(Arc::clone),
                join_clause,
                rows,
            )?;
        }

        Ok(rows)
    }
}

fn join<'a, T: GStore>(
    storage: &'a T,
    filter_context: Option<Arc<RowContext<'a>>>,
    join_plan: &'a JoinPlan,
    left_rows: Joined<'a>,
) -> Result<Joined<'a>> {
    let JoinPlan {
        relation,
        join_operator,
        join_executor,
    } = join_plan;

    let table_alias = relation.alias_name();
    let join_executor =
        JoinExecutor::new(storage, relation, filter_context.as_ref(), join_executor)?;

    let (join_operator, where_clause) = match join_operator {
        JoinOperatorPlan::Inner(JoinConstraintPlan::None) => (JoinOperator::Inner, None),
        JoinOperatorPlan::Inner(JoinConstraintPlan::On(where_clause)) => {
            (JoinOperator::Inner, Some(where_clause))
        }
        JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => (JoinOperator::LeftOuter, None),
        JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(where_clause)) => {
            (JoinOperator::LeftOuter, Some(where_clause))
        }
    };

    let columns: Arc<[String]> = Arc::from(fetch_relation_columns(storage, relation)?);
    let rows = left_rows.flat_map(move |project_context| {
        let project_context = match project_context {
            Ok(project_context) => project_context,
            Err(error) => return Box::new(std::iter::once(Err(error))) as Joined<'a>,
        };

        let init_context = {
            let columns = Arc::clone(&columns);
            let init_row = Row {
                values: columns.iter().map(|_| Value::Null).collect(),
                columns,
            };

            Arc::new(RowContext::new(
                table_alias,
                Cow::Owned(init_row),
                Some(Arc::clone(&project_context)),
            ))
        };

        let row_filter_context = match filter_context.as_ref() {
            Some(filter_context) => Arc::new(RowContext::concat(
                Arc::clone(&project_context),
                Arc::clone(filter_context),
            )),
            None => Arc::clone(&project_context),
        };
        let row_filter_context = Some(row_filter_context);

        let rows: Joined<'a> = match &join_executor {
            JoinExecutor::NestedLoop => {
                let rows = match fetch_relation_rows(storage, relation, row_filter_context.as_ref())
                {
                    Ok(rows) => rows,
                    Err(error) => return Box::new(std::iter::once(Err(error))) as Joined<'a>,
                };
                Box::new(rows.filter_map(move |row| {
                    let row = match row {
                        Ok(row) => row,
                        Err(error) => return Some(Err(error)),
                    };

                    match check_where_clause(
                        storage,
                        table_alias,
                        row_filter_context.as_ref().map(Arc::clone),
                        Some(Arc::clone(&project_context)),
                        where_clause,
                        Cow::Owned(row),
                    ) {
                        Ok(Some(row)) => Some(Ok(row)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    }
                }))
            }
            JoinExecutor::Hash {
                rows_map,
                value_expr,
            } => {
                let rows = match evaluate(storage, row_filter_context.as_ref(), None, value_expr)
                    .and_then(|evaluated| {
                        Key::try_from(evaluated).map(|hash_key| rows_map.get(&hash_key))
                    }) {
                    Ok(rows) => rows,
                    Err(error) => return Box::new(std::iter::once(Err(error))) as Joined<'a>,
                };

                match rows {
                    Some(rows) => {
                        let rows =
                            rows.clone().into_iter().filter_map(
                                move |row| match check_where_clause(
                                    storage,
                                    table_alias,
                                    row_filter_context.as_ref().map(Arc::clone),
                                    Some(Arc::clone(&project_context)),
                                    where_clause,
                                    Cow::Owned(row),
                                ) {
                                    Ok(Some(row)) => Some(Ok(row)),
                                    Ok(None) => None,
                                    Err(error) => Some(Err(error)),
                                },
                            );

                        Box::new(rows)
                    }
                    None => Box::new(std::iter::empty()),
                }
            }
        };

        match join_operator {
            JoinOperator::Inner => rows,
            JoinOperator::LeftOuter => Box::new(LeftOuter::new(rows, init_context)),
        }
    });

    Ok(Box::new(rows))
}

#[derive(Copy, Clone)]
enum JoinOperator {
    Inner,
    LeftOuter,
}

enum JoinExecutor<'a> {
    NestedLoop,
    Hash {
        rows_map: HashMap<Key, Vec<Row>>,
        value_expr: &'a ExprPlan,
    },
}

impl<'a> JoinExecutor<'a> {
    fn new<T: GStore>(
        storage: &'a T,
        relation: &TableFactorPlan,
        filter_context: Option<&Arc<RowContext<'a>>>,
        join_executor: &'a JoinExecutorPlan,
    ) -> Result<JoinExecutor<'a>> {
        let (key_expr, value_expr, where_clause) = match join_executor {
            JoinExecutorPlan::NestedLoop => return Ok(Self::NestedLoop),
            JoinExecutorPlan::Hash {
                key_expr,
                value_expr,
                where_clause,
            } => (key_expr, value_expr, where_clause),
        };

        let mut rows = Vec::new();
        for row in fetch_relation_rows(storage, relation, filter_context)? {
            let row = row?;
            let filter_context = Arc::new(RowContext::new(
                relation.alias_name(),
                Cow::Borrowed(&row),
                filter_context.cloned(),
            ));

            let hash_key: Key =
                evaluate(storage, Some(&filter_context), None, key_expr)?.try_into()?;

            if matches!(hash_key, Key::None) {
                continue;
            }

            let pass = match where_clause {
                Some(expr) => check_expr(storage, Some(&filter_context), None, expr)?,
                None => true,
            };

            if pass {
                rows.push((hash_key, row));
            }
        }

        Ok(Self::Hash {
            rows_map: rows.into_iter().into_group_map(),
            value_expr,
        })
    }
}

fn check_where_clause<'a, T: GStore>(
    storage: &'a T,
    table_alias: &'a str,
    filter_context: Option<Arc<RowContext<'a>>>,
    project_context: Option<Arc<RowContext<'a>>>,
    where_clause: Option<&'a ExprPlan>,
    row: Cow<'_, Row>,
) -> Result<Option<Arc<RowContext<'a>>>> {
    let filter_context = RowContext::new(table_alias, Cow::Borrowed(&row), filter_context);
    let filter_context = Some(Arc::new(filter_context));

    match where_clause {
        Some(expr) => check_expr(storage, filter_context.as_ref(), None, expr)?,
        None => true,
    }
    .then(|| RowContext::new(table_alias, Cow::Owned(row.into_owned()), project_context))
    .map(Arc::new)
    .map(Ok)
    .transpose()
}
