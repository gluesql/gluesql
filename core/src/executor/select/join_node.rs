use {
    super::{SelectedRows, source_node},
    crate::{
        data::{Key, Row, Value},
        executor::{context::RowContext, evaluate::evaluate, filter::check_expr},
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinInputPlan, JoinOperatorPlan,
            JoinPlan, SourcePlan,
        },
        result::Result,
        store::GStore,
    },
    itertools::Itertools,
    std::{borrow::Cow, collections::HashMap, rc::Rc},
};

type JoinItem<'a> = Rc<RowContext<'a>>;
type JoinedColumns<'a> = Vec<(&'a str, Rc<[String]>)>;
type JoinColumns<'a> = (&'a str, Rc<[String]>, JoinedColumns<'a>);

struct LeftOuter<'a> {
    rows: SelectedRows<'a>,
    init: Option<JoinItem<'a>>,
    matched: bool,
}

impl<'a> LeftOuter<'a> {
    fn new(rows: SelectedRows<'a>, init: JoinItem<'a>) -> Self {
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

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    plan: &'a JoinPlan,
    filter_context: Option<&Rc<RowContext<'a>>>,
) -> Result<SelectedRows<'a>> {
    let rows = match &plan.input {
        JoinInputPlan::Source(source) => {
            source_node::execute(storage, source, None)?.into_selected(None)
        }
        JoinInputPlan::Join(join) => execute(storage, join, filter_context)?,
    };

    join(storage, filter_context.cloned(), plan, rows)
}

fn join<'a, T: GStore>(
    storage: &'a T,
    filter_context: Option<Rc<RowContext<'a>>>,
    join_plan: &'a JoinPlan,
    left_rows: SelectedRows<'a>,
) -> Result<SelectedRows<'a>> {
    let JoinPlan {
        right,
        join_operator,
        join_executor,
        ..
    } = join_plan;

    let table_alias = right.alias_name();
    let join_executor = JoinExecutor::new(storage, right, filter_context.as_ref(), join_executor)?;

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

    let columns = source_node::columns(storage, right)?;
    let rows = left_rows.flat_map(move |project_context| {
        let project_context = match project_context {
            Ok(project_context) => project_context,
            Err(error) => return Box::new(std::iter::once(Err(error))) as SelectedRows<'a>,
        };

        let init_context = {
            let columns = Rc::clone(&columns);
            let init_row = Row {
                values: columns.iter().map(|_| Value::Null).collect(),
                columns,
            };

            Rc::new(RowContext::new(
                table_alias,
                Cow::Owned(init_row),
                Some(Rc::clone(&project_context)),
            ))
        };

        let row_filter_context = match filter_context.as_ref() {
            Some(filter_context) => Rc::new(RowContext::concat(
                Rc::clone(&project_context),
                Rc::clone(filter_context),
            )),
            None => Rc::clone(&project_context),
        };
        let row_filter_context = Some(row_filter_context);

        let rows: SelectedRows<'a> = match &join_executor {
            JoinExecutor::NestedLoop => {
                let rows = match source_node::execute(storage, right, row_filter_context.as_ref()) {
                    Ok(source) => source.rows,
                    Err(error) => {
                        return Box::new(std::iter::once(Err(error))) as SelectedRows<'a>;
                    }
                };
                Box::new(rows.filter_map(move |row| {
                    let row = match row {
                        Ok(row) => row,
                        Err(error) => return Some(Err(error)),
                    };

                    match check_where_clause(
                        storage,
                        table_alias,
                        row_filter_context.as_ref().map(Rc::clone),
                        Some(Rc::clone(&project_context)),
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
                    Err(error) => {
                        return Box::new(std::iter::once(Err(error))) as SelectedRows<'a>;
                    }
                };

                match rows {
                    Some(rows) => {
                        let rows =
                            rows.clone().into_iter().filter_map(
                                move |row| match check_where_clause(
                                    storage,
                                    table_alias,
                                    row_filter_context.as_ref().map(Rc::clone),
                                    Some(Rc::clone(&project_context)),
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
        source: &'a SourcePlan,
        filter_context: Option<&Rc<RowContext<'a>>>,
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
        let source_rows = source_node::execute(storage, source, filter_context)?;
        for row in source_rows.rows {
            let row = row?;
            let filter_context = Rc::new(RowContext::new(
                source.alias_name(),
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

pub(super) fn columns<'a, T: GStore>(storage: &T, join: &'a JoinPlan) -> Result<JoinColumns<'a>> {
    let (alias, columns, mut joined) = match &join.input {
        JoinInputPlan::Source(source) => (
            source.alias_name(),
            source_node::columns(storage, source)?,
            Vec::new(),
        ),
        JoinInputPlan::Join(join) => columns(storage, join)?,
    };
    joined.push((
        join.right.alias_name(),
        source_node::columns(storage, &join.right)?,
    ));

    Ok((alias, columns, joined))
}

fn check_where_clause<'a, T: GStore>(
    storage: &'a T,
    table_alias: &'a str,
    filter_context: Option<Rc<RowContext<'a>>>,
    project_context: Option<Rc<RowContext<'a>>>,
    where_clause: Option<&'a ExprPlan>,
    row: Cow<'_, Row>,
) -> Result<Option<Rc<RowContext<'a>>>> {
    let filter_context = RowContext::new(table_alias, Cow::Borrowed(&row), filter_context);
    let filter_context = Some(Rc::new(filter_context));

    match where_clause {
        Some(expr) => check_expr(storage, filter_context.as_ref(), None, expr)?,
        None => true,
    }
    .then(|| RowContext::new(table_alias, Cow::Owned(row.into_owned()), project_context))
    .map(Rc::new)
    .map(Ok)
    .transpose()
}
