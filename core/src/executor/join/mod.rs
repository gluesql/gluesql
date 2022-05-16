mod hash_key;

use {
    self::hash_key::HashKey,
    crate::{
        ast::{
            Expr, Join as AstJoin, JoinConstraint, JoinExecutor as AstJoinExecutor,
            JoinOperator as AstJoinOperator,
        },
        data::{Row, Table},
        executor::{
            context::{BlendContext, FilterContext},
            evaluate::evaluate,
            filter::check_expr,
        },
        result::{Error, Result},
        store::GStore,
    },
    futures::{
        future,
        stream::{self, empty, once, Stream, StreamExt, TryStream, TryStreamExt},
    },
    itertools::Itertools,
    std::{borrow::Cow, collections::HashMap, pin::Pin, rc::Rc},
    utils::OrStream,
};

pub struct Join<'a, T> {
    storage: &'a dyn GStore<T>,
    join_clauses: &'a [AstJoin],
    join_columns: Vec<Rc<[String]>>,
    filter_context: Option<Rc<FilterContext<'a>>>,
}

type JoinItem<'a> = Rc<BlendContext<'a>>;
type Joined<'a> =
    Pin<Box<dyn TryStream<Ok = JoinItem<'a>, Error = Error, Item = Result<JoinItem<'a>>> + 'a>>;

impl<'a, T> Join<'a, T> {
    pub fn new(
        storage: &'a dyn GStore<T>,
        join_clauses: &'a [AstJoin],
        join_columns: Vec<Rc<[String]>>,
        filter_context: Option<Rc<FilterContext<'a>>>,
    ) -> Self {
        Self {
            storage,
            join_clauses,
            join_columns,
            filter_context,
        }
    }

    pub async fn apply(
        self,
        rows: impl Stream<Item = Result<BlendContext<'a>>> + 'a,
    ) -> Result<Joined<'a>> {
        let init_rows: Joined = Box::pin(rows.map(|row| row.map(Rc::new)));
        let joins = self
            .join_clauses
            .iter()
            .zip(self.join_columns.iter().map(Rc::clone));

        stream::iter(joins)
            .map(Ok)
            .try_fold(init_rows, |rows, (join_clause, join_columns)| {
                let filter_context = self.filter_context.as_ref().map(Rc::clone);

                async move {
                    join(
                        self.storage,
                        filter_context,
                        join_clause,
                        join_columns,
                        rows,
                    )
                    .await
                }
            })
            .await
    }
}

async fn join<'a, T>(
    storage: &'a dyn GStore<T>,
    filter_context: Option<Rc<FilterContext<'a>>>,
    ast_join: &'a AstJoin,
    columns: Rc<[String]>,
    left_rows: impl TryStream<Ok = JoinItem<'a>, Error = Error, Item = Result<JoinItem<'a>>> + 'a,
) -> Result<Joined<'a>> {
    let AstJoin {
        relation,
        join_operator,
        join_executor,
    } = ast_join;
    let table = Table::new(relation)?;
    let table_name = table.get_name();
    let table_alias = table.get_alias();
    let join_executor = JoinExecutor::new(
        storage,
        table_name,
        table_alias,
        Rc::clone(&columns),
        filter_context.as_ref().map(Rc::clone),
        join_executor,
    )
    .await
    .map(Rc::new)?;

    let (join_operator, where_clause) = match join_operator {
        AstJoinOperator::Inner(JoinConstraint::None) => (JoinOperator::Inner, None),
        AstJoinOperator::Inner(JoinConstraint::On(where_clause)) => {
            (JoinOperator::Inner, Some(where_clause))
        }
        AstJoinOperator::LeftOuter(JoinConstraint::None) => (JoinOperator::LeftOuter, None),
        AstJoinOperator::LeftOuter(JoinConstraint::On(where_clause)) => {
            (JoinOperator::LeftOuter, Some(where_clause))
        }
    };

    let rows = left_rows.and_then(move |blend_context| {
        let filter_context = filter_context.as_ref().map(Rc::clone);
        let columns = Rc::clone(&columns);
        let init_context = Rc::new(BlendContext::new(
            table.get_alias(),
            Rc::clone(&columns),
            None,
            Some(Rc::clone(&blend_context)),
        ));
        let join_executor = Rc::clone(&join_executor);

        async move {
            let filter_context = Some(Rc::new(FilterContext::concat(
                filter_context.as_ref().map(Rc::clone),
                Some(&blend_context).map(Rc::clone),
            )));

            #[derive(futures_enum::Stream)]
            enum Rows<I1, I2, I3> {
                NestedLoop(I1),
                Hash(I2),
                Empty(I3),
            }

            let rows = match join_executor.as_ref() {
                JoinExecutor::NestedLoop => {
                    let rows = storage
                        .scan_data(table_name)
                        .await
                        .map(stream::iter)?
                        .and_then(|(_, row)| future::ok(Cow::Owned(row)))
                        .try_filter_map(move |row| {
                            check_where_clause(
                                storage,
                                table_alias,
                                Rc::clone(&columns),
                                filter_context.as_ref().map(Rc::clone),
                                Some(&blend_context).map(Rc::clone),
                                where_clause,
                                row,
                            )
                        });

                    Rows::NestedLoop(rows)
                }
                JoinExecutor::Hash {
                    rows_map,
                    value_expr,
                } => {
                    let rows = evaluate(
                        storage,
                        filter_context.as_ref().map(Rc::clone),
                        None,
                        value_expr,
                    )
                    .await
                    .map(Option::<HashKey>::try_from)??
                    .and_then(|hash_key| rows_map.get(&hash_key));

                    match rows {
                        None => Rows::Empty(empty()),
                        Some(rows) => {
                            let rows = stream::iter(rows.iter().map(Cow::Borrowed).map(Ok));
                            let rows = rows.try_filter_map(move |row| {
                                check_where_clause(
                                    storage,
                                    table_alias,
                                    Rc::clone(&columns),
                                    filter_context.as_ref().map(Rc::clone),
                                    Some(&blend_context).map(Rc::clone),
                                    where_clause,
                                    row,
                                )
                            });
                            let rows = stream::iter(rows.collect::<Vec<_>>().await);

                            Rows::Hash(rows)
                        }
                    }
                }
            };

            let rows: Joined = match join_operator {
                JoinOperator::Inner => Box::pin(rows),
                JoinOperator::LeftOuter => {
                    let init_rows = once(async { Ok(init_context) });

                    Box::pin(OrStream::new(rows, init_rows))
                }
            };

            Ok(rows)
        }
    });

    Ok(Box::pin(rows.try_flatten()))
}

#[derive(Copy, Clone)]
enum JoinOperator {
    Inner,
    LeftOuter,
}

enum JoinExecutor<'a> {
    NestedLoop,
    Hash {
        rows_map: HashMap<HashKey, Vec<Row>>,
        value_expr: &'a Expr,
    },
}

impl<'a> JoinExecutor<'a> {
    async fn new<T>(
        storage: &'a dyn GStore<T>,
        table_name: &'a str,
        table_alias: &'a str,
        columns: Rc<[String]>,
        filter_context: Option<Rc<FilterContext<'a>>>,
        ast_join_executor: &'a AstJoinExecutor,
    ) -> Result<JoinExecutor<'a>> {
        let (key_expr, value_expr, where_clause) = match ast_join_executor {
            AstJoinExecutor::NestedLoop => return Ok(Self::NestedLoop),
            AstJoinExecutor::Hash {
                key_expr,
                value_expr,
                where_clause,
            } => (key_expr, value_expr, where_clause),
        };

        let rows_map = storage
            .scan_data(table_name)
            .await
            .map(stream::iter)?
            .try_filter_map(|(_, row)| {
                let columns = Rc::clone(&columns);
                let filter_context = filter_context.as_ref().map(Rc::clone);

                async move {
                    let filter_context = Rc::new(FilterContext::new(
                        table_alias,
                        columns,
                        Some(&row),
                        filter_context,
                    ));

                    let hash_key: Option<HashKey> = evaluate(
                        storage,
                        Some(&filter_context).map(Rc::clone),
                        None,
                        key_expr,
                    )
                    .await?
                    .try_into()?;

                    let hash_key = match hash_key {
                        Some(hash_key) => hash_key,
                        None => return Ok(None),
                    };

                    match where_clause {
                        Some(expr) => check_expr(storage, Some(filter_context), None, expr)
                            .await
                            .map(|pass| pass.then(|| (hash_key, row))),
                        None => Ok(Some((hash_key, row))),
                    }
                }
            })
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .into_group_map();

        Ok(Self::Hash {
            rows_map,
            value_expr,
        })
    }
}

async fn check_where_clause<'a, 'b, T>(
    storage: &'a dyn GStore<T>,
    table_alias: &'a str,
    columns: Rc<[String]>,
    filter_context: Option<Rc<FilterContext<'a>>>,
    blend_context: Option<Rc<BlendContext<'a>>>,
    where_clause: Option<&'a Expr>,
    row: Cow<'b, Row>,
) -> Result<Option<Rc<BlendContext<'a>>>> {
    let filter_context =
        FilterContext::new(table_alias, Rc::clone(&columns), Some(&row), filter_context);
    let filter_context = Some(Rc::new(filter_context));

    match where_clause {
        Some(expr) => check_expr(storage, filter_context, None, expr).await?,
        None => true,
    }
    .then(|| BlendContext::new(table_alias, columns, Some(row.into_owned()), blend_context))
    .map(Rc::new)
    .map(Ok)
    .transpose()
}
