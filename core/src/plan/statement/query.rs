mod limit;
mod offset;
mod order_by;
mod set_expr;

pub use {
    limit::{LimitInputPlan, LimitPlan},
    offset::{OffsetInputPlan, OffsetPlan},
    order_by::{OrderByExprPlan, OrderByPlan},
    set_expr::{SelectPlan, SetExprPlan, ValuesPlan},
};

use {
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryPlan {
    Body(SetExprPlan),
    OrderBy(OrderByPlan),
    Offset(OffsetPlan),
    Limit(LimitPlan),
}

impl From<ast::Query> for QueryPlan {
    fn from(query: ast::Query) -> Self {
        let ast::Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let body = body.into();
        let order_by = order_by.into_iter().map(Into::into).collect::<Vec<_>>();

        match (order_by.is_empty(), offset, limit) {
            (true, None, None) => Self::Body(body),
            (false, None, None) => Self::OrderBy(OrderByPlan {
                input: body,
                exprs: order_by,
            }),
            (true, Some(offset), None) => Self::Offset(OffsetPlan {
                input: OffsetInputPlan::Body(body),
                count: offset.into(),
            }),
            (false, Some(offset), None) => Self::Offset(OffsetPlan {
                input: OffsetInputPlan::OrderBy(OrderByPlan {
                    input: body,
                    exprs: order_by,
                }),
                count: offset.into(),
            }),
            (true, None, Some(limit)) => Self::Limit(LimitPlan {
                input: LimitInputPlan::Body(body),
                count: limit.into(),
            }),
            (false, None, Some(limit)) => Self::Limit(LimitPlan {
                input: LimitInputPlan::OrderBy(OrderByPlan {
                    input: body,
                    exprs: order_by,
                }),
                count: limit.into(),
            }),
            (true, Some(offset), Some(limit)) => {
                let offset = OffsetPlan {
                    input: OffsetInputPlan::Body(body),
                    count: offset.into(),
                };

                Self::Limit(LimitPlan {
                    input: LimitInputPlan::Offset(offset),
                    count: limit.into(),
                })
            }
            (false, Some(offset), Some(limit)) => {
                let order_by = OrderByPlan {
                    input: body,
                    exprs: order_by,
                };
                let offset = OffsetPlan {
                    input: OffsetInputPlan::OrderBy(order_by),
                    count: offset.into(),
                };

                Self::Limit(LimitPlan {
                    input: LimitInputPlan::Offset(offset),
                    count: limit.into(),
                })
            }
        }
    }
}

impl QueryPlan {
    pub fn body(&self) -> &SetExprPlan {
        match self {
            Self::Body(body) => body,
            Self::OrderBy(OrderByPlan { input, .. }) => input,
            Self::Offset(OffsetPlan { input, .. }) => match input {
                OffsetInputPlan::Body(body) => body,
                OffsetInputPlan::OrderBy(OrderByPlan { input, .. }) => input,
            },
            Self::Limit(LimitPlan { input, .. }) => match input {
                LimitInputPlan::Body(body) => body,
                LimitInputPlan::OrderBy(OrderByPlan { input, .. }) => input,
                LimitInputPlan::Offset(OffsetPlan { input, .. }) => match input {
                    OffsetInputPlan::Body(body) => body,
                    OffsetInputPlan::OrderBy(OrderByPlan { input, .. }) => input,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan, OrderByPlan, QueryPlan,
            SetExprPlan,
        },
        crate::{
            ast::Literal,
            parse_sql::parse,
            plan::{ExprPlan, StatementPlan},
            translate::translate,
        },
    };

    fn statement_plan(sql: &str) -> StatementPlan {
        parse(sql)
            .and_then(|mut statements| translate(&statements.remove(0)))
            .map(StatementPlan::from)
            .unwrap()
    }

    #[test]
    fn query_plan_wraps_only_present_terminal_stages() {
        assert!(matches!(
            statement_plan("SELECT * FROM Item"),
            StatementPlan::Query(QueryPlan::Body(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id"),
            StatementPlan::Query(QueryPlan::OrderBy(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan { input, .. }))
                if matches!(input, LimitInputPlan::Body(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::OrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::OrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                count: ExprPlan::Literal(Literal::Number(limit)),
                input: LimitInputPlan::Offset(OffsetPlan {
                    count: ExprPlan::Literal(Literal::Number(offset)),
                    input: OffsetInputPlan::Body(SetExprPlan::Select(_)),
                }),
            })) if limit == 3 && offset == 2
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::OrderBy(OrderByPlan {
                        input: SetExprPlan::Select(_),
                        ..
                    }),
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id OFFSET 2"),
            StatementPlan::Query(query) if matches!(query.body(), SetExprPlan::Select(_))
        ));
    }
}
