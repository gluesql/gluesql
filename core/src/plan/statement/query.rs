mod limit;
mod offset;
mod order_by_expr;
mod select;
mod values;

pub use {
    limit::{LimitInputPlan, LimitPlan},
    offset::{OffsetInputPlan, OffsetPlan},
    order_by_expr::OrderByExprPlan,
    select::{SelectOrderByPlan, SelectPlan},
    values::{ValuesOrderByPlan, ValuesPlan},
};

use {
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryPlan {
    Select(Box<SelectPlan>),
    Values(ValuesPlan),
    SelectOrderBy(SelectOrderByPlan),
    ValuesOrderBy(ValuesOrderByPlan),
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

        let order_by = order_by.into_iter().map(Into::into).collect::<Vec<_>>();

        match body {
            ast::SetExpr::Select(select) => {
                let input = Box::new((*select).into());

                match (order_by.is_empty(), offset, limit) {
                    (true, None, None) => Self::Select(input),
                    (false, None, None) => Self::SelectOrderBy(SelectOrderByPlan {
                        input,
                        exprs: order_by,
                    }),
                    (true, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Select(input),
                        count: offset.into(),
                    }),
                    (false, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::SelectOrderBy(SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: offset.into(),
                    }),
                    (true, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Select(input),
                        count: limit.into(),
                    }),
                    (false, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::SelectOrderBy(SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: limit.into(),
                    }),
                    (true, Some(offset), Some(limit)) => {
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Select(input),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (false, Some(offset), Some(limit)) => {
                        let order_by = SelectOrderByPlan {
                            input,
                            exprs: order_by,
                        };
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::SelectOrderBy(order_by),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                }
            }
            ast::SetExpr::Values(values) => {
                let input = values.into();

                match (order_by.is_empty(), offset, limit) {
                    (true, None, None) => Self::Values(input),
                    (false, None, None) => Self::ValuesOrderBy(ValuesOrderByPlan {
                        input,
                        exprs: order_by,
                    }),
                    (true, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::Values(input),
                        count: offset.into(),
                    }),
                    (false, Some(offset), None) => Self::Offset(OffsetPlan {
                        input: OffsetInputPlan::ValuesOrderBy(ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: offset.into(),
                    }),
                    (true, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::Values(input),
                        count: limit.into(),
                    }),
                    (false, None, Some(limit)) => Self::Limit(LimitPlan {
                        input: LimitInputPlan::ValuesOrderBy(ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        }),
                        count: limit.into(),
                    }),
                    (true, Some(offset), Some(limit)) => {
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::Values(input),
                            count: offset.into(),
                        };

                        Self::Limit(LimitPlan {
                            input: LimitInputPlan::Offset(offset),
                            count: limit.into(),
                        })
                    }
                    (false, Some(offset), Some(limit)) => {
                        let order_by = ValuesOrderByPlan {
                            input,
                            exprs: order_by,
                        };
                        let offset = OffsetPlan {
                            input: OffsetInputPlan::ValuesOrderBy(order_by),
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
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan, QueryPlan},
        crate::{
            ast::Literal,
            parse_sql::parse,
            plan::{ExprPlan, SelectOrderByPlan, StatementPlan, ValuesOrderByPlan},
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
            StatementPlan::Query(QueryPlan::Select(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id"),
            StatementPlan::Query(QueryPlan::SelectOrderBy(_))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Select(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Select(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::SelectOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::SelectOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                count: ExprPlan::Literal(Literal::Number(limit)),
                input: LimitInputPlan::Offset(OffsetPlan {
                    count: ExprPlan::Literal(Literal::Number(offset)),
                    input: OffsetInputPlan::Select(_),
                }),
            })) if limit == 3 && offset == 2
        ));
        assert!(matches!(
            statement_plan("SELECT * FROM Item ORDER BY id LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::SelectOrderBy(SelectOrderByPlan { .. }),
                    ..
                }),
                ..
            }))
        ));
    }

    #[test]
    fn query_plan_preserves_values_terminal_stage_relations() {
        assert!(matches!(
            statement_plan("VALUES (1)"),
            StatementPlan::Query(QueryPlan::Values(_))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1"),
            StatementPlan::Query(QueryPlan::ValuesOrderBy(_))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::Values(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Offset(OffsetPlan {
                input: OffsetInputPlan::ValuesOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Values(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 LIMIT 3"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::ValuesOrderBy(_),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::Values(_),
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            statement_plan("VALUES (1) ORDER BY column1 LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: OffsetInputPlan::ValuesOrderBy(ValuesOrderByPlan { .. }),
                    ..
                }),
                ..
            }))
        ));
    }
}
