mod body;
mod limit;
mod offset;

pub use {
    body::{OrderByExprPlan, QueryBodyPlan, SelectPlan, SetExprPlan, ValuesPlan},
    limit::{LimitInputPlan, LimitPlan},
    offset::OffsetPlan,
};

use {
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryPlan {
    Body(QueryBodyPlan),
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

        let body = QueryBodyPlan {
            body: body.into(),
            order_by: order_by.into_iter().map(Into::into).collect(),
        };

        match (offset, limit) {
            (None, None) => Self::Body(body),
            (Some(offset), None) => Self::Offset(OffsetPlan {
                input: body,
                count: offset.into(),
            }),
            (None, Some(limit)) => Self::Limit(LimitPlan {
                input: LimitInputPlan::Body(body),
                count: limit.into(),
            }),
            (Some(offset), Some(limit)) => {
                let offset = OffsetPlan {
                    input: body,
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

#[cfg(test)]
mod tests {
    use {
        super::{LimitInputPlan, LimitPlan, OffsetPlan, QueryBodyPlan, QueryPlan, SetExprPlan},
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
    fn query_plan_wraps_only_present_limit_and_offset() {
        assert!(matches!(
            statement_plan("SELECT * FROM Item"),
            StatementPlan::Query(QueryPlan::Body(_))
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
            statement_plan("SELECT * FROM Item LIMIT 3 OFFSET 2"),
            StatementPlan::Query(QueryPlan::Limit(LimitPlan {
                count: ExprPlan::Literal(Literal::Number(limit)),
                input: LimitInputPlan::Offset(OffsetPlan {
                    count: ExprPlan::Literal(Literal::Number(offset)),
                    input: QueryBodyPlan {
                        body: SetExprPlan::Select(_),
                        ..
                    },
                }),
            })) if limit == 3 && offset == 2
        ));
    }
}
