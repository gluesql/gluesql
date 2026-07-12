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

    fn query_plan(sql: &str) -> QueryPlan {
        let statement = parse(sql)
            .and_then(|mut statements| translate(&statements.remove(0)))
            .map(StatementPlan::from)
            .unwrap();

        let StatementPlan::Query(query) = statement else {
            panic!("expected query plan");
        };

        query
    }

    #[test]
    fn query_plan_wraps_only_present_limit_and_offset() {
        assert!(matches!(
            query_plan("SELECT * FROM Item"),
            QueryPlan::Body(_)
        ));
        assert!(matches!(
            query_plan("SELECT * FROM Item LIMIT 3"),
            QueryPlan::Limit(LimitPlan { input, .. })
                if matches!(input, LimitInputPlan::Body(_))
        ));
        assert!(matches!(
            query_plan("SELECT * FROM Item OFFSET 2"),
            QueryPlan::Offset(_)
        ));

        let QueryPlan::Limit(LimitPlan {
            count: limit,
            input,
        }) = query_plan("SELECT * FROM Item LIMIT 3 OFFSET 2")
        else {
            panic!("expected limit plan");
        };
        let LimitInputPlan::Offset(OffsetPlan {
            count: offset,
            input,
        }) = input
        else {
            panic!("expected offset plan");
        };
        let QueryBodyPlan { body, .. } = input;

        assert!(matches!(body, SetExprPlan::Select(_)));
        assert_eq!(limit, ExprPlan::Literal(Literal::Number(3.into())));
        assert_eq!(offset, ExprPlan::Literal(Literal::Number(2.into())));
    }
}
