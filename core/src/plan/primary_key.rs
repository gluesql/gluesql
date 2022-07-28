use {
    super::context::Context,
    crate::{
        ast::{Query, Select, SetExpr, Statement},
        data::Schema,
    },
    std::{collections::HashMap, rc::Rc},
};

pub fn plan(schema_map: &HashMap<String, Schema>, statement: Statement) -> Statement {
    let planner = Planner {
        _schema_map: schema_map,
    };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, *query);

            Statement::Query(Box::new(query))
        }
        _ => statement,
    }
}

struct Planner<'a> {
    _schema_map: &'a HashMap<String, Schema>,
}

impl<'a> Planner<'a> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, mut query: Query) -> Query {
        query.body = match query.body {
            SetExpr::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExpr::Select(Box::new(select))
            }
            SetExpr::Values(_) => query.body,
        };

        query
    }

    fn select(&self, _outer_context: Option<Rc<Context<'a>>>, select: Select) -> Select {
        select
    }
}
