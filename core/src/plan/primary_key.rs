use {
    super::{context::Context, evaluable::check_expr as check_evaluable, planner::Planner},
    crate::{
        ast::{BinaryOperator, Expr, IndexItem, Query, Select, SetExpr, Statement, TableFactor},
        data::Schema,
    },
    std::{collections::HashMap, rc::Rc},
};

pub fn plan(schema_map: &HashMap<String, Schema>, statement: Statement) -> Statement {
    let planner = PrimaryKeyPlanner { schema_map };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, query);

            Statement::Query(query)
        }
        _ => statement,
    }
}

struct PrimaryKeyPlanner<'a> {
    schema_map: &'a HashMap<String, Schema>,
}

impl<'a> Planner<'a> for PrimaryKeyPlanner<'a> {
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

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

impl<'a> PrimaryKeyPlanner<'a> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, mut select: Select) -> Select {
        let current_context = self.update_context(None, &select.from.relation);
        let outer_context = select
            .from
            .joins
            .iter()
            .fold(outer_context, |context, join| {
                self.update_context(context, &join.relation)
            });

        let check_primary_key = |key: &Expr| {
            let key = match key {
                Expr::Identifier(ident) => ident,
                Expr::CompoundIdentifier(idents) => &idents[1],
                _ => return false,
            };

            current_context
                .as_ref()
                .map(|context| context.contains_primary_key(key))
                .unwrap_or(false)
        };

        let (index, selection) = select
            .selection
            .map(|expr| match expr {
                Expr::BinaryOp {
                    left: key,
                    op: BinaryOperator::Eq,
                    right: value,
                }
                | Expr::BinaryOp {
                    left: value,
                    op: BinaryOperator::Eq,
                    right: key,
                } if check_primary_key(key.as_ref())
                    && check_evaluable(current_context.as_ref().map(Rc::clone), &key)
                    && check_evaluable(outer_context.as_ref().map(Rc::clone), &value) =>
                {
                    let index_item = IndexItem::PrimaryKey(*value);

                    (Some(index_item), None)
                }
                _ => {
                    let current_context = current_context.as_ref().map(Rc::clone);
                    let outer_context =
                        Some(Rc::new(Context::concat(current_context, outer_context)));

                    (None, Some(self.subquery_expr(outer_context, expr)))
                }
            })
            .unwrap_or((None, None));

        if let TableFactor::Table {
            name,
            alias,
            index: None,
        } = select.from.relation
        {
            select.from.relation = TableFactor::Table { name, alias, index };
        }

        select.selection = selection;
        select
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_primary_key,
        crate::{
            ast::Statement,
            parse_sql::parse,
            plan::{
                fetch_schema_map,
                mock::{run, MockStorage},
            },
            translate::translate,
        },
        futures::executor::block_on,
    };

    fn plan(storage: &MockStorage, sql: &str) -> Statement {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();

        plan_primary_key(&schema_map, statement)
    }

    #[test]
    fn basic() {
        let storage = run("
            CREATE TABLE User (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM User WHERE id = 1;";
        let actual = plan(&storage, sql);

        println!("{:#?}", actual);
        /*
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: table_factor("User", None),
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
        });
        assert_eq!(actual, expected, "basic select:\n{sql}");
        */
    }
}
