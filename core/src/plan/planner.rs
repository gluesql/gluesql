use {
    super::context::Context,
    crate::{
        ast::{ColumnDef, Expr, Function, Query, Subscript, TableAlias, TableFactor},
        data::Schema,
    },
    std::rc::Rc,
};

pub trait Planner<'a> {
    fn get_schema(&self, name: &str) -> Option<&'a Schema>;

    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: Query) -> Query;

    fn subquery_expr(&self, outer_context: Option<Rc<Context<'a>>>, expr: Expr) -> Expr {
        match expr {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier { .. }
            | Expr::Literal(_)
            | Expr::TypedString { .. } => expr,
            Expr::IsNull(expr) => Expr::IsNull(Box::new(self.subquery_expr(outer_context, *expr))),
            Expr::IsNotNull(expr) => {
                Expr::IsNotNull(Box::new(self.subquery_expr(outer_context, *expr)))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let list = list
                    .into_iter()
                    .map(|expr| self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr))
                    .collect();
                let expr = Box::new(self.subquery_expr(outer_context, *expr));

                Expr::InList {
                    expr,
                    list,
                    negated,
                }
            }
            Expr::Subquery(query) => Expr::Subquery(Box::new(self.query(outer_context, *query))),
            Expr::Exists { subquery, negated } => Expr::Exists {
                subquery: Box::new(self.query(outer_context, *subquery)),
                negated,
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let subquery = Box::new(self.query(outer_context, *subquery));

                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let low = Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *low));
                let high = Box::new(self.subquery_expr(outer_context, *high));

                Expr::Between {
                    expr,
                    negated,
                    low,
                    high,
                }
            }
            Expr::Like {
                expr,
                negated,
                pattern,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let pattern =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *pattern));

                Expr::Like {
                    expr,
                    negated,
                    pattern,
                }
            }
            Expr::ILike {
                expr,
                negated,
                pattern,
            } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let pattern =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *pattern));

                Expr::ILike {
                    expr,
                    negated,
                    pattern,
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *left)),
                op,
                right: Box::new(self.subquery_expr(outer_context, *right)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(self.subquery_expr(outer_context, *expr)),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.subquery_expr(outer_context, *expr))),
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let operand = operand.map(|expr| {
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr))
                });
                let when_then = when_then
                    .into_iter()
                    .map(|(when, then)| {
                        let when = self.subquery_expr(outer_context.as_ref().map(Rc::clone), when);
                        let then = self.subquery_expr(outer_context.as_ref().map(Rc::clone), then);

                        (when, then)
                    })
                    .collect();
                let else_result =
                    else_result.map(|expr| Box::new(self.subquery_expr(outer_context, *expr)));

                Expr::Case {
                    operand,
                    when_then,
                    else_result,
                }
            }
            Expr::Subscript { expr, subscript } => {
                let expr =
                    Box::new(self.subquery_expr(outer_context.as_ref().map(Rc::clone), *expr));
                let subscript = Box::new(match subscript.as_ref() {
                    Subscript::Index { index } => Subscript::Index {
                        index: self.subquery_expr(outer_context, index.clone()),
                    },
                    Subscript::Slice {
                        lower_bound,
                        upper_bound,
                        stride,
                    } => Subscript::Slice {
                        lower_bound: lower_bound.as_ref().map(|expr| {
                            self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr.clone())
                        }),
                        upper_bound: upper_bound.as_ref().map(|expr| {
                            self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr.clone())
                        }),
                        stride: stride.as_ref().map(|expr| {
                            self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr.clone())
                        }),
                    },
                });

                Expr::Subscript { expr, subscript }
            }
            Expr::Array { elem } => {
                let elem = elem
                    .into_iter()
                    .map(|expr| self.subquery_expr(outer_context.as_ref().map(Rc::clone), expr))
                    .collect();
                Expr::Array { elem }
            }
            Expr::Interval {
                expr,
                leading_field,
                last_field,
            } => Expr::Interval {
                expr: Box::new(self.subquery_expr(outer_context, *expr)),
                leading_field,
                last_field,
            },
            Expr::Function(func) => match *func {
                Function::Cast { expr, data_type } => Expr::Function(Box::new(Function::Cast {
                    expr: self.subquery_expr(outer_context, expr),
                    data_type,
                })),
                Function::Extract { field, expr } => Expr::Function(Box::new(Function::Extract {
                    field,
                    expr: self.subquery_expr(outer_context, expr),
                })),
                _ => Expr::Function(func),
            },
            Expr::Aggregate(_) => expr,
        }
    }

    fn update_context(
        &self,
        next: Option<Rc<Context<'a>>>,
        table_factor: &TableFactor,
    ) -> Option<Rc<Context<'a>>> {
        let (name, alias) = match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let alias = alias.as_ref().map(|TableAlias { name, .. }| name.clone());

                (name, alias)
            }
            TableFactor::Derived { .. }
            | TableFactor::Series { .. }
            | TableFactor::Dictionary { .. } => return next,
        };

        let schema = self.get_schema(name);

        let columns = match schema.as_ref().and_then(|Schema { column_defs, .. }| {
            column_defs.as_ref().map(|column_defs| {
                column_defs
                    .iter()
                    .map(|ColumnDef { name, .. }| name.as_str())
                    .collect::<Vec<_>>()
            })
        }) {
            Some(columns) => columns,
            None => return next,
        };

        let primary_key = schema
            .as_ref()
            .and_then(|schema| Some(schema.primary_key_column_names()?.collect()));

        let context = Context::new(
            alias.unwrap_or_else(|| name.to_owned()),
            columns,
            primary_key,
            next,
        );
        Some(Rc::new(context))
    }
}
