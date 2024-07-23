//! Submodule providing the planner for handling primary keys.
//!
//! # Primary Key Planner
//! The primary goal of the primary key planner is to optimize the query by using the primary key
//! whenever possible. This is done by checking if the primary key is used in the query, for instance
//! as part of the WHERE clause. If the primary key is used, the planner will remove the primary key
//! from the WHERE clause and move it into the index field. This will allow the query to be executed
//! more efficiently by calling directly the Store::fetch_data instead of the Store::scan_data.
//!

use {
    super::{context::Context, evaluable::check_expr as check_evaluable, planner::Planner},
    crate::{
        ast::{
            BinaryOperator, Expr, IndexItem, Query, Select, SetExpr, Statement, TableFactor,
            TableWithJoins,
        },
        data::Schema,
    },
    std::{collections::HashMap, rc::Rc},
};

/// Plan the statement by optimizing the query using the primary key.
///
/// # Arguments
/// * `schema_map` - A map of schema names to schema definitions.
/// * `statement` - The statement to plan.
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

/// Planner for optimizing the query using the primary key.
struct PrimaryKeyPlanner<'a> {
    schema_map: &'a HashMap<String, Schema>,
}

impl<'a> Planner<'a> for PrimaryKeyPlanner<'a> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: Query) -> Query {
        let body = match query.body {
            SetExpr::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExpr::Select(Box::new(select))
            }
            SetExpr::Values(_) => query.body,
        };

        Query { body, ..query }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

#[derive(Debug)]
struct Partial {
    /// Vector where we store the index items we have identified
    /// so far, where in each i-th position we set the value for
    /// the curresponding primary key column as determined by the
    /// order of the columns in the table schema. The vector is
    /// initialized with None values for all columns, and we set
    /// the values as we explore the WHERE clause. If we find all
    /// the primary key columns, we can convert the Partial into
    /// a Found variant.
    index_items: Vec<Option<Expr>>,
    /// The underlying expression that we are refactoring to remove
    /// the primary key from the WHERE clause.
    refactored_expr: Option<Expr>,
    /// The backup of the original expression that we are refactoring,
    /// in case we need to revert the refactoring as the Partial primary
    /// key is never completed to a Found primary key.
    original_expr: Expr,
}

#[derive(Debug)]
enum PrimaryKey {
    /// The primary key was found in the WHERE clause.
    Found {
        /// The primary key index item.
        index_item: IndexItem,
        /// The underlying expression that we are refactoring to remove
        /// the primary key from the WHERE clause.
        refactored_expr: Option<Expr>,
    },
    /// Part of a compound primary key was found in the WHERE clause,
    /// but not all parts were found yet.
    Partial(Partial),
    /// The primary key was not found in the WHERE clause.
    NotFound(Expr),
}

impl From<Partial> for PrimaryKey {
    fn from(partial: Partial) -> Self {
        PrimaryKey::Partial(partial)
    }
}

impl PrimaryKey {
    /// Creates a new Partial variant with the provided number of columns.
    ///
    /// # Arguments
    /// * `value` - The value of the primary key.
    /// * `index` - The index of the primary key column.
    /// * `columns` - The number of columns in the primary key.
    /// * `original_expr` - The original expression that we are refactoring.
    fn new_partial(value: Expr, index: usize, columns: usize, original_expr: Expr) -> PrimaryKey {
        let mut index_items = vec![None; columns];
        index_items[index] = Some(value);

        Partial {
            index_items,
            refactored_expr: None,
            original_expr,
        }
        .into()
    }

    /// Creates a new Found variant from the provided value.
    fn new_found(value: Expr) -> PrimaryKey {
        PrimaryKey::Found {
            index_item: IndexItem::PrimaryKey(vec![value]),
            refactored_expr: None,
        }
    }
}

impl<'a> PrimaryKeyPlanner<'a> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, select: Select) -> Select {
        let current_context = self.update_context(None, &select.from.relation);
        let current_context = select
            .from
            .joins
            .iter()
            .fold(current_context, |context, join| {
                self.update_context(context, &join.relation)
            });

        let (index, selection) = select
            .selection
            .map(|expr| match current_context.as_ref() {
                Some(current_context) => {
                    let result = self.expr(outer_context, current_context, expr);
                    match result {
                        PrimaryKey::Found {
                            index_item,
                            refactored_expr,
                        } => (Some(index_item), refactored_expr),
                        PrimaryKey::NotFound(expr)
                        | PrimaryKey::Partial(Partial {
                            original_expr: expr,
                            ..
                        }) => (None, Some(expr)),
                    }
                }
                None => (None, Some(expr)),
            })
            .unwrap_or((None, None));

        if let TableFactor::Table {
            name,
            alias,
            index: None,
        } = select.from.relation
        {
            let from = TableWithJoins {
                relation: TableFactor::Table { name, alias, index },
                ..select.from
            };

            Select {
                selection,
                from,
                ..select
            }
        } else {
            Select {
                selection,
                ..select
            }
        }
    }

    fn expr(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        current_context: &Rc<Context<'a>>,
        expr: Expr,
    ) -> PrimaryKey {
        enum PossibleResults {
            Matched(PrimaryKey),
            Retry(Box<Expr>, Box<Expr>),
        }

        // Returns the primary key variant associated to the provided key and value, if any.
        let get_primary_key = |key: Box<Expr>, value: Box<Expr>| {
            if !(check_evaluable(Some(current_context.clone()), &key)
                && check_evaluable(None, &value))
            {
                return PossibleResults::Retry(key, value);
            }

            let key_column: &str = if let Expr::Identifier(ident)
            | Expr::CompoundIdentifier { ident, .. } = key.as_ref()
            {
                ident
            } else {
                return PossibleResults::Retry(key, value);
            };

            if let Some(index) = current_context.get_primary_key_index_by_name(key_column) {
                let number_of_primary_key_columns = current_context.number_of_primary_key_columns();
                PossibleResults::Matched(if number_of_primary_key_columns == 1 {
                    // If we have a single primary key column, we can directly create a Found primary key.
                    PrimaryKey::new_found(*value)
                } else {
                    // Otherwise, we create a Partial primary key.
                    PrimaryKey::new_partial(
                        *value.clone(),
                        index,
                        number_of_primary_key_columns,
                        Expr::BinaryOp {
                            left: key,
                            op: BinaryOperator::Eq,
                            right: value,
                        },
                    )
                })
            } else {
                PossibleResults::Retry(key, value)
            }
        };

        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => match get_primary_key(left, right) {
                PossibleResults::Matched(primary_key) => primary_key,
                PossibleResults::Retry(left, right) => match get_primary_key(right, left) {
                    PossibleResults::Matched(primary_key) => primary_key,
                    PossibleResults::Retry(right, left) => {
                        PrimaryKey::NotFound(self.subquery_expr(
                            Context::concat(Some(current_context.clone()), outer_context),
                            Expr::BinaryOp {
                                left,
                                op: BinaryOperator::Eq,
                                right,
                            },
                        ))
                    }
                },
            },
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                match self.expr(
                    outer_context.as_ref().map(Rc::clone),
                    current_context,
                    *left,
                ) {
                    PrimaryKey::Found {
                        index_item,
                        refactored_expr: refactored_left_expr,
                    } => PrimaryKey::Found {
                        index_item,
                        refactored_expr: match refactored_left_expr {
                            Some(refactored_left_expr) => Some(Expr::BinaryOp {
                                left: Box::new(refactored_left_expr),
                                op: BinaryOperator::And,
                                right,
                            }),
                            None => Some(*right),
                        },
                    },
                    // If the search of the left branch has determined part of a primary key, we
                    // need to explore whether the right branch can complete the primary key. An
                    // example of this case is, given a table with a composite primary key (user_id, comment_id),
                    // a WHERE statement such as `user_id = 1 AND comment_id = 2`. In such a case
                    // the left branch will determine the user_id, and the right branch will determine
                    // the comment_id.
                    PrimaryKey::Partial(Partial {
                        index_items: left_index_items,
                        refactored_expr: left_refactored_expr,
                        original_expr: left_original_expr,
                    }) => {
                        match self.expr(outer_context, current_context, *right) {
                            // If we have found a complete primary key in the right branch, we must
                            // give priority to the refactored expression obtained from the right branch
                            // and use only the original expression from the left branch.
                            PrimaryKey::Found {
                                index_item,
                                refactored_expr: refactored_right_expr,
                            } => PrimaryKey::Found {
                                index_item,
                                refactored_expr: match refactored_right_expr {
                                    Some(refactored_right_expr) => Some(Expr::BinaryOp {
                                        left: Box::new(left_original_expr),
                                        op: BinaryOperator::And,
                                        right: Box::new(refactored_right_expr),
                                    }),
                                    None => Some(left_original_expr),
                                },
                            },
                            // If we have found part of a primary key in the right branch, we must
                            // merge the index items from the left branch with the index items from the
                            // right branch. In some instances, we may encounter cases where the same
                            // primary key column is set in both the left branch and the right branch. In
                            // such cases, we must give priority to the value set in the left branch,
                            // and leave as-is the right branch.
                            PrimaryKey::Partial(Partial {
                                index_items: right_index_items,
                                refactored_expr: right_refactored_expr,
                                original_expr: right_original_expr,
                            }) => {
                                // First, we check whether the two set of index items overlap.
                                if left_index_items
                                    .iter()
                                    .zip(right_index_items.iter())
                                    .any(|(left, right)| left.is_some() && right.is_some())
                                {
                                    // If all of the elements of the smaller set as entirely contained
                                    // by the larger set, we keep the larger set and discard the smaller set.
                                    let number_of_left_elements = left_index_items
                                        .iter()
                                        .filter(|item| item.is_some())
                                        .count();
                                    let number_of_right_elements = right_index_items
                                        .iter()
                                        .filter(|item| item.is_some())
                                        .count();

                                    let left_partial = Partial {
                                        index_items: left_index_items,
                                        refactored_expr: left_refactored_expr,
                                        original_expr: left_original_expr,
                                    };
                                    let right_partial = Partial {
                                        index_items: right_index_items,
                                        refactored_expr: right_refactored_expr,
                                        original_expr: right_original_expr,
                                    };

                                    let (smaller_partial, larger_partial) =
                                        if number_of_left_elements < number_of_right_elements {
                                            (left_partial, right_partial)
                                        } else {
                                            (right_partial, left_partial)
                                        };

                                    // We merge the index items from the larger set with the index items
                                    // from the smaller set, giving priority to the larger set in case of
                                    // overlapping values.
                                    let merged_index_items = larger_partial
                                        .index_items
                                        .iter()
                                        .zip(smaller_partial.index_items)
                                        .map(|(larger, smaller)| {
                                            if larger.is_some() {
                                                larger.clone()
                                            } else {
                                                smaller
                                            }
                                        })
                                        .collect::<Vec<_>>();

                                    let refactored_expr = match larger_partial.refactored_expr {
                                        Some(larger_refactored_expr) => Some(Expr::BinaryOp {
                                            left: Box::new(larger_refactored_expr),
                                            op: BinaryOperator::And,
                                            right: Box::new(smaller_partial.original_expr.clone()),
                                        }),
                                        None => Some(smaller_partial.original_expr.clone()),
                                    };

                                    // We check whether the merged index items form a complete primary key.
                                    return if merged_index_items.iter().all(Option::is_some) {
                                        PrimaryKey::Found {
                                            index_item: IndexItem::PrimaryKey(
                                                merged_index_items
                                                    .into_iter()
                                                    .map(Option::unwrap)
                                                    .collect(),
                                            ),
                                            refactored_expr,
                                        }
                                    } else {
                                        Partial {
                                            index_items: merged_index_items,
                                            refactored_expr,
                                            original_expr: Expr::BinaryOp {
                                                left: Box::new(larger_partial.original_expr),
                                                op: BinaryOperator::And,
                                                right: Box::new(smaller_partial.original_expr),
                                            },
                                        }
                                        .into()
                                    };
                                }

                                // Otherwise, we merge the index items from the left branch with the index
                                // items from the right branch, knowing that there are no overlapping values.
                                let index_items: Vec<_> = left_index_items
                                    .into_iter()
                                    .zip(right_index_items)
                                    .map(|(left, right)| left.or(right))
                                    .collect();

                                // If we have identified all the primary key columns, we can convert the
                                // Partial primary key into a Found primary key.
                                if index_items.iter().all(Option::is_some) {
                                    return PrimaryKey::Found {
                                        index_item: IndexItem::PrimaryKey(
                                            index_items.into_iter().map(Option::unwrap).collect(),
                                        ),
                                        refactored_expr: match left_refactored_expr {
                                            Some(left_refactored_expr) => {
                                                match right_refactored_expr {
                                                    Some(right_refactored_expr) => {
                                                        Some(Expr::BinaryOp {
                                                            left: Box::new(left_refactored_expr),
                                                            op: BinaryOperator::And,
                                                            right: Box::new(right_refactored_expr),
                                                        })
                                                    }
                                                    None => Some(left_refactored_expr),
                                                }
                                            }
                                            None => right_refactored_expr,
                                        },
                                    };
                                }

                                // Otherwise, we return an extended Partial primary key.
                                Partial {
                                    index_items,
                                    refactored_expr: match left_refactored_expr {
                                        Some(left_refactored_expr) => match right_refactored_expr {
                                            Some(right_refactored_expr) => Some(Expr::BinaryOp {
                                                left: Box::new(left_refactored_expr),
                                                op: BinaryOperator::And,
                                                right: Box::new(right_refactored_expr),
                                            }),
                                            None => Some(left_refactored_expr),
                                        },
                                        None => right_refactored_expr,
                                    },
                                    original_expr: Expr::BinaryOp {
                                        left: Box::new(left_original_expr),
                                        op: BinaryOperator::And,
                                        right: Box::new(right_original_expr),
                                    },
                                }
                                .into()
                            }

                            // Otherwise, if we have not identified any primary key within the right branch,
                            // since we still have the partial match from the left branch, we need to reconstruct
                            // the original expression from the left branch and the right branch and return a
                            // partial primary key.
                            PrimaryKey::NotFound(right_expr) => Partial {
                                index_items: left_index_items,
                                refactored_expr: match left_refactored_expr {
                                    Some(left_refactored_expr) => Some(Expr::BinaryOp {
                                        left: Box::new(left_refactored_expr),
                                        op: BinaryOperator::And,
                                        right: Box::new(right_expr.clone()),
                                    }),
                                    None => Some(right_expr.clone()),
                                },
                                original_expr: Expr::BinaryOp {
                                    left: Box::new(left_original_expr),
                                    op: BinaryOperator::And,
                                    right: Box::new(right_expr),
                                },
                            }
                            .into(),
                        }
                    }
                    PrimaryKey::NotFound(left) => {
                        match self.expr(outer_context, current_context, *right) {
                            PrimaryKey::Found {
                                index_item,
                                refactored_expr,
                            } => PrimaryKey::Found {
                                index_item,
                                refactored_expr: match refactored_expr {
                                    Some(refactored_expr) => Some(Expr::BinaryOp {
                                        left: Box::new(left),
                                        op: BinaryOperator::And,
                                        right: Box::new(refactored_expr),
                                    }),
                                    None => Some(left),
                                },
                            },
                            PrimaryKey::Partial(Partial {
                                index_items,
                                refactored_expr: right_refactored_expr,
                                original_expr: right_original_expr,
                            }) => Partial {
                                index_items,
                                refactored_expr: match right_refactored_expr {
                                    Some(right_refactored_expr) => Some(Expr::BinaryOp {
                                        left: Box::new(left.clone()),
                                        op: BinaryOperator::And,
                                        right: Box::new(right_refactored_expr),
                                    }),
                                    None => Some(left.clone()),
                                },
                                original_expr: Expr::BinaryOp {
                                    left: Box::new(left),
                                    op: BinaryOperator::And,
                                    right: Box::new(right_original_expr),
                                },
                            }
                            .into(),
                            PrimaryKey::NotFound(right) => PrimaryKey::NotFound(Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::And,
                                right: Box::new(right),
                            }),
                        }
                    }
                }
            }
            Expr::Nested(expr) => match self.expr(outer_context, current_context, *expr) {
                PrimaryKey::Found {
                    index_item,
                    refactored_expr,
                } => PrimaryKey::Found {
                    index_item,
                    refactored_expr: refactored_expr.map(Box::new).map(Expr::Nested),
                },
                PrimaryKey::Partial(Partial {
                    index_items,
                    refactored_expr,
                    original_expr,
                }) => Partial {
                    index_items,
                    refactored_expr: refactored_expr.map(Box::new).map(Expr::Nested),
                    original_expr: Expr::Nested(Box::new(original_expr)),
                }
                .into(),
                PrimaryKey::NotFound(expr) => PrimaryKey::NotFound(Expr::Nested(Box::new(expr))),
            },
            expr => {
                let outer_context = Context::concat(Some(current_context.clone()), outer_context);
                PrimaryKey::NotFound(self.subquery_expr(outer_context, expr))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_primary_key,
        crate::{
            ast::{
                AstLiteral, BinaryOperator, Expr, IndexItem, Join, JoinConstraint, JoinExecutor,
                JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
                TableWithJoins, Values,
            },
            mock::{run, MockStorage},
            parse_sql::{parse, parse_expr},
            plan::fetch_schema_map,
            translate::{translate, translate_expr},
        },
        futures::executor::block_on,
    };

    fn plan(storage: &MockStorage, sql: &str) -> Statement {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = translate(&parsed).unwrap();
        let schema_map = block_on(fetch_schema_map(storage, &statement)).unwrap();

        plan_primary_key(&schema_map, statement)
    }

    fn select(select: Select) -> Statement {
        Statement::Query(Query {
            body: SetExpr::Select(Box::new(select)),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        })
    }

    fn expr(sql: &str) -> Expr {
        let parsed = parse_expr(sql).expect(sql);

        translate_expr(&parsed).expect(sql)
    }

    #[test]
    fn where_expr() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "primary key in lhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE 1 = id;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "primary key in rhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 AND True;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND id = 1
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 2:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND True
                AND id = 1;
        ";
        let actual = plan(&storage, sql);
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND (True AND id = 1);
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND (True)")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");
    }

    #[test]
    fn where_expr_multiple_primary_keys() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT,
                PRIMARY KEY (id, name)
            );
        ");

        let sql = "SELECT * FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "multiple primary key in lhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE 1 = id;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            // In this case the primary key is not used in the WHERE clause,
            // but the order of the equivalence is normalized to key = value
            // instead of value = key.
            selection: Some(expr("id = 1")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "primary key in rhs:\n{sql}");

        let sql = "SELECT * FROM Player WHERE id = 1 AND True;";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1 AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND with IS NOT NULL:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND id = 1
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND id = 1 AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 2:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND True
                AND id = 1;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND True AND id = 1")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name = 'Merlin'
                AND True
                AND id = 1;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1"), expr("'Merlin'")])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE
                name IS NOT NULL
                AND (True AND id = 1);
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("name IS NOT NULL AND (True AND id = 1)")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we test the case where the left branch yields a Partial, but the right branch
        // yields a complete primary key, without need to merge with the left branch.
        let sql = "
            SELECT * FROM Player
            WHERE
                id = 1 And True
                AND (name = 'Merlin' AND id = 2);
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("2"), expr("'Merlin'")])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1 AND True")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we test the case where the left branch yields a Partial, and the right branch
        // also yields a Partial, with the two branches overlapping on the same primary key column.
        let sql = "
            SELECT * FROM Player
            WHERE
                id = 1 And True
                AND id = 4;
        ";

        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1 AND True AND id = 4")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we create a new table with a composite primary key, containing a triple
        // of columns (id, name, age). We test two cases: one where the branches yield
        // two non-overlapping partial primary keys, but that are not sufficient to complete
        // the full primary key, and one where the branches yield three partial primary keys
        // that together form the full primary key.
        let storage = run("
            CREATE TABLE Player2 (
                id INTEGER,
                name TEXT,
                age INTEGER,
                PRIMARY KEY (id, name, age)
            );
        ");

        let sql = "
            SELECT * FROM Player2
            WHERE
                id = 1
                AND name = 'Merlin'
                AND True;
        ";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1 AND name = 'Merlin' AND True")),
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        let sql = "
            SELECT * FROM Player2
            WHERE
                id = 1
                AND name = 'Merlin'
                AND age = 42;
        ";

        let actual = plan(&storage, sql);

        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![
                        expr("1"),
                        expr("'Merlin'"),
                        expr("42"),
                    ])),
                },
                joins: Vec::new(),
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we explore the case where the left branch yields a Partial
        // and the right yields a Found, with also a refactored expression.
        let sql = "
            SELECT * FROM Player2
            WHERE
                id = 1
                AND (
                    id = 2 AND name = 'Merlin' and age = 56 AND True
                )
                AND id = 3;
        ";

        let actual = plan(&storage, sql);

        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![
                        expr("2"),
                        expr("'Merlin'"),
                        expr("56"),
                    ])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("id = 1 AND (True) AND id = 3")),
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we explore the case where the left branch yields a Partial
        // and the rights yields a non-overlapping Partial, with the left branch
        // actually having a non-null refactored expression.

        let sql = "
            SELECT * FROM Player2
            WHERE
                (id = 1 AND True)
                AND (
                    name = 'Merlin'
                );
        ";

        let actual = plan(&storage, sql);

        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(expr("(id = 1 AND True) AND (name = 'Merlin')")),
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we test the case where both the left and right branched yield
        // Partial primary keys with non-null refactored expressions.

        let sql = "
            SELECT * FROM Player2
            WHERE
                (id = 1 AND True)
                AND (name='Merlin' AND True)
                AND (age = 42 AND True);
        ";

        let actual = plan(&storage, sql);

        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![
                        expr("1"),
                        expr("'Merlin'"),
                        expr("42"),
                    ])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("(True) AND (True) AND (True)")),
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");

        // Next, we explore the case where both the left and right branches yield
        // Partial primary keys, but with overlapping values.

        let sql = "
            SELECT * FROM Player2
            WHERE
                (id = 1 AND True)
                AND (id = 2 AND True)
                AND (id = 4 AND True)
                AND (name = 'Merlin' AND id = 3)
                AND (age = 42 AND True AND name = 'Jeff');
        ";

        let actual = plan(&storage, sql);

        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player2".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![
                        expr("3"),
                        expr("'Merlin'"),
                        expr("42"),
                    ])),
                },
                joins: Vec::new(),
            },
            selection: Some(expr("(id = 1 AND True) AND (id = 2 AND True) AND (id = 4 AND True) AND (age = 42 AND True AND name = 'Jeff')")),
            group_by: Vec::new(),
            having: None,
        });

        assert_eq!(actual, expected, "AND binary op 3:\n{sql}");
    }

    #[test]
    fn join_and_nested() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT,
            );
            CREATE TABLE Badge (
                title TEXT PRIMARY KEY,
                user_id INTEGER,
            );
        ");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = 1";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic inner join:\n{sql}");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = Badge.user_id";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: Some(expr("Player.id = Badge.user_id")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "join but no primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE name IN (
                SELECT * FROM Player WHERE id = 1
            )";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: Some(IndexItem::PrimaryKey(vec![expr("1")])),
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(expr("name")),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "nested select:\n{sql}");
    }

    #[test]
    fn join_and_nested_multiple_primary_keys() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT,
                PRIMARY KEY (id, name)
            );
            CREATE TABLE Badge (
                title TEXT,
                user_id INTEGER,
                PRIMARY KEY (title, user_id)
            );
        ");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = 1";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: Some(expr("Player.id = 1")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic inner join:\n{sql}");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = 1 AND Player.name = 'Merlin'";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: Some(IndexItem::PrimaryKey(vec![expr("1"), expr("'Merlin'")])),
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: None,
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "basic inner join:\n{sql}");

        let sql = "SELECT * FROM Player JOIN Badge WHERE Player.id = Badge.user_id";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: "Badge".to_owned(),
                        alias: None,
                        index: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::None),
                    join_executor: JoinExecutor::NestedLoop,
                }],
            },
            selection: Some(expr("Player.id = Badge.user_id")),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "join but no primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player
            WHERE name IN (
                SELECT * FROM Player WHERE id = 1
            )";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("id".to_owned())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Literal(AstLiteral::Number(1.into()))),
                    }),
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(expr("name")),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "nested select:\n{sql}");
    }

    #[test]
    fn not_found() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        ");

        let sql = "SELECT * FROM Player WHERE name = (SELECT name FROM Player LIMIT 1);";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("name".to_owned()),
                        label: "name".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: Some(expr("1")),
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("name".to_owned())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Subquery(Box::new(subquery))),
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "name is not primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player WHERE id IN (
                SELECT id FROM Player WHERE id = id
            );
        ";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("id".to_owned()),
                        label: "id".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(expr("id = id")),
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(Expr::Identifier("id".to_owned())),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "ambiguous nested contexts:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = Statement::Delete {
            table_name: "Player".to_owned(),
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::Number(1.into()))),
            }),
        };
        assert_eq!(actual, expected, "delete statement:\n{sql}");

        let sql = "VALUES (1), (2);";
        let actual = plan(&storage, sql);
        let expected = Statement::Query(Query {
            body: SetExpr::Values(Values(vec![
                vec![Expr::Literal(AstLiteral::Number(1.into()))],
                vec![Expr::Literal(AstLiteral::Number(2.into()))],
            ])),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        });
        assert_eq!(actual, expected, "values:\n{sql}");

        let sql = "SELECT * FROM Player WHERE (name);";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(Expr::Nested(Box::new(expr("name")))),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "nested:\n{sql}");
    }

    #[test]
    fn not_found_multiple_primary_keys() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT,
                PRIMARY KEY (id, name)
            );
        ");

        let sql = "SELECT * FROM Player WHERE name = (SELECT name FROM Player LIMIT 1);";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("name".to_owned()),
                        label: "name".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: None,
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: Some(expr("1")),
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("name".to_owned())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Subquery(Box::new(subquery))),
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "name is not primary key:\n{sql}");

        let sql = "
            SELECT * FROM Player WHERE id IN (
                SELECT id FROM Player WHERE id = id
            );
        ";
        let actual = plan(&storage, sql);
        let expected = {
            let subquery = Query {
                body: SetExpr::Select(Box::new(Select {
                    projection: vec![SelectItem::Expr {
                        expr: Expr::Identifier("id".to_owned()),
                        label: "id".to_owned(),
                    }],
                    from: TableWithJoins {
                        relation: TableFactor::Table {
                            name: "Player".to_owned(),
                            alias: None,
                            index: None,
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(expr("id = id")),
                    group_by: Vec::new(),
                    having: None,
                })),
                limit: None,
                offset: None,
                order_by: Vec::new(),
            };

            select(Select {
                projection: vec![SelectItem::Wildcard],
                from: TableWithJoins {
                    relation: TableFactor::Table {
                        name: "Player".to_owned(),
                        alias: None,
                        index: None,
                    },
                    joins: Vec::new(),
                },
                selection: Some(Expr::InSubquery {
                    expr: Box::new(Expr::Identifier("id".to_owned())),
                    subquery: Box::new(subquery),
                    negated: false,
                }),
                group_by: Vec::new(),
                having: None,
            })
        };
        assert_eq!(actual, expected, "ambiguous nested contexts:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan(&storage, sql);
        let expected = Statement::Delete {
            table_name: "Player".to_owned(),
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::Number(1.into()))),
            }),
        };
        assert_eq!(actual, expected, "delete statement:\n{sql}");

        let sql = "VALUES (1), (2);";
        let actual = plan(&storage, sql);
        let expected = Statement::Query(Query {
            body: SetExpr::Values(Values(vec![
                vec![Expr::Literal(AstLiteral::Number(1.into()))],
                vec![Expr::Literal(AstLiteral::Number(2.into()))],
            ])),
            limit: None,
            offset: None,
            order_by: Vec::new(),
        });
        assert_eq!(actual, expected, "values:\n{sql}");

        let sql = "SELECT * FROM Player WHERE (name);";
        let actual = plan(&storage, sql);
        let expected = select(Select {
            projection: vec![SelectItem::Wildcard],
            from: TableWithJoins {
                relation: TableFactor::Table {
                    name: "Player".to_owned(),
                    alias: None,
                    index: None,
                },
                joins: Vec::new(),
            },
            selection: Some(Expr::Nested(Box::new(expr("name")))),
            group_by: Vec::new(),
            having: None,
        });
        assert_eq!(actual, expected, "nested:\n{sql}");
    }
}
