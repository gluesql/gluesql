use {
    super::{context::Context, planner::Planner},
    crate::{
        ast::BinaryOperator,
        data::Schema,
        plan::{
            ExprPlan, JoinConstraintPlan, JoinExecutorPlan, JoinOperatorPlan, JoinPlan, QueryPlan,
            SelectPlan, SetExprPlan, StatementPlan, TableWithJoinsPlan,
            expr::evaluable::check_expr as check_evaluable,
        },
    },
    std::{collections::HashMap, hash::BuildHasher, rc::Rc},
    utils::Vector,
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: StatementPlan,
) -> StatementPlan {
    let planner = JoinPlanner { schema_map };

    match statement {
        StatementPlan::Query(query) => {
            let query = planner.query(None, query);

            StatementPlan::Query(query)
        }
        _ => statement,
    }
}

struct JoinPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
}

impl<'a, S: BuildHasher> Planner<'a> for JoinPlanner<'a, S> {
    fn query(&self, outer_context: Option<Rc<Context<'a>>>, query: QueryPlan) -> QueryPlan {
        let QueryPlan {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let body = match body {
            SetExprPlan::Select(select) => {
                let select = self.select(outer_context, *select);

                SetExprPlan::Select(Box::new(select))
            }
            SetExprPlan::Values(_) => body,
        };

        QueryPlan {
            body,
            order_by,
            limit,
            offset,
        }
    }

    fn get_schema(&self, name: &str) -> Option<&'a Schema> {
        self.schema_map.get(name)
    }
}

impl<'a, S: BuildHasher> JoinPlanner<'a, S> {
    fn select(&self, outer_context: Option<Rc<Context<'a>>>, select: SelectPlan) -> SelectPlan {
        let SelectPlan {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            aggregate_slots,
        } = select;

        let (outer_context, from) = self.table_with_joins(outer_context, from);
        let selection = selection.map(|expr| self.subquery_expr(outer_context, expr));

        SelectPlan {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            aggregate_slots,
        }
    }

    fn table_with_joins(
        &self,
        outer_context: Option<Rc<Context<'a>>>,
        table_with_joins: TableWithJoinsPlan,
    ) -> (Option<Rc<Context<'a>>>, TableWithJoinsPlan) {
        let TableWithJoinsPlan { relation, joins } = table_with_joins;
        let init_context = self.update_context(None, &relation);
        let (context, joins) =
            joins
                .into_iter()
                .fold((init_context, Vector::new()), |(context, joins), join| {
                    let (context, join) = self.join(outer_context.as_ref(), context, join);
                    let joins = joins.push(join);

                    (context, joins)
                });
        let joins = joins.into();
        let context = Context::concat(context, outer_context);

        (context, TableWithJoinsPlan { relation, joins })
    }

    fn join(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        inner_context: Option<Rc<Context<'a>>>,
        join: JoinPlan,
    ) -> (Option<Rc<Context<'a>>>, JoinPlan) {
        enum JoinOp {
            Inner,
            LeftOuter,
        }

        let JoinPlan {
            relation,
            join_operator,
            join_executor,
        } = join;

        if matches!(join_executor, JoinExecutorPlan::Hash { .. }) {
            let context = self.update_context(inner_context, &relation);
            let join = JoinPlan {
                relation,
                join_operator,
                join_executor,
            };

            return (context, join);
        }

        let (join_op, expr) = match join_operator {
            JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr)) => (JoinOp::Inner, expr),
            JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => (JoinOp::LeftOuter, expr),
            JoinOperatorPlan::Inner(JoinConstraintPlan::None)
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => {
                let context = self.update_context(inner_context, &relation);
                let join = JoinPlan {
                    relation,
                    join_operator,
                    join_executor,
                };

                return (context, join);
            }
        };

        let current_context = self.update_context(None, &relation);
        let (join_executor, join_constraint) = self.plan_join_condition(
            outer_context,
            inner_context.as_ref(),
            current_context.as_ref(),
            expr,
        );

        let join_operator = match join_op {
            JoinOp::Inner => JoinOperatorPlan::Inner(join_constraint),
            JoinOp::LeftOuter => JoinOperatorPlan::LeftOuter(join_constraint),
        };

        let context = self.update_context(inner_context, &relation);
        let join = JoinPlan {
            relation,
            join_operator,
            join_executor,
        };

        (context, join)
    }

    fn plan_join_condition(
        &self,
        outer_context: Option<&Rc<Context<'a>>>,
        inner_context: Option<&Rc<Context<'a>>>,
        current_context: Option<&Rc<Context<'a>>>,
        expr: ExprPlan,
    ) -> (JoinExecutorPlan, JoinConstraintPlan) {
        let key_context = {
            let current = current_context.cloned();
            let outer = outer_context.cloned();

            Context::concat(current, outer)
        };
        let value_context = {
            let context = Context::concat(current_context.cloned(), inner_context.cloned());

            Context::concat(context, outer_context.cloned())
        };

        let mut candidate = None;
        let mut before_candidate = Vec::new();
        let mut after_candidate = Vec::new();

        for expr in split_conjuncts(expr) {
            if candidate.is_none() {
                match match_hash_join_candidate(key_context.as_ref(), value_context.as_ref(), expr)
                {
                    HashJoinCandidateMatch::Matched(hash_candidate) => {
                        candidate = Some(hash_candidate);
                    }
                    HashJoinCandidateMatch::Unmatched(expr) => before_candidate.push(expr),
                }
            } else {
                after_candidate.push(expr);
            }
        }

        let Some(HashJoinCandidate {
            key_expr,
            value_expr,
        }) = candidate
        else {
            let expr = merge_conjuncts(before_candidate)
                .map(|expr| self.subquery_expr(value_context, expr))
                .map_or(JoinConstraintPlan::None, JoinConstraintPlan::On);

            return (JoinExecutorPlan::NestedLoop, expr);
        };

        let remaining = after_candidate
            .into_iter()
            .chain(before_candidate)
            .collect();
        let (where_clause, remainder) = merge_conjuncts(remaining).map_or((None, None), |expr| {
            find_evaluable(key_context.clone(), expr)
        });
        let key_expr = self.subquery_expr(key_context.as_ref().map(Rc::clone), key_expr);
        let value_expr = self.subquery_expr(value_context.as_ref().map(Rc::clone), value_expr);
        let where_clause =
            where_clause.map(|expr| self.subquery_expr(key_context.as_ref().map(Rc::clone), expr));
        let join_constraint = remainder
            .map(|expr| self.subquery_expr(value_context, expr))
            .map_or(JoinConstraintPlan::None, JoinConstraintPlan::On);
        let join_executor = JoinExecutorPlan::Hash {
            key_expr,
            value_expr,
            where_clause,
        };

        (join_executor, join_constraint)
    }
}

type EvaluableExpr = Option<ExprPlan>;
type RemainderExpr = Option<ExprPlan>;

struct HashJoinCandidate {
    key_expr: ExprPlan,
    value_expr: ExprPlan,
}

enum HashJoinCandidateMatch {
    Matched(HashJoinCandidate),
    Unmatched(ExprPlan),
}

fn split_conjuncts(expr: ExprPlan) -> Vec<ExprPlan> {
    match expr {
        ExprPlan::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => split_conjuncts(*left)
            .into_iter()
            .chain(split_conjuncts(*right))
            .collect(),
        ExprPlan::Nested(expr) => split_conjuncts(*expr),
        expr => vec![expr],
    }
}

fn merge_conjuncts(exprs: Vec<ExprPlan>) -> Option<ExprPlan> {
    exprs.into_iter().reduce(|left, right| ExprPlan::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

fn match_hash_join_candidate(
    key_context: Option<&Rc<Context<'_>>>,
    value_context: Option<&Rc<Context<'_>>>,
    expr: ExprPlan,
) -> HashJoinCandidateMatch {
    let ExprPlan::BinaryOp { left, op, right } = expr else {
        return HashJoinCandidateMatch::Unmatched(expr);
    };

    if op != BinaryOperator::Eq {
        return HashJoinCandidateMatch::Unmatched(ExprPlan::BinaryOp { left, op, right });
    }

    let left_as_key = check_evaluable(key_context.map(Rc::clone), &left);
    let right_as_value = check_evaluable(value_context.map(Rc::clone), &right);

    if left_as_key && right_as_value {
        return HashJoinCandidateMatch::Matched(HashJoinCandidate {
            key_expr: *left,
            value_expr: *right,
        });
    }

    let right_as_key = check_evaluable(key_context.map(Rc::clone), &right);
    let left_as_value = left_as_key || check_evaluable(value_context.map(Rc::clone), &left);

    if right_as_key && left_as_value {
        return HashJoinCandidateMatch::Matched(HashJoinCandidate {
            key_expr: *right,
            value_expr: *left,
        });
    }

    HashJoinCandidateMatch::Unmatched(ExprPlan::BinaryOp { left, op, right })
}

fn find_evaluable(
    context: Option<Rc<Context<'_>>>,
    expr: ExprPlan,
) -> (EvaluableExpr, RemainderExpr) {
    match expr {
        ExprPlan::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (evaluable, remainder) = find_evaluable(context.as_ref().map(Rc::clone), *left);
            let (evaluable2, remainder2) = find_evaluable(context, *right);

            let merge = |expr, expr2| match (expr, expr2) {
                (Some(expr), Some(expr2)) => Some(ExprPlan::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::And,
                    right: Box::new(expr2),
                }),
                (Some(expr), None) | (None, Some(expr)) => Some(expr),
                (None, None) => None,
            };

            let evaluable_expr = merge(evaluable, evaluable2);
            let remainder_expr = merge(remainder, remainder2);

            (evaluable_expr, remainder_expr)
        }
        _ if check_evaluable(context, &expr) => (Some(expr), None),
        _ => (None, Some(expr)),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::DateTimeField,
            ast_builder::{Build, QueryNode, col, exists, num, subquery, table},
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{StatementPlan, fetch_schema_map},
            translate::translate,
        },
    };

    fn plan_join(storage: &MockStorage, sql: &str) -> crate::plan::StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let schema_map = fetch_schema_map(storage, &statement).unwrap();

        plan(&schema_map, statement)
    }

    macro_rules! test {
        ($actual: expr, $expected: expr, $name: literal) => {
            let expected = $expected.build().unwrap();

            assert_eq!($actual, expected, $name);
        };
    }

    #[test]
    fn basic() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE PlayerItem (
                user_id INTEGER,
                item_id INTEGER,
                amount INTEGER
            );
        ");

        let sql = "SELECT * FROM Player;";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").select();
        test!(actual, expected, "basic select:\n{sql}");

        let sql = "DELETE FROM Player WHERE id = 1;";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").delete().filter("id = 1");
        test!(actual, expected, "plan not covered:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id != Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on("PlayerItem.user_id != Player.id");
        test!(actual, expected, "basic nested loop join:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON PlayerItem.amount > 2
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .on("PlayerItem.amount > 2");
        test!(actual, expected, "basic nested loop join 2:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN Empty u2
            LEFT JOIN Player u3;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join_as("Empty", "u2")
            .left_join_as("Player", "u3");
        test!(actual, expected, "self multiple joins:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id");
        test!(actual, expected, "basic hash join query:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON PlayerItem.user_id = Player.id
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id");
        test!(
            actual,
            expected,
            "redundant plan does not change the plan result:\n{sql}"
        );

        let statement = actual;
        let schema_map = fetch_schema_map(&storage, &statement).unwrap();
        let actual = plan(&schema_map, statement.clone());
        assert_eq!(actual, statement, "planned hash join remains unchanged");

        let sql = "
            SELECT * FROM Player
            JOIN PlayerItem ON (SELECT * FROM Player u2)
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on("(SELECT * FROM Player u2)");
        test!(actual, expected, "subquery in join_constraint:\n{sql}");
    }

    #[test]
    fn hash_join() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE Item (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE PlayerItem (
                user_id INTEGER,
                item_id INTEGER,
                amount INTEGER
            );
        ");

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                PlayerItem.user_id = Player.id
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(actual, expected, "where_clause AND hash_join expr:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                (PlayerItem.user_id = Player.id) AND
                Player.name = 'abcd' AND
                Player.name != 'barcode'
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .on("Player.name = 'abcd' AND Player.name != 'barcode'");
        test!(
            actual,
            expected,
            "nested expr & remaining join constraint:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            LEFT JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                PlayerItem.amount * 3 <= 2 AND
                PlayerItem.user_id = Player.id
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .left_join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10 AND PlayerItem.amount * 3 <= 2")
            .filter(true);
        test!(actual, expected, "complex where_clause:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id = PlayerItem.user_id AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(actual, expected, "hash_join expr AND where_clause:\n{sql}");

        let sql = "
            SELECT *
            FROM Player u1
            LEFT OUTER JOIN Player u2
            WHERE u2.id = (
                SELECT u3.id
                FROM Player u3
                JOIN Player u4 ON
                    u4.id = u3.id AND
                    u4.id = u1.id
            );
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .alias_as("u1")
            .select()
            .left_join_as("Player", "u2")
            .filter(
                col("u2.id").eq(subquery(
                    table("Player")
                        .alias_as("u3")
                        .select()
                        .join_as("Player", "u4")
                        .hash_executor("u4.id", "u3.id")
                        .hash_filter("u4.id = u1.id")
                        .project("u3.id"),
                )),
            );
        test!(actual, expected, "hash join in subquery:\n{sql}");

        let sql = "
            SELECT * FROM Player u1
            WHERE u1.id = (
                SELECT * FROM Player u2
                WHERE u2.id = (
                    SELECT * FROM Player u3
                    JOIN Player u4 ON
                        u4.id = u3.id + u1.id
                )
            );
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").alias_as("u1").select().filter(
            col("u1.id").eq(subquery(
                table("Player").alias_as("u2").select().filter(
                    col("u2.id").eq(subquery(
                        table("Player")
                            .alias_as("u3")
                            .select()
                            .join_as("Player", "u4")
                            .hash_executor(col("u4.id"), col("u3.id").add("u1.id")),
                    )),
                ),
            )),
        );
        test!(actual, expected, "hash join in nested subquery:\n{sql}");

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id = PlayerItem.user_id AND
                Player.id > 10 AND
                PlayerItem.item_id IS NOT NULL AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .on("Player.id > 10")
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join_constraint AND where_clause:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                Player.id > Player.id + PlayerItem.user_id AND
                Player.id = PlayerItem.user_id AND
                PlayerItem.item_id IS NOT NULL AND
                PlayerItem.amount > 10
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .on("Player.id > Player.id + PlayerItem.user_id")
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join_constraint AND where_clause 2:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                PlayerItem.amount > 10 AND
                (
                    Player.id = PlayerItem.user_id AND
                    PlayerItem.item_id IS NOT NULL
                )
            WHERE True;
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .hash_executor("PlayerItem.user_id", "Player.id")
            .hash_filter("PlayerItem.item_id IS NOT NULL")
            .hash_filter("PlayerItem.amount > 10")
            .filter(true);
        test!(
            actual,
            expected,
            "hash join merges existing where_clause with current-table filter:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                (SELECT * FROM Player JOIN PlayerItem ON Player.id = PlayerItem.user_id)
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player").select().join("PlayerItem").on(subquery(
            table("Player")
                .select()
                .join("PlayerItem")
                .hash_executor("Player.id", "PlayerItem.user_id"),
        ));
        test!(
            actual,
            expected,
            "hash join with join_constraint subquery:\n{sql}"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                1 IN (SELECT * FROM PlayerItem JOIN Player ON PlayerItem.user_id = Player.id)
            WHERE True
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on(num(1).in_list(
                table("PlayerItem")
                    .select()
                    .join("Player")
                    .hash_executor("PlayerItem.user_id", "Player.id"),
            ))
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join constraint in subquery"
        );

        let sql = "
            SELECT *
            FROM Player
            JOIN PlayerItem ON
                EXISTS (SELECT * FROM PlayerItem JOIN Player ON PlayerItem.user_id = Player.id WHERE Player.id > 3)
            WHERE True
        ";
        let actual = plan_join(&storage, sql);
        let expected = table("Player")
            .select()
            .join("PlayerItem")
            .on(exists(
                table("PlayerItem")
                    .select()
                    .join("Player")
                    .hash_executor("PlayerItem.user_id", "Player.id")
                    .filter("Player.id > 3"),
            ))
            .filter(true);
        test!(
            actual,
            expected,
            "hash join with join constraint in subquery"
        );
    }

    #[test]
    fn hash_join_in_subquery() {
        let storage = run("
            CREATE TABLE Player (
                id INTEGER,
                name TEXT
            );
            CREATE TABLE Flag (
                id INTEGER,
                user_id INTEGER,
                name TEXT
            );
        ");

        let subquery_sql = "
            SELECT u.id
            FROM Player u
            JOIN Flag f ON f.user_id = u.id
        ";
        let subquery_node = || -> QueryNode {
            table("Player")
                .alias_as("u")
                .select()
                .join_as("Flag", "f")
                .hash_executor("f.user_id", "u.id")
                .project("u.id")
                .into()
        };

        let sql = format!("SELECT * FROM Player WHERE id = ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(col("id").eq(subquery_node()));
        test!(actual, expected, "binary operator:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE -({subquery_sql}) IN ({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).minus().in_list(subquery_node()));
        test!(actual, expected, "unary operator and in subquery:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                CAST(({subquery_sql}) AS INTEGER) IN (1, 2, 3)
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).cast("INTEGER").in_list("1, 2, 3"));
        test!(actual, expected, "cast and in list:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                ({subquery_sql}) IS NULL
                OR
                ({subquery_sql}) IS NOT NULL
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .is_null()
                .or(subquery(subquery_node()).is_not_null()),
        );
        test!(actual, expected, "is null and is not null:\n{sql}");

        let sql = format!("SELECT * FROM Player WHERE EXISTS({subquery_sql})");
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(exists(subquery_node()));
        test!(actual, expected, "exists:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE ({subquery_sql}) BETWEEN ({subquery_sql}) AND 100;
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player")
            .select()
            .filter(subquery(subquery_node()).between(subquery_node(), num(100)));
        test!(actual, expected, "between:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE EXTRACT(HOUR FROM (({subquery_sql}))) IS NULL
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .nested()
                .extract(DateTimeField::Hour)
                .is_null(),
        );
        test!(actual, expected, "extract and nested:\n{sql}");

        let sql = format!(
            "
            SELECT * FROM Player
            WHERE
                CASE ({subquery_sql})
                    WHEN 10 THEN True
                    WHEN 20 THEN ({subquery_sql}) IS NULL
                    ELSE col3
                END
        "
        );
        let actual = plan_join(&storage, &sql);
        let expected = table("Player").select().filter(
            subquery(subquery_node())
                .case()
                .when_then(10, true)
                .when_then(20, subquery(subquery_node()).is_null())
                .or_else("col3"),
        );
        test!(actual, expected, "case expr:\n{sql}");
    }
}
