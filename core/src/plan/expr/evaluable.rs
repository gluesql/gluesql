use {
    crate::{
        plan::{
            DistinctInputPlan, DistinctPlan, ExprPlan, JoinConstraintPlan, JoinOperatorPlan,
            JoinPlan, LimitInputPlan, LimitPlan, OffsetInputPlan, OffsetPlan, ProjectPlan,
            ProjectionPlan, QueryPlan, SelectItemPlan, SelectOrderByPlan, SelectPlan,
            TableAliasPlan, TableFactorPlan, TableWithJoinsPlan, ValuesOrderByPlan, ValuesPlan,
        },
        plan::{context::Context, expr::PlanExpr},
    },
    std::rc::Rc,
};

pub fn check_expr(context: Option<Rc<Context<'_>>>, expr: &ExprPlan) -> bool {
    match expr.into() {
        PlanExpr::None => true,
        PlanExpr::Identifier(ident) => context.is_some_and(|c| c.contains_column(ident)),
        PlanExpr::CompoundIdentifier { alias, ident } => {
            let table_alias = &alias;
            let column = &ident;

            context.is_some_and(|c| c.contains_aliased_column(table_alias, column))
        }
        PlanExpr::Expr(expr) => check_expr(context, expr),
        PlanExpr::TwoExprs(expr, expr2) => {
            check_expr(context.as_ref().map(Rc::clone), expr) && check_expr(context, expr2)
        }
        PlanExpr::ThreeExprs(expr, expr2, expr3) => {
            check_expr(context.as_ref().map(Rc::clone), expr)
                && check_expr(context.as_ref().map(Rc::clone), expr2)
                && check_expr(context, expr3)
        }
        PlanExpr::MultiExprs(exprs) => exprs
            .iter()
            .all(|expr| check_expr(context.as_ref().map(Rc::clone), expr)),
        PlanExpr::Query(query) => check_query(context.as_ref(), query),
        PlanExpr::QueryAndExpr { query, expr } => {
            check_query(context.as_ref(), query) && check_expr(context, expr)
        }
    }
}

fn check_query(context: Option<&Rc<Context<'_>>>, query: &QueryPlan) -> bool {
    match query {
        QueryPlan::Project(project) => check_project(context, project),
        QueryPlan::Values(values) => check_values(context, values),
        QueryPlan::SelectOrderBy(order_by) => check_select_order_by(context, order_by),
        QueryPlan::ValuesOrderBy(order_by) => check_values_order_by(context, order_by),
        QueryPlan::Distinct(distinct) => check_distinct(context, distinct),
        QueryPlan::Offset(offset) => check_offset(context, offset),
        QueryPlan::Limit(LimitPlan { input, count }) => {
            let input = match input {
                LimitInputPlan::Project(project) => check_project(context, project),
                LimitInputPlan::Values(values) => check_values(context, values),
                LimitInputPlan::SelectOrderBy(order_by) => check_select_order_by(context, order_by),
                LimitInputPlan::ValuesOrderBy(order_by) => check_values_order_by(context, order_by),
                LimitInputPlan::Distinct(distinct) => check_distinct(context, distinct),
                LimitInputPlan::Offset(offset) => check_offset(context, offset),
            };

            input && check_expr(context.map(Rc::clone), count)
        }
    }
}

fn check_offset(context: Option<&Rc<Context<'_>>>, plan: &OffsetPlan) -> bool {
    let input = match &plan.input {
        OffsetInputPlan::Project(project) => check_project(context, project),
        OffsetInputPlan::Values(values) => check_values(context, values),
        OffsetInputPlan::SelectOrderBy(order_by) => check_select_order_by(context, order_by),
        OffsetInputPlan::ValuesOrderBy(order_by) => check_values_order_by(context, order_by),
        OffsetInputPlan::Distinct(distinct) => check_distinct(context, distinct),
    };

    input && check_expr(context.map(Rc::clone), &plan.count)
}

fn check_distinct(context: Option<&Rc<Context<'_>>>, plan: &DistinctPlan) -> bool {
    match &plan.input {
        DistinctInputPlan::Project(project) => check_project(context, project),
        DistinctInputPlan::SelectOrderBy(order_by) => check_select_order_by(context, order_by),
    }
}

fn check_select_order_by(
    context: Option<&Rc<Context<'_>>>,
    SelectOrderByPlan { input, exprs }: &SelectOrderByPlan,
) -> bool {
    check_project(context, input)
        && exprs
            .iter()
            .all(|order_by| check_expr(context.map(Rc::clone), &order_by.expr))
}

fn check_values_order_by(
    context: Option<&Rc<Context<'_>>>,
    ValuesOrderByPlan { input, exprs }: &ValuesOrderByPlan,
) -> bool {
    check_values(context, input)
        && exprs
            .iter()
            .all(|order_by| check_expr(context.map(Rc::clone), &order_by.expr))
}

fn check_values(context: Option<&Rc<Context<'_>>>, ValuesPlan(rows): &ValuesPlan) -> bool {
    rows.iter()
        .flatten()
        .all(|expr| check_expr(context.map(Rc::clone), expr))
}

fn check_select(context: Option<&Rc<Context<'_>>>, select: &SelectPlan) -> bool {
    let SelectPlan {
        from,
        selection,
        group_by,
        having,
        ..
    } = select;

    let TableWithJoinsPlan { relation, joins } = from;

    if !check_table_factor(context, relation) {
        return false;
    }

    if !joins.iter().all(|join| {
        let JoinPlan {
            relation,
            join_operator,
            ..
        } = join;

        if !check_table_factor(context, relation) {
            return false;
        }

        match join_operator {
            JoinOperatorPlan::Inner(JoinConstraintPlan::On(expr))
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::On(expr)) => {
                check_expr(context.map(Rc::clone), expr)
            }
            JoinOperatorPlan::Inner(JoinConstraintPlan::None)
            | JoinOperatorPlan::LeftOuter(JoinConstraintPlan::None) => true,
        }
    }) {
        return false;
    }

    selection
        .iter()
        .chain(group_by.iter())
        .chain(having.iter())
        .all(|expr| check_expr(context.map(Rc::clone), expr))
}

fn check_project(context: Option<&Rc<Context<'_>>>, project: &ProjectPlan) -> bool {
    if !check_select(context, &project.input) {
        return false;
    }

    match &project.projection {
        ProjectionPlan::SelectItems(items) => items.iter().all(|select_item| match select_item {
            SelectItemPlan::Expr { expr, .. } => check_expr(context.map(Rc::clone), expr),
            SelectItemPlan::QualifiedWildcard(_) | SelectItemPlan::Wildcard => true,
        }),
        ProjectionPlan::SchemalessMap => true,
    }
}

fn check_table_factor(context: Option<&Rc<Context<'_>>>, table_factor: &TableFactorPlan) -> bool {
    let contains_alias = |alias: &str| context.is_some_and(|context| context.contains_alias(alias));

    match table_factor {
        TableFactorPlan::Table { name, alias, .. } => {
            let alias = alias
                .as_ref()
                .map_or_else(|| name, |TableAliasPlan { name, .. }| name);

            contains_alias(alias)
        }
        TableFactorPlan::Derived { subquery, alias } => {
            contains_alias(&alias.name) && check_query(context, subquery)
        }
        TableFactorPlan::Series { alias, size } => {
            contains_alias(&alias.name) && check_expr(context.map(Rc::clone), size)
        }
        TableFactorPlan::Dictionary { alias, .. } => contains_alias(&alias.name),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{check_expr, check_query},
        crate::{
            parse_sql::{parse_expr, parse_query},
            plan::context::Context,
            plan::{ExprPlan, QueryPlan},
            translate::{NO_PARAMS, translate_expr, translate_query},
        },
        std::rc::Rc,
    };

    fn test(context: Option<Rc<Context<'_>>>, sql: &str, expected: bool) {
        let parsed = parse_expr(sql).unwrap();
        let expr = translate_expr(&parsed, NO_PARAMS);
        let actual = match expr {
            Ok(expr) => check_expr(context, &ExprPlan::from(expr)),
            Err(_) => false,
        };

        assert_eq!(actual, expected, "{sql}");
    }

    #[test]
    fn evaluable() {
        let context = {
            let left_child = Context::new("Empty".to_owned(), Vec::new(), None);
            let left = Context::new(
                "Foo".to_owned(),
                vec!["id", "name"],
                Some(Rc::new(left_child)),
            );
            let right_child = Context::new("Src".to_owned(), Vec::new(), None);
            let right = Context::new(
                "Bar".to_owned(),
                vec!["id", "rate"],
                Some(Rc::new(right_child)),
            );

            Context::concat(Some(Rc::new(left)), Some(Rc::new(right)))
        };

        macro_rules! test {
            ($sql: literal, $expected: expr) => {
                test(context.as_ref().map(Rc::clone), $sql, $expected);
            };
        }

        // PlanExpr::None
        test!("DATE '2011-01-09'", true);
        test!("'hello world'", true);

        // PlanExpr::Identifier
        test!("id", true);
        test!("name", true);
        test!("new_column", false);

        // PlanExpr::CompoundIdentifier
        test!("Foo.id", true);
        test!("B.rate", false);
        test!("Bar.rate", true);
        test!("Foo.rate", false);
        test!("Rand.id", false);
        test!("a.b.c", false);

        // PlanExpr::Expr
        test!("-10", true);
        test!("rate!", true);
        test!("-wow", false);
        test!("('hello' || 'world')", true);
        test!("(name)", true);
        test!("(1 + cat)", false);
        test!("CAST(id AS DECIMAL)", true);
        test!("CAST(Hello.world AS BOOLEAN)", false);
        test!("EXTRACT(YEAR FROM DATE '2022-03-01')", true);
        test!("EXTRACT(YEAR FROM rate)", true);
        test!("EXTRACT(HOUR FROM virtual_env)", false);
        test!("rate IS NULL", true);
        test!("30 IS NULL", true);
        test!("rate IS NOT NULL", true);
        test!("taste IS NULL", false);
        test!("(1 + random) IS NOT NULL", false);
        test!("SUM(1)", true);
        test!("COUNT(*)", true);
        test!("MAX(rate)", true);
        test!("MIN(anywhere)", false);
        test!("AVG(countable)", false);

        // PlanExpr::TwoExprs
        test!("1 + 2", true);
        test!("1 + name", true);
        test!("mic - 30", false);

        // PlanExpr::ThreeExprs
        test!("30 BETWEEN 10 AND 20", true);
        test!("id BETWEEN rate AND 102", true);
        test!("margin BETWEEN 1 AND 2", false);

        // PlanExpr::MultiExprs
        test!("1 IN (1, 2, 3)", true);
        test!("id IN (1, 30, 4)", true);
        test!("rate IN (id, 1, 2)", true);
        test!("9 IN (id, 1, 2)", true);
        test!("lab IN (100, 101)", false);
        test!("id IN (lab, 101)", false);
        test!("tree IN (something, 101)", false);
        test!("ROUND(1.54)", true);
        test!("TRIM(LEADING 'a' FROM name)", true);
        test!("LOWER(icecream)", false);

        // PlanExpr::Query
        test!(
            "(
                SELECT Bar.*, id, *
                FROM Foo
                JOIN Bar ON True
                LEFT JOIN Empty ON True
                WHERE Foo.id = 1
                LIMIT 1 OFFSET 1
            )",
            true
        );
        test!("(SELECT * FROM Foo JOIN Bar)", true);
        test!("(SELECT * FROM Foo JOIN Berry)", false);
        test!("(SELECT id FROM Carry)", false);
        test!("(SELECT id FROM Carry AS Foo)", true);
        test!("(SELECT T.id FROM Carry AS Bar)", false);
        test!("(SELECT * FROM Foo LIMIT 1)", true);
        test!("(SELECT * FROM Foo OFFSET 1)", true);
        test!("(SELECT * FROM (SELECT id FROM Foo) AS Bar)", true);
        test!("(SELECT * FROM (SELECT id FROM Berry) AS Bar)", false);
        test!("(SELECT * FROM (SELECT id FROM Foo) AS Unknown)", false);
        test!("(SELECT * FROM SERIES(id) AS Bar)", true);
        test!("(SELECT * FROM SERIES(unknown) AS Bar)", false);
        test!("(SELECT * FROM SERIES(id) AS Unknown)", false);
        test!("(SELECT * FROM GLUE_TABLES AS Bar)", true);

        // PlanExpr::QueryAndExpr
        test!(
            "1 IN (
                SELECT id, SUM(rate)
                FROM Bar
                GROUP BY id
                HAVING True
            )",
            true
        );
    }

    #[test]
    fn terminal_query_plan_paths_are_evaluable() {
        for sql in [
            "VALUES (1)",
            "VALUES (1) ORDER BY 1",
            "VALUES (1) OFFSET 1",
            "VALUES (1) ORDER BY 1 OFFSET 1",
            "VALUES (1) LIMIT 1",
            "VALUES (1) ORDER BY 1 LIMIT 1",
            "VALUES (1) LIMIT 1 OFFSET 1",
            "VALUES (1) ORDER BY 1 LIMIT 1 OFFSET 1",
        ] {
            let parsed = parse_query(sql).expect(sql);
            let query = translate_query(&parsed, NO_PARAMS)
                .map(QueryPlan::from)
                .expect(sql);

            assert!(check_query(None, &query), "{sql}");
        }
    }
}
