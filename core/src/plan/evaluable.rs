use {
    super::{context::Context, expr::PlanExpr},
    crate::ast::{
        Expr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, TableAlias,
        TableFactor, TableWithJoins, Values,
    },
    std::{convert::identity, rc::Rc},
};

pub fn check_expr(context: Option<Rc<Context<'_>>>, expr: &Expr) -> bool {
    match expr.into() {
        PlanExpr::None => true,
        PlanExpr::Identifier(ident) => context.map(|c| c.contains_column(ident)).unwrap_or(false),
        PlanExpr::CompoundIdentifier { alias, ident } => {
            let table_alias = &alias;
            let column = &ident;

            context
                .map(|c| c.contains_aliased_column(table_alias, column))
                .unwrap_or(false)
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
        PlanExpr::Query(query) => check_query(context, query),
        PlanExpr::QueryAndExpr { query, expr } => {
            check_query(context.as_ref().map(Rc::clone), query) && check_expr(context, expr)
        }
    }
}

fn check_query(context: Option<Rc<Context<'_>>>, query: &Query) -> bool {
    let Query {
        body,
        order_by,
        limit,
        offset,
    } = query;

    let body = match body {
        SetExpr::Select(select) => check_select(context.as_ref().map(Rc::clone), select),
        SetExpr::Values(Values(rows)) => rows
            .iter()
            .flatten()
            .map(|expr| check_expr(context.as_ref().map(Rc::clone), expr))
            .all(identity),
    };

    if !body {
        return false;
    }

    let order_by = order_by
        .iter()
        .map(|order_by| &order_by.expr)
        .map(|expr| check_expr(context.as_ref().map(Rc::clone), expr))
        .all(identity);
    if !order_by {
        return false;
    }

    limit
        .iter()
        .chain(offset.iter())
        .map(|expr| check_expr(context.as_ref().map(Rc::clone), expr))
        .all(identity)
}

fn check_select(context: Option<Rc<Context<'_>>>, select: &Select) -> bool {
    let Select {
        projection,
        from,
        selection,
        group_by,
        having,
    } = select;

    if !projection
        .iter()
        .map(|select_item| match select_item {
            SelectItem::Expr { expr, .. } => check_expr(context.as_ref().map(Rc::clone), expr),
            SelectItem::QualifiedWildcard(_) | SelectItem::Wildcard => true,
        })
        .all(identity)
    {
        return false;
    }

    let TableWithJoins { relation, joins } = from;

    if !check_table_factor(context.as_ref().map(Rc::clone), relation) {
        return false;
    }

    if !joins
        .iter()
        .map(|join| {
            let Join {
                relation,
                join_operator,
                ..
            } = join;

            if !check_table_factor(context.as_ref().map(Rc::clone), relation) {
                return false;
            }

            match join_operator {
                JoinOperator::Inner(JoinConstraint::On(expr))
                | JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                    check_expr(context.as_ref().map(Rc::clone), expr)
                }
                JoinOperator::Inner(JoinConstraint::None)
                | JoinOperator::LeftOuter(JoinConstraint::None) => true,
            }
        })
        .all(identity)
    {
        return false;
    }

    selection
        .iter()
        .chain(group_by.iter())
        .chain(having.iter())
        .map(|expr| check_expr(context.as_ref().map(Rc::clone), expr))
        .all(identity)
}

fn check_table_factor(context: Option<Rc<Context<'_>>>, table_factor: &TableFactor) -> bool {
    let alias = match table_factor {
        TableFactor::Table { name, alias, .. } => alias
            .as_ref()
            .map(|TableAlias { name, .. }| name)
            .unwrap_or_else(|| name),
        TableFactor::Derived { alias, .. }
        | TableFactor::Series { alias, .. }
        | TableFactor::Dictionary { alias, .. } => &alias.name,
    };

    context
        .map(|context| context.contains_alias(alias))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use {
        super::{check_expr, Context},
        crate::{parse_sql::parse_expr, translate::translate_expr},
        std::rc::Rc,
    };

    fn test(context: Option<Rc<Context<'_>>>, sql: &str, expected: bool) {
        let parsed = parse_expr(sql).unwrap();
        let expr = translate_expr(&parsed);
        let actual = match expr {
            Ok(expr) => check_expr(context, &expr),
            Err(_) => false,
        };

        assert_eq!(actual, expected, "{sql}");
    }

    #[test]
    fn evaluable() {
        let context = {
            let left_child = Context::new("Empty".to_owned(), Vec::new(), None, None);
            let left = Context::new(
                "Foo".to_owned(),
                vec!["id", "name"],
                None,
                Some(Rc::new(left_child)),
            );
            let right_child = Context::new("Src".to_owned(), Vec::new(), None, None);
            let right = Context::new(
                "Bar".to_owned(),
                vec!["id", "rate"],
                None,
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
}
