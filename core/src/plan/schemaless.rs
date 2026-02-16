use {
    super::{expr::visit_mut_expr, planner::Planner},
    crate::{
        ast::{
            Assignment, Expr, Join, JoinConstraint, JoinExecutor, JoinOperator, Literal,
            OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
            TableWithJoins,
        },
        data::{SCHEMALESS_DOC_COLUMN, Schema},
    },
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasher,
        sync::Arc,
    },
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    statement: Statement,
) -> Statement {
    let schemaless_tables: HashSet<&str> = schema_map
        .iter()
        .filter_map(|(name, schema)| schema.column_defs.is_none().then_some(name.as_str()))
        .collect();

    if schemaless_tables.is_empty() {
        return statement;
    }

    let planner = SchemalessPlanner {
        schema_map,
        schemaless_tables,
    };

    match statement {
        Statement::Query(query) => {
            let query = planner.query(None, query);
            Statement::Query(query)
        }
        Statement::Insert {
            table_name,
            columns,
            source,
        } => {
            let source = planner.query(None, source);
            let columns = if planner.is_schemaless_table(&table_name) {
                vec![SCHEMALESS_DOC_COLUMN.to_owned()]
            } else {
                columns
            };
            Statement::Insert {
                table_name,
                columns,
                source,
            }
        }
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let table_alias = if planner.is_schemaless_table(&table_name) {
                Some(table_name.as_str())
            } else {
                None
            };
            let assignments = assignments
                .into_iter()
                .map(|a| Assignment {
                    id: a.id,
                    value: planner.transform_expr(a.value, table_alias),
                })
                .collect();
            let selection = selection.map(|expr| planner.transform_expr(expr, table_alias));
            Statement::Update {
                table_name,
                assignments,
                selection,
            }
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let table_alias = if planner.is_schemaless_table(&table_name) {
                Some(table_name.as_str())
            } else {
                None
            };
            let selection = selection.map(|expr| planner.transform_expr(expr, table_alias));
            Statement::Delete {
                table_name,
                selection,
            }
        }
        _ => statement,
    }
}

struct SchemalessPlanner<'a, S> {
    schema_map: &'a HashMap<String, Schema, S>,
    schemaless_tables: HashSet<&'a str>,
}

impl<'a, S: BuildHasher> Planner<'a> for SchemalessPlanner<'a, S> {
    fn query(
        &self,
        _outer_context: Option<Arc<super::context::Context<'a>>>,
        query: Query,
    ) -> Query {
        let Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        let (body, table_alias) = match body {
            SetExpr::Select(select) => {
                let table_alias = get_table_alias(&select.from.relation);
                let select = self.select(*select, table_alias.as_deref());
                (SetExpr::Select(Box::new(select)), table_alias)
            }
            SetExpr::Values(_) => (body, None),
        };

        let order_by = order_by
            .into_iter()
            .map(|ob| {
                let expr = self.transform_expr(ob.expr, table_alias.as_deref());
                OrderByExpr { expr, asc: ob.asc }
            })
            .collect();

        let limit = limit.map(|expr| self.transform_expr(expr, table_alias.as_deref()));
        let offset = offset.map(|expr| self.transform_expr(expr, table_alias.as_deref()));

        Query {
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

impl<S: BuildHasher> SchemalessPlanner<'_, S> {
    fn select(&self, select: Select, table_alias: Option<&str>) -> Select {
        let Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
        } = select;

        let projection = projection
            .into_iter()
            .map(|item| self.transform_select_item(item, table_alias))
            .collect();

        let from = self.transform_table_with_joins(from, table_alias);

        let selection = selection.map(|expr| self.transform_expr(expr, table_alias));

        let group_by = group_by
            .into_iter()
            .map(|expr| self.transform_expr(expr, table_alias))
            .collect();

        let having = having.map(|expr| self.transform_expr(expr, table_alias));

        Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
        }
    }

    fn transform_table_with_joins(
        &self,
        table_with_joins: TableWithJoins,
        table_alias: Option<&str>,
    ) -> TableWithJoins {
        let TableWithJoins { relation, joins } = table_with_joins;

        let relation = self.transform_table_factor(relation);
        let joins = joins
            .into_iter()
            .map(|join| self.transform_join(join, table_alias))
            .collect();

        TableWithJoins { relation, joins }
    }

    fn transform_join(&self, join: Join, table_alias: Option<&str>) -> Join {
        let Join {
            relation,
            join_operator,
            join_executor,
        } = join;

        let relation = self.transform_table_factor(relation);

        let join_operator = match join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                JoinOperator::Inner(JoinConstraint::On(self.transform_expr(expr, table_alias)))
            }
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                JoinOperator::LeftOuter(JoinConstraint::On(self.transform_expr(expr, table_alias)))
            }
            other => other,
        };

        let join_executor = match join_executor {
            JoinExecutor::Hash {
                key_expr,
                value_expr,
                where_clause,
            } => JoinExecutor::Hash {
                key_expr: self.transform_expr(key_expr, table_alias),
                value_expr: self.transform_expr(value_expr, table_alias),
                where_clause: where_clause.map(|e| self.transform_expr(e, table_alias)),
            },
            JoinExecutor::NestedLoop => JoinExecutor::NestedLoop,
        };

        Join {
            relation,
            join_operator,
            join_executor,
        }
    }

    fn transform_table_factor(&self, table_factor: TableFactor) -> TableFactor {
        match table_factor {
            TableFactor::Derived { subquery, alias } => {
                let subquery = self.query(None, subquery);
                TableFactor::Derived { subquery, alias }
            }
            other => other,
        }
    }

    fn is_schemaless_table(&self, table_name: &str) -> bool {
        self.schemaless_tables.contains(table_name)
    }

    fn transform_select_item(&self, item: SelectItem, table_alias: Option<&str>) -> SelectItem {
        match item {
            SelectItem::Expr { expr, label } => {
                let expr = self.transform_expr(expr, table_alias);
                SelectItem::Expr { expr, label }
            }
            SelectItem::Wildcard => {
                if let Some(alias) = table_alias
                    && self.is_schemaless_table(alias)
                {
                    return SelectItem::Expr {
                        expr: Expr::Identifier(SCHEMALESS_DOC_COLUMN.to_owned()),
                        label: SCHEMALESS_DOC_COLUMN.to_owned(),
                    };
                }
                SelectItem::Wildcard
            }
            SelectItem::QualifiedWildcard(ref alias) => {
                if self.is_schemaless_table(alias) {
                    SelectItem::Expr {
                        expr: Expr::Identifier(SCHEMALESS_DOC_COLUMN.to_owned()),
                        label: SCHEMALESS_DOC_COLUMN.to_owned(),
                    }
                } else {
                    item
                }
            }
        }
    }

    fn transform_expr(&self, mut expr: Expr, table_alias: Option<&str>) -> Expr {
        visit_mut_expr(&mut expr, &mut |e| match e {
            Expr::Identifier(ident) => {
                if table_alias.is_some_and(|a| self.is_schemaless_table(a)) {
                    *e = make_doc_access(ident);
                }
            }
            Expr::CompoundIdentifier { alias, ident } => {
                if self.is_schemaless_table(alias) {
                    *e = make_compound_doc_access(alias, ident);
                }
            }
            _ => {}
        });
        expr
    }
}

fn get_table_alias(table_factor: &TableFactor) -> Option<String> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => Some(
            alias
                .as_ref()
                .map_or_else(|| name.clone(), |a| a.name.clone()),
        ),
        _ => None,
    }
}

fn make_doc_access(column: &str) -> Expr {
    Expr::ArrayIndex {
        obj: Box::new(Expr::Identifier(SCHEMALESS_DOC_COLUMN.to_owned())),
        indexes: vec![Expr::Literal(Literal::QuotedString(column.to_owned()))],
    }
}

fn make_compound_doc_access(alias: &str, column: &str) -> Expr {
    Expr::ArrayIndex {
        obj: Box::new(Expr::CompoundIdentifier {
            alias: alias.to_owned(),
            ident: SCHEMALESS_DOC_COLUMN.to_owned(),
        }),
        indexes: vec![Expr::Literal(Literal::QuotedString(column.to_owned()))],
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_schemaless,
        crate::{
            data::Schema,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::fetch_schema_map,
            store::StoreMut,
            translate::translate,
        },
        futures::executor::block_on,
    };

    fn setup_schemaless_storage() -> MockStorage {
        let mut storage = MockStorage::default();

        let schema = Schema {
            table_name: "Player".to_owned(),
            column_defs: None,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        };
        block_on(storage.insert_schema(&schema)).unwrap();

        storage
    }

    #[test]
    fn plan_schemaless_transforms() {
        let storage = setup_schemaless_storage();
        let test = |actual: &str, expected: &str, name: &str| {
            let parsed = parse(actual).expect(actual).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let result = plan_schemaless(&schema_map, statement);

            let expected_parsed = parse(expected).expect(expected).into_iter().next().unwrap();
            let expected_stmt = translate(&expected_parsed).unwrap();

            assert_eq!(
                result, expected_stmt,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "SELECT id FROM Player";
        let expected = "SELECT _doc['id'] as id FROM Player";
        test(actual, expected, "single column");

        let actual = "SELECT id, name FROM Player";
        let expected = "SELECT _doc['id'] as id, _doc['name'] as name FROM Player";
        test(actual, expected, "multiple columns");

        let actual = "SELECT * FROM Player";
        let expected = "SELECT _doc as _doc FROM Player";
        test(actual, expected, "wildcard");

        let actual = "SELECT Player.* FROM Player";
        let expected = "SELECT _doc as _doc FROM Player";
        test(actual, expected, "qualified wildcard");

        let actual = "SELECT Player.id FROM Player";
        let expected = "SELECT Player._doc['id'] as id FROM Player";
        test(actual, expected, "compound identifier");

        let actual = "SELECT * FROM Player WHERE id = 1";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] = 1";
        test(actual, expected, "where clause");

        let actual = "SELECT * FROM Player WHERE id = 1 AND name = 'foo'";
        let expected =
            "SELECT _doc as _doc FROM Player WHERE _doc['id'] = 1 AND _doc['name'] = 'foo'";
        test(actual, expected, "where clause complex");

        let actual = "SELECT * FROM Player WHERE name IS NULL";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] IS NULL";
        test(actual, expected, "is null");

        let actual = "SELECT * FROM Player WHERE name IS NOT NULL";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] IS NOT NULL";
        test(actual, expected, "is not null");

        let actual = "SELECT * FROM Player WHERE id IN (1, 2, 3)";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] IN (1, 2, 3)";
        test(actual, expected, "in list");

        let actual = "SELECT * FROM Player WHERE id BETWEEN 1 AND 10";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['id'] BETWEEN 1 AND 10";
        test(actual, expected, "between");

        let actual = "SELECT * FROM Player WHERE name LIKE '%foo%'";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] LIKE '%foo%'";
        test(actual, expected, "like");

        let actual = "SELECT * FROM Player WHERE name ILIKE '%foo%'";
        let expected = "SELECT _doc as _doc FROM Player WHERE _doc['name'] ILIKE '%foo%'";
        test(actual, expected, "ilike");

        let actual = "SELECT * FROM Player WHERE (id = 1)";
        let expected = "SELECT _doc as _doc FROM Player WHERE (_doc['id'] = 1)";
        test(actual, expected, "nested");

        let actual = "SELECT -id as neg_id FROM Player";
        let expected = "SELECT -_doc['id'] as neg_id FROM Player";
        test(actual, expected, "unary op");

        let actual = "SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END as label FROM Player";
        let expected =
            "SELECT CASE WHEN _doc['id'] = 1 THEN 'one' ELSE 'other' END as label FROM Player";
        test(actual, expected, "case expr");

        let actual = "SELECT CAST(id AS TEXT) as id_text FROM Player";
        let expected = "SELECT CAST(_doc['id'] AS TEXT) as id_text FROM Player";
        test(actual, expected, "cast function");

        let actual = "SELECT SUM(score) as total FROM Player";
        let expected = "SELECT SUM(_doc['score']) as total FROM Player";
        test(actual, expected, "aggregate sum");

        let actual = "SELECT COUNT(id) as cnt FROM Player";
        let expected = "SELECT COUNT(_doc['id']) as cnt FROM Player";
        test(actual, expected, "aggregate count");

        let actual = "SELECT COUNT(*) as cnt FROM Player";
        let expected = "SELECT COUNT(*) as cnt FROM Player";
        test(actual, expected, "aggregate count wildcard");

        let actual = "SELECT team, COUNT(*) as cnt FROM Player GROUP BY team";
        let expected =
            "SELECT _doc['team'] as team, COUNT(*) as cnt FROM Player GROUP BY _doc['team']";
        test(actual, expected, "group by");

        let actual = "SELECT team, COUNT(*) as cnt FROM Player GROUP BY team HAVING COUNT(*) > 1";
        let expected = "SELECT _doc['team'] as team, COUNT(*) as cnt FROM Player GROUP BY _doc['team'] HAVING COUNT(*) > 1";
        test(actual, expected, "having");

        let actual = "SELECT id, name FROM Player ORDER BY score";
        let expected =
            "SELECT _doc['id'] as id, _doc['name'] as name FROM Player ORDER BY _doc['score']";
        test(actual, expected, "order by");

        let actual = "SELECT id FROM Player ORDER BY score DESC";
        let expected = "SELECT _doc['id'] as id FROM Player ORDER BY _doc['score'] DESC";
        test(actual, expected, "order by desc");

        let actual = "UPDATE Player SET score = 100 WHERE id = 1";
        let expected = "UPDATE Player SET score = 100 WHERE _doc['id'] = 1";
        test(actual, expected, "update where");

        let actual = "UPDATE Player SET score = score + 1 WHERE id = 1";
        let expected = "UPDATE Player SET score = _doc['score'] + 1 WHERE _doc['id'] = 1";
        test(actual, expected, "update expr");

        let actual = "DELETE FROM Player WHERE id = 1";
        let expected = "DELETE FROM Player WHERE _doc['id'] = 1";
        test(actual, expected, "delete where");

        let actual = "SELECT id FROM Player LIMIT num";
        let expected = "SELECT _doc['id'] as id FROM Player LIMIT _doc['num']";
        test(actual, expected, "limit expr");

        let actual = "SELECT id FROM Player OFFSET skip";
        let expected = "SELECT _doc['id'] as id FROM Player OFFSET _doc['skip']";
        test(actual, expected, "offset expr");

        let actual = "SELECT id FROM Player LIMIT num OFFSET skip";
        let expected = "SELECT _doc['id'] as id FROM Player LIMIT _doc['num'] OFFSET _doc['skip']";
        test(actual, expected, "limit offset expr");

        let actual = "SELECT * FROM (SELECT id FROM Player) AS sub";
        let expected = "SELECT * FROM (SELECT _doc['id'] as id FROM Player) AS sub";
        test(actual, expected, "derived subquery");

        let actual = "INSERT INTO Player VALUES ('{}')";
        let expected = "INSERT INTO Player (_doc) VALUES ('{}')";
        test(actual, expected, "insert schemaless");

        let actual = "INSERT INTO Player VALUES ('{}'), ('{}')";
        let expected = "INSERT INTO Player (_doc) VALUES ('{}'), ('{}')";
        test(actual, expected, "insert schemaless multiple");
    }

    #[test]
    fn schemaful_table_unchanged() {
        let storage = run("CREATE TABLE Item (id INTEGER, name TEXT);");
        let test = |actual: &str, expected: &str, name: &str| {
            let parsed = parse(actual).expect(actual).into_iter().next().unwrap();
            let statement = translate(&parsed).unwrap();
            let schema_map = block_on(fetch_schema_map(&storage, &statement)).unwrap();
            let result = plan_schemaless(&schema_map, statement);

            let expected_parsed = parse(expected).expect(expected).into_iter().next().unwrap();
            let expected_stmt = translate(&expected_parsed).unwrap();

            assert_eq!(
                result, expected_stmt,
                "\n[{name}]\nactual: {actual}\nexpected: {expected}"
            );
        };

        let actual = "SELECT id FROM Item WHERE id = 1";
        let expected = "SELECT id FROM Item WHERE id = 1";
        test(actual, expected, "schemaful unchanged");
    }
}
