use {
    super::{
        PlannerError,
        visit_mut::{VisitorMut, walk_expr_mut, walk_query_mut},
    },
    crate::{
        plan::{
            AggregateExprPlan, AggregateFunctionPlan, ColumnDefPlan, CountArgExprPlan, ExprPlan,
            ProjectInputPlan, ProjectionPlan, QueryPlan, SchemaMap, SchemaMapPlan, SelectItemPlan,
            SourcePlan, StatementPlan, StatementPlanKind, TableAccessPlan, TableSourcePlan,
        },
        result::Result,
    },
    std::{collections::BTreeSet, convert::Infallible, iter, vec::IntoIter},
};

pub fn plan(mut statement: StatementPlan) -> Result<StatementPlan> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(PlannerError::UnplannedSchemaMap.into());
    };

    let mut planner = ScanColumnsPlanner {
        schema_map,
        scope_stack: Vec::new(),
    };
    planner.statement(&mut statement.kind);

    Ok(statement)
}

// Schemaful FullScan sources belonging to one query.
type ScanScope<'a> = Vec<ScanSource<'a>>;

struct ScanColumnsPlanner<'a> {
    schema_map: &'a SchemaMap,
    scope_stack: Vec<ScanScope<'a>>,
}

struct ScanSource<'a> {
    source_name: String,
    column_aliases: Vec<String>,
    column_defs: &'a [ColumnDefPlan],
    required_columns: Option<BTreeSet<String>>,
}

impl ScanColumnsPlanner<'_> {
    fn statement(&mut self, kind: &mut StatementPlanKind) {
        match kind {
            StatementPlanKind::Query(query) | StatementPlanKind::Insert { source: query, .. } => {
                self.visit_query(query).unwrap();
            }
            StatementPlanKind::CreateTable {
                source: Some(query),
                ..
            } => {
                self.visit_query(query).unwrap();
            }
            StatementPlanKind::Update {
                assignments,
                selection,
                ..
            } => {
                for assignment in assignments {
                    self.visit_expr(&mut assignment.value).unwrap();
                }
                if let Some(expr) = selection {
                    self.visit_expr(expr).unwrap();
                }
            }
            StatementPlanKind::Delete {
                selection: Some(expr),
                ..
            } => {
                self.visit_expr(expr).unwrap();
            }
            _ => {}
        }
    }

    fn require_column(&mut self, alias: Option<&str>, ident: &str) {
        for source in self.scope_stack.iter_mut().flatten() {
            if alias.is_some_and(|alias| alias != source.source_name) {
                continue;
            }
            let Some(required_columns) = &mut source.required_columns else {
                continue;
            };

            for (index, column) in source.column_defs.iter().enumerate() {
                let exposed_name = source.column_aliases.get(index).unwrap_or(&column.name);
                if exposed_name == ident {
                    required_columns.insert(column.name.clone());
                }
            }
        }
    }

    fn require_all_columns(&mut self) {
        for source in self.scope_stack.iter_mut().flatten() {
            source.required_columns = None;
        }
    }
}

impl VisitorMut for ScanColumnsPlanner<'_> {
    type Error = Infallible;

    fn visit_query(&mut self, query: &mut QueryPlan) -> Result<(), Self::Error> {
        let mut scope = collect_scan_sources(self.schema_map, query);
        if let Some(project) = query.project()
            && let ProjectionPlan::SelectItems(items) = &project.projection
        {
            for item in items {
                for source in &mut scope {
                    match item {
                        SelectItemPlan::Wildcard => source.required_columns = None,
                        SelectItemPlan::QualifiedWildcard(alias)
                            if alias == &source.source_name =>
                        {
                            source.required_columns = None;
                        }
                        _ => {}
                    }
                }
            }
        }
        self.scope_stack.push(scope);

        let aggregation = query.project().and_then(|project| match &project.input {
            ProjectInputPlan::Aggregation(aggregation) => Some(aggregation),
            ProjectInputPlan::Having(having) => Some(&having.input),
            _ => None,
        });
        if aggregation
            .is_some_and(|aggregation| aggregation.aggregate_slots.iter().any(is_distinct_wildcard))
        {
            self.require_all_columns();
        }

        walk_query_mut(self, query)?;

        let scope = self
            .scope_stack
            .pop()
            .expect("query scope was pushed before walking");
        apply_scan_columns(self.schema_map, scope, query);

        Ok(())
    }

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
        match expr {
            ExprPlan::Identifier(ident) => self.require_column(None, ident),
            ExprPlan::CompoundIdentifier { alias, ident } => {
                self.require_column(Some(alias), ident);
            }
            ExprPlan::Aggregate(aggregate) if is_distinct_wildcard(aggregate) => {
                self.require_all_columns();
            }
            _ => {}
        }

        walk_expr_mut(self, expr)
    }
}

fn is_distinct_wildcard(aggregate: &AggregateExprPlan) -> bool {
    aggregate.distinct
        && matches!(
            aggregate.func,
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard)
        )
}

fn scan_column_defs<'a>(
    schema_map: &'a SchemaMap,
    table: &TableSourcePlan,
) -> Option<&'a [ColumnDefPlan]> {
    if !matches!(table.access, TableAccessPlan::FullScan { .. }) {
        return None;
    }

    schema_map.get(&table.name)?.column_defs.as_deref()
}

fn collect_scan_sources<'a>(schema_map: &'a SchemaMap, query: &QueryPlan) -> ScanScope<'a> {
    let Some(project) = query.project() else {
        return Vec::new();
    };

    iter::once(project.input.base_source())
        .chain(project.input.joined_sources())
        .filter_map(|source| {
            let SourcePlan::Table(table) = source else {
                return None;
            };
            let column_defs = scan_column_defs(schema_map, table)?;

            Some(ScanSource {
                source_name: source.alias_name().to_owned(),
                column_aliases: table
                    .alias
                    .as_ref()
                    .map_or_else(Vec::new, |alias| alias.columns.clone()),
                column_defs,
                required_columns: Some(BTreeSet::new()),
            })
        })
        .collect()
}

fn apply_scan_columns<'a>(schema_map: &'a SchemaMap, scope: ScanScope<'a>, query: &mut QueryPlan) {
    let mut applier = ScanColumnsApplier {
        schema_map,
        sources: scope.into_iter(),
    };
    walk_query_mut(&mut applier, query).unwrap();
}

struct ScanColumnsApplier<'a> {
    schema_map: &'a SchemaMap,
    sources: IntoIter<ScanSource<'a>>,
}

impl VisitorMut for ScanColumnsApplier<'_> {
    type Error = Infallible;

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<(), Self::Error> {
        let SourcePlan::Table(table) = source else {
            return Ok(());
        };
        let column_defs = scan_column_defs(self.schema_map, table);
        let TableAccessPlan::FullScan { columns } = &mut table.access else {
            return Ok(());
        };
        let Some(column_defs) = column_defs else {
            *columns = None;
            return Ok(());
        };
        let source = self
            .sources
            .next()
            .expect("collected scan sources follow query source order");
        *columns = source.required_columns.map(|required_columns| {
            column_defs
                .iter()
                .filter(|column| required_columns.contains(&column.name))
                .map(|column| column.name.clone())
                .collect()
        });

        Ok(())
    }

    // Nested queries have already applied their own scan columns.
    fn visit_expr(&mut self, _: &mut ExprPlan) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::plan as plan_scan_columns,
        crate::{
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{SourcePlan, StatementPlan, StatementPlanKind, TableAccessPlan},
            planner::{
                PlannerError, plan_aggregate, plan_hash_join, plan_schema_map,
                visit::{Visitor, walk_source},
            },
            query_builder::{Build, exists, non_clustered, primary_key, table},
            translate::translate,
        },
        std::convert::Infallible,
    };

    fn statement(sql: &str) -> StatementPlan {
        let parsed = parse(sql).expect(sql).into_iter().next().expect(sql);
        translate(&parsed).expect(sql).into()
    }

    fn input(storage: &MockStorage, sql: &str) -> StatementPlan {
        plan_aggregate(plan_schema_map(storage, statement(sql)).unwrap())
    }

    #[derive(Default)]
    struct ScanColumns(Vec<(String, Option<Vec<String>>)>);

    impl Visitor for ScanColumns {
        type Error = Infallible;

        fn visit_source(&mut self, source: &SourcePlan) -> Result<(), Self::Error> {
            if let SourcePlan::Table(table) = source
                && let TableAccessPlan::FullScan { columns } = &table.access
            {
                self.0
                    .push((source.alias_name().to_owned(), columns.clone()));
            }
            walk_source(self, source)
        }
    }

    fn test_plan(input: StatementPlan, expected: &[(&str, Option<&[&str]>)], name: &str) {
        let actual = plan_scan_columns(input).unwrap();
        let mut columns = ScanColumns::default();
        match &actual.kind {
            StatementPlanKind::Query(query) | StatementPlanKind::Insert { source: query, .. } => {
                columns.visit_query(query).unwrap();
            }
            StatementPlanKind::CreateTable {
                source: Some(query),
                ..
            } => {
                columns.visit_query(query).unwrap();
            }
            StatementPlanKind::Update {
                assignments,
                selection,
                ..
            } => {
                for assignment in assignments {
                    columns.visit_expr(&assignment.value).unwrap();
                }
                if let Some(expr) = selection {
                    columns.visit_expr(expr).unwrap();
                }
            }
            StatementPlanKind::Delete {
                selection: Some(expr),
                ..
            } => {
                columns.visit_expr(expr).unwrap();
            }
            _ => {}
        }
        let expected = expected
            .iter()
            .map(|(source, columns)| {
                (
                    (*source).to_owned(),
                    columns
                        .map(|columns| columns.iter().map(|column| (*column).to_owned()).collect()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(columns.0, expected, "{name}");
    }

    fn test(storage: &MockStorage, sql: &str, expected: &[(&str, Option<&[&str]>)]) {
        test_plan(input(storage, sql), expected, sql);
    }

    #[test]
    fn column_names_and_aliases() {
        let storage = run("CREATE TABLE Item (a INT, b INT, c INT, d INT)");

        let sql = "SELECT COALESCE(b, 0) FROM Item WHERE c > 0 ORDER BY d";
        test(&storage, sql, &[("Item", Some(&["b", "c", "d"]))]);
        let actual = plan_scan_columns(input(&storage, sql)).unwrap();
        test_plan(
            actual,
            &[("Item", Some(&["b", "c", "d"]))],
            "repeat planning retains column dependencies",
        );

        let sql = "SELECT y FROM Item AS i(x, y, z, w)";
        test(&storage, sql, &[("i", Some(&["b"]))]);

        let sql = "SELECT b AS a FROM Item ORDER BY a";
        test(&storage, sql, &[("Item", Some(&["a", "b"]))]);

        let sql = "SELECT x FROM Item AS i(x, x, z, w)";
        test(&storage, sql, &[("i", Some(&["a", "b"]))]);

        let sql = "SELECT Item.a FROM Item AS i";
        test(&storage, sql, &[("i", Some(&[]))]);

        let sql = "SELECT missing FROM Item";
        test(&storage, sql, &[("Item", Some(&[]))]);

        let sql = "SELECT x FROM Item AS i(x, y, z, w, extra)";
        test(&storage, sql, &[("i", Some(&["a"]))]);
    }

    #[test]
    fn wildcard_and_aggregates() {
        let storage = run("CREATE TABLE Item (a INT, b INT, c INT)");

        let sql = "SELECT * FROM Item";
        test(&storage, sql, &[("Item", None)]);

        let sql = "SELECT i.*, i.b FROM Item i";
        test(&storage, sql, &[("i", None)]);

        let sql = "SELECT 1 FROM Item";
        test(&storage, sql, &[("Item", Some(&[]))]);

        let sql = "SELECT COUNT(*) FROM Item";
        test(&storage, sql, &[("Item", Some(&[]))]);

        let sql = "SELECT COUNT(*) FROM Item WHERE b > 0";
        test(&storage, sql, &[("Item", Some(&["b"]))]);

        let sql = "SELECT COUNT(DISTINCT *) FROM Item";
        test(&storage, sql, &[("Item", None)]);
        let actual = plan_schema_map(&storage, statement(sql)).unwrap();
        test_plan(
            actual,
            &[("Item", None)],
            "distinct wildcard before aggregate slots",
        );

        let sql = "SELECT DISTINCT b FROM Item";
        test(&storage, sql, &[("Item", Some(&["b"]))]);

        let sql = "SELECT SUM(b) FROM Item GROUP BY c HAVING MAX(b) > 0";
        test(&storage, sql, &[("Item", Some(&["b", "c"]))]);
    }

    #[test]
    fn joins_and_nested_scopes() {
        let storage = run(
            "CREATE TABLE Item (a INT, b INT, c INT); CREATE TABLE Ref (ref INT, payload TEXT)",
        );

        let sql = "SELECT l.a, r.b FROM Item l JOIN Item r ON l.c = r.c";
        test(
            &storage,
            sql,
            &[("l", Some(&["a", "c"])), ("r", Some(&["b", "c"]))],
        );
        let actual = plan_hash_join(input(&storage, sql)).unwrap();
        test_plan(
            actual,
            &[("l", Some(&["a", "c"])), ("r", Some(&["b", "c"]))],
            sql,
        );

        let sql = "SELECT a FROM Item l JOIN Item r ON l.c = r.c";
        test(
            &storage,
            sql,
            &[("l", Some(&["a", "c"])), ("r", Some(&["a", "c"]))],
        );

        let sql = "SELECT l.* FROM Item l LEFT JOIN Item r ON l.c = r.c";
        test(&storage, sql, &[("l", None), ("r", Some(&["c"]))]);

        let sql = "SELECT i.a FROM Item i WHERE EXISTS (SELECT 1 FROM Ref r WHERE r.ref = i.b)";
        test(
            &storage,
            sql,
            &[("i", Some(&["a", "b"])), ("r", Some(&["ref"]))],
        );

        let sql = "SELECT i.a FROM Item i WHERE EXISTS (SELECT * FROM Ref)";
        test(&storage, sql, &[("i", Some(&["a"])), ("Ref", None)]);

        let sql = "SELECT i.a FROM Item i WHERE EXISTS (SELECT COUNT(DISTINCT *) FROM Ref)";
        test(&storage, sql, &[("i", None), ("Ref", None)]);

        let sql = "SELECT d.x FROM (SELECT a AS x, b FROM Item) d";
        test(&storage, sql, &[("Item", Some(&["a", "b"]))]);

        let sql = "SELECT (SELECT i.b FROM SERIES(i.c)) FROM Item i";
        test(&storage, sql, &[("i", Some(&["b", "c"]))]);

        let sql = "SELECT (SELECT l.b FROM Item l), (SELECT r.c FROM Item r) FROM Item outer_item";
        test(
            &storage,
            sql,
            &[
                ("outer_item", Some(&[])),
                ("l", Some(&["b"])),
                ("r", Some(&["c"])),
            ],
        );

        let sql = "VALUES ((SELECT b FROM Item))";
        test(&storage, sql, &[("Item", Some(&["b"]))]);
    }

    #[test]
    fn excluded_sources_and_lookup_dependencies() {
        let storage = run(
            "CREATE TABLE Item (a INT, b INT); CREATE TABLE Lookup (id INT PRIMARY KEY, payload TEXT); CREATE TABLE Logs",
        );

        let sql = "SELECT i.b, l.name FROM Item i JOIN Logs l ON i.a = l.id";
        test(&storage, sql, &[("i", Some(&["a", "b"])), ("l", None)]);

        let sql = "SELECT COUNT(*) FROM Logs";
        test(&storage, sql, &[("Logs", None)]);

        let sql = "SELECT * FROM Missing";
        test(&storage, sql, &[("Missing", None)]);

        let actual = table("Item")
            .select()
            .filter(exists(
                table("Lookup")
                    .index_by(primary_key().eq("Item.a"))
                    .select()
                    .project("1"),
            ))
            .project("b")
            .build()
            .unwrap();
        let actual = plan_schema_map(&storage, actual).unwrap();
        test_plan(
            actual,
            &[("Item", Some(&["a", "b"]))],
            "primary-key lookup references outer source",
        );

        let actual = table("Item")
            .select()
            .filter(exists(
                table("Lookup")
                    .index_by(non_clustered("idx_id".to_owned()).eq("Item.a"))
                    .select()
                    .project("1"),
            ))
            .project("b")
            .build()
            .unwrap();
        let actual = plan_schema_map(&storage, actual).unwrap();
        test_plan(
            actual,
            &[("Item", Some(&["a", "b"]))],
            "index lookup references outer source",
        );
    }

    #[test]
    fn statement_sources() {
        let storage =
            run("CREATE TABLE Item (a INT, b INT); CREATE TABLE Ref (ref INT, payload TEXT)");

        let sql = "INSERT INTO Item SELECT ref, 1 FROM Ref";
        test(&storage, sql, &[("Ref", Some(&["ref"]))]);

        let sql = "CREATE TABLE Copy AS SELECT b FROM Item";
        test(&storage, sql, &[("Item", Some(&["b"]))]);

        let sql = "UPDATE Item SET a = (SELECT ref FROM Ref WHERE ref = Item.b) WHERE EXISTS (SELECT 1 FROM Ref WHERE ref = Item.a)";
        test(
            &storage,
            sql,
            &[("Ref", Some(&["ref"])), ("Ref", Some(&["ref"]))],
        );

        let sql = "DELETE FROM Item WHERE EXISTS (SELECT ref FROM Ref WHERE ref = Item.b)";
        test(&storage, sql, &[("Ref", Some(&["ref"]))]);

        let sql = "UPDATE Item SET a = 1";
        test(&storage, sql, &[]);

        let sql = "DELETE FROM Item";
        test(&storage, sql, &[]);

        let sql = "BEGIN";
        test(&storage, sql, &[]);
    }

    #[test]
    fn unplanned_schema_map() {
        let actual = plan_scan_columns(statement("SELECT * FROM Item"));
        let expected = Err(PlannerError::UnplannedSchemaMap.into());
        assert_eq!(actual, expected);
    }
}
