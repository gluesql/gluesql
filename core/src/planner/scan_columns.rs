use {
    super::{
        PlannerError,
        visit_mut::{VisitorMut, walk_expr_mut, walk_query_mut},
    },
    crate::{
        plan::{
            AggregateExprPlan, AggregateFunctionPlan, CountArgExprPlan, ExprPlan, ProjectInputPlan,
            ProjectionPlan, QueryPlan, SchemaMap, SchemaMapPlan, SelectItemPlan, SourcePlan,
            StatementPlan, StatementPlanKind, TableAccessPlan,
        },
        result::Result,
    },
    std::{collections::BTreeSet, convert::Infallible},
};

pub fn plan(mut statement: StatementPlan) -> Result<StatementPlan> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(PlannerError::UnplannedSchemaMap.into());
    };

    let mut planner = ScanColumnsPlanner {
        schema_map,
        required_columns: RequiredColumns::Selected(BTreeSet::new()),
    };
    planner.statement(&mut statement.kind);

    Ok(statement)
}

struct ScanColumnsPlanner<'a> {
    schema_map: &'a SchemaMap,
    required_columns: RequiredColumns,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ColumnReference {
    source_name: Option<String>,
    column_name: String,
}

enum RequiredColumns {
    // COUNT(DISTINCT *) includes outer row context values in its distinct key.
    All,
    Selected(BTreeSet<ColumnReference>),
}

impl RequiredColumns {
    fn require_column(&mut self, source_name: Option<&str>, column_name: &str) {
        if let Self::Selected(columns) = self {
            columns.insert(ColumnReference {
                source_name: source_name.map(str::to_owned),
                column_name: column_name.to_owned(),
            });
        }
    }

    fn merge(&mut self, other: Self) {
        match (self, other) {
            (columns, Self::All) => *columns = Self::All,
            (Self::Selected(columns), Self::Selected(other)) => columns.extend(other),
            (Self::All, Self::Selected(_)) => {}
        }
    }
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
}

impl VisitorMut for ScanColumnsPlanner<'_> {
    type Error = Infallible;

    fn visit_query(&mut self, query: &mut QueryPlan) -> Result<(), Self::Error> {
        let mut query_planner = ScanColumnsPlanner {
            schema_map: self.schema_map,
            required_columns: RequiredColumns::Selected(BTreeSet::new()),
        };

        let aggregation = query.project().and_then(|project| match &project.input {
            ProjectInputPlan::Aggregation(aggregation) => Some(aggregation),
            ProjectInputPlan::Having(having) => Some(&having.input),
            _ => None,
        });
        if aggregation.is_some_and(|aggregation| {
            aggregation
                .aggregate_slots
                .iter()
                .any(is_count_distinct_wildcard)
        }) {
            query_planner.required_columns = RequiredColumns::All;
        }

        walk_query_mut(&mut query_planner, query)?;

        apply_scan_columns(self.schema_map, &query_planner.required_columns, query);
        self.required_columns.merge(query_planner.required_columns);

        Ok(())
    }

    fn visit_expr(&mut self, expr: &mut ExprPlan) -> Result<(), Self::Error> {
        match expr {
            ExprPlan::Identifier(ident) => self.required_columns.require_column(None, ident),
            ExprPlan::CompoundIdentifier { alias, ident } => {
                self.required_columns.require_column(Some(alias), ident);
            }
            ExprPlan::Aggregate(aggregate) if is_count_distinct_wildcard(aggregate) => {
                self.required_columns = RequiredColumns::All;
            }
            _ => {}
        }

        walk_expr_mut(self, expr)
    }
}

fn is_count_distinct_wildcard(aggregate: &AggregateExprPlan) -> bool {
    aggregate.distinct
        && matches!(
            aggregate.func,
            AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard)
        )
}

fn apply_scan_columns(
    schema_map: &SchemaMap,
    required_columns: &RequiredColumns,
    query: &mut QueryPlan,
) {
    let mut applier = ScanColumnsApplier {
        schema_map,
        required_columns,
        wildcards: ProjectionWildcards::from_query(query),
    };
    walk_query_mut(&mut applier, query).unwrap();
}

struct ScanColumnsApplier<'a> {
    schema_map: &'a SchemaMap,
    required_columns: &'a RequiredColumns,
    wildcards: ProjectionWildcards,
}

enum ProjectionWildcards {
    All,
    Qualified(BTreeSet<String>),
}

impl ProjectionWildcards {
    fn from_query(query: &QueryPlan) -> Self {
        let mut sources = BTreeSet::new();
        if let Some(project) = query.project()
            && let ProjectionPlan::SelectItems(items) = &project.projection
        {
            for item in items {
                match item {
                    SelectItemPlan::Wildcard => return Self::All,
                    SelectItemPlan::QualifiedWildcard(alias) => {
                        sources.insert(alias.clone());
                    }
                    SelectItemPlan::Expr { .. } => {}
                }
            }
        }

        Self::Qualified(sources)
    }

    fn includes(&self, source_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Qualified(sources) => sources.contains(source_name),
        }
    }
}

impl VisitorMut for ScanColumnsApplier<'_> {
    type Error = Infallible;

    fn visit_source(&mut self, source: &mut SourcePlan) -> Result<(), Self::Error> {
        let SourcePlan::Table(table) = source else {
            return Ok(());
        };
        if !matches!(
            table.access,
            TableAccessPlan::FullScan | TableAccessPlan::ProjectedScan { .. }
        ) {
            return Ok(());
        }
        let Some(column_defs) = self
            .schema_map
            .get(&table.name)
            .and_then(|schema| schema.column_defs.as_deref())
        else {
            table.access = TableAccessPlan::FullScan;
            return Ok(());
        };
        let source_name = table
            .alias
            .as_ref()
            .map_or(table.name.as_str(), |alias| alias.name.as_str());

        table.access = match self.required_columns {
            RequiredColumns::Selected(required_columns)
                if !self.wildcards.includes(source_name) =>
            {
                TableAccessPlan::ProjectedScan {
                    columns: column_defs
                        .iter()
                        .enumerate()
                        .filter(|(index, column)| {
                            let exposed_name = table
                                .alias
                                .as_ref()
                                .and_then(|alias| alias.columns.get(*index))
                                .unwrap_or(&column.name);

                            required_columns.iter().any(|reference| {
                                reference.column_name == *exposed_name
                                    && reference
                                        .source_name
                                        .as_deref()
                                        .is_none_or(|name| name == source_name)
                            })
                        })
                        .map(|(_, column)| column.name.clone())
                        .collect(),
                }
            }
            _ => TableAccessPlan::FullScan,
        };

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
            data::Value,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{
                AggregateExprPlan, AggregateFunctionPlan, AggregationInputPlan, AggregationPlan,
                CountArgExprPlan, ExprPlan, ProjectInputPlan, ProjectPlan, ProjectionPlan,
                QueryPlan, SelectItemPlan, SourcePlan, StatementPlan, StatementPlanKind,
                TableAccessPlan::{self, FullScan},
                TableSourcePlan,
            },
            planner::{
                PlannerError, plan_schema_map,
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

    fn plan(storage: &MockStorage, sql: &str) -> StatementPlan {
        let input = plan_schema_map(storage, statement(sql)).unwrap();

        plan_scan_columns(input).unwrap()
    }

    fn table_source(name: &str) -> SourcePlan {
        SourcePlan::Table(TableSourcePlan {
            name: name.to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        })
    }

    fn projected_scan(columns: &[&str]) -> TableAccessPlan {
        TableAccessPlan::ProjectedScan {
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        }
    }

    #[derive(Default)]
    struct ScanAccesses(Vec<(String, TableAccessPlan)>);

    impl Visitor for ScanAccesses {
        type Error = Infallible;

        fn visit_source(&mut self, source: &SourcePlan) -> Result<(), Self::Error> {
            if let SourcePlan::Table(table) = source
                && matches!(
                    table.access,
                    TableAccessPlan::FullScan | TableAccessPlan::ProjectedScan { .. }
                )
            {
                self.0
                    .push((source.alias_name().to_owned(), table.access.clone()));
            }
            walk_source(self, source)
        }
    }

    fn collect_scan_accesses(statement: &StatementPlan) -> Vec<(String, TableAccessPlan)> {
        let mut accesses = ScanAccesses::default();
        match &statement.kind {
            StatementPlanKind::Query(query) | StatementPlanKind::Insert { source: query, .. } => {
                accesses.visit_query(query).unwrap();
            }
            StatementPlanKind::CreateTable {
                source: Some(query),
                ..
            } => {
                accesses.visit_query(query).unwrap();
            }
            StatementPlanKind::Update {
                assignments,
                selection,
                ..
            } => {
                for assignment in assignments {
                    accesses.visit_expr(&assignment.value).unwrap();
                }
                if let Some(expr) = selection {
                    accesses.visit_expr(expr).unwrap();
                }
            }
            StatementPlanKind::Delete {
                selection: Some(expr),
                ..
            } => {
                accesses.visit_expr(expr).unwrap();
            }
            _ => {}
        }

        accesses.0
    }

    macro_rules! test {
        ($actual:expr, $expected:expr, $name:literal) => {
            let actual = collect_scan_accesses(&$actual);
            let expected: &[(&str, TableAccessPlan)] = &$expected;
            let expected = expected
                .iter()
                .map(|(source, access)| ((*source).to_owned(), access.clone()))
                .collect::<Vec<_>>();

            assert_eq!(actual, expected, $name);
        };
    }

    #[test]
    fn column_names_and_aliases() {
        let storage = run("CREATE TABLE Item (a INT, b INT, c INT, d INT)");

        let sql = "SELECT COALESCE(b, 0) FROM Item WHERE c > 0 ORDER BY d";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b", "c", "d"]))];
        test!(
            actual,
            expected,
            "projection, filter, and order columns:\n{sql}"
        );

        let actual = plan_scan_columns(actual).unwrap();
        let expected = [("Item", projected_scan(&["b", "c", "d"]))];
        test!(
            actual,
            expected,
            "repeat planning retains column dependencies:\n{sql}"
        );

        let sql = "SELECT y FROM Item AS i(x, y, z, w)";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["b"]))];
        test!(
            actual,
            expected,
            "positional alias resolves to original name:\n{sql}"
        );

        let sql = "SELECT b AS a FROM Item ORDER BY a";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["a", "b"]))];
        test!(
            actual,
            expected,
            "keeps both projection and source alias candidates:\n{sql}"
        );

        let sql = "SELECT x FROM Item AS i(x, x, z, w)";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["a", "b"]))];
        test!(
            actual,
            expected,
            "keeps columns sharing a positional alias:\n{sql}"
        );

        let sql = "SELECT Item.a FROM Item AS i";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&[]))];
        test!(
            actual,
            expected,
            "table name does not match an aliased source:\n{sql}"
        );

        let sql = "SELECT missing FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&[]))];
        test!(actual, expected, "unknown name adds no scan column:\n{sql}");

        let sql = "SELECT x FROM Item AS i(x, y, z, w, extra)";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["a"]))];
        test!(
            actual,
            expected,
            "resolves existing columns with excess aliases:\n{sql}"
        );
    }

    #[test]
    fn wildcard_and_aggregates() {
        let storage = run("CREATE TABLE Item (a INT, b INT, c INT)");

        let sql = "SELECT * FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", FullScan)];
        test!(
            actual,
            expected,
            "wildcard requires all source columns:\n{sql}"
        );

        let sql = "SELECT i.*, i.b FROM Item i";
        let actual = plan(&storage, sql);
        let expected = [("i", FullScan)];
        test!(
            actual,
            expected,
            "qualified wildcard requires all matching columns:\n{sql}"
        );

        let sql = "SELECT 1 FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&[]))];
        test!(
            actual,
            expected,
            "constant projection requires rows without columns:\n{sql}"
        );

        let sql = "SELECT COUNT(*) FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&[]))];
        test!(
            actual,
            expected,
            "count wildcard requires rows without columns:\n{sql}"
        );

        let sql = "SELECT COUNT(*) FROM Item WHERE b > 0";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b"]))];
        test!(
            actual,
            expected,
            "count wildcard retains filter columns:\n{sql}"
        );

        let sql = "SELECT COUNT(DISTINCT *) FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", FullScan)];
        test!(
            actual,
            expected,
            "distinct wildcard expression requires all columns:\n{sql}"
        );

        let sql = "SELECT DISTINCT b FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b"]))];
        test!(
            actual,
            expected,
            "distinct projection requires selected columns:\n{sql}"
        );

        let sql = "
            SELECT SUM(b)
            FROM Item
            GROUP BY c
            HAVING MAX(b) > 0
        ";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b", "c"]))];
        test!(
            actual,
            expected,
            "aggregate, group, and having columns:\n{sql}"
        );
    }

    #[test]
    fn aggregate_slots() {
        let storage = run("CREATE TABLE Item (a INT, b INT, c INT)");
        let aggregate = AggregateExprPlan {
            func: AggregateFunctionPlan::Count(CountArgExprPlan::Wildcard),
            distinct: true,
            slot: Some(0),
        };
        let input = StatementPlanKind::Query(QueryPlan::Project(ProjectPlan {
            input: ProjectInputPlan::Aggregation(AggregationPlan {
                input: AggregationInputPlan::Source(table_source("Item")),
                group_by: Vec::new(),
                aggregate_slots: vec![aggregate],
            }),
            projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                expr: ExprPlan::Value(Value::I64(1)),
                label: "1".to_owned(),
            }]),
        }));
        let input = plan_schema_map(&storage, input.into()).unwrap();
        let actual = plan_scan_columns(input).unwrap();
        let expected = [("Item", FullScan)];
        test!(
            actual,
            expected,
            "distinct wildcard in aggregate slots requires all columns"
        );
    }

    #[test]
    fn joins_and_nested_scopes() {
        let storage = run("
            CREATE TABLE Item (a INT, b INT, c INT);
            CREATE TABLE Ref (ref INT, payload TEXT);
        ");

        let sql = "
            SELECT l.a, r.b
            FROM Item l
            JOIN Item r ON l.c = r.c
        ";
        let actual = plan(&storage, sql);
        let expected = [
            ("l", projected_scan(&["a", "c"])),
            ("r", projected_scan(&["b", "c"])),
        ];
        test!(actual, expected, "columns per joined source:\n{sql}");

        let input = table("Item")
            .alias_as("l")
            .select()
            .join_as("Item", "r")
            .hash_executor("r.c", "l.c")
            .project("l.a, r.b")
            .build()
            .unwrap();
        let input = plan_schema_map(&storage, input).unwrap();
        let actual = plan_scan_columns(input).unwrap();
        let expected = [
            ("l", projected_scan(&["a", "c"])),
            ("r", projected_scan(&["b", "c"])),
        ];
        test!(
            actual,
            expected,
            "projection and hash keys contribute source columns"
        );

        let sql = "SELECT a FROM Item l JOIN Item r ON l.c = r.c";
        let actual = plan(&storage, sql);
        let expected = [
            ("l", projected_scan(&["a", "c"])),
            ("r", projected_scan(&["a", "c"])),
        ];
        test!(
            actual,
            expected,
            "unqualified name retains matching source columns:\n{sql}"
        );

        let sql = "SELECT l.* FROM Item l LEFT JOIN Item r ON l.c = r.c";
        let actual = plan(&storage, sql);
        let expected = [("l", FullScan), ("r", projected_scan(&["c"]))];
        test!(
            actual,
            expected,
            "qualified wildcard leaves joined source columns selective:\n{sql}"
        );

        let sql = "
            SELECT i.a
            FROM Item i
            WHERE EXISTS (
                SELECT 1 FROM Ref r WHERE r.ref = i.b
            )
        ";
        let actual = plan(&storage, sql);
        let expected = [
            ("i", projected_scan(&["a", "b"])),
            ("r", projected_scan(&["ref"])),
        ];
        test!(
            actual,
            expected,
            "correlated subquery retains outer columns:\n{sql}"
        );

        let sql = "
            SELECT 1
            FROM Item AS i(x, y, z)
            WHERE EXISTS (
                SELECT i.x FROM Item AS i(y, z, x)
            )
        ";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["a"])), ("i", projected_scan(&["c"]))];
        test!(
            actual,
            expected,
            "each scope resolves matching positional aliases independently:\n{sql}"
        );

        let sql = "
            SELECT i.a
            FROM Item i
            WHERE EXISTS (SELECT * FROM Ref)
        ";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["a"])), ("Ref", FullScan)];
        test!(
            actual,
            expected,
            "nested wildcard applies to its own sources:\n{sql}"
        );

        let sql = "
            SELECT i.a
            FROM Item i
            WHERE EXISTS (SELECT COUNT(DISTINCT *) FROM Ref)
        ";
        let actual = plan(&storage, sql);
        let expected = [("i", FullScan), ("Ref", FullScan)];
        test!(
            actual,
            expected,
            "distinct wildcard retains all visible source columns:\n{sql}"
        );

        let sql = "SELECT d.x FROM (SELECT a AS x, b FROM Item) d";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["a", "b"]))];
        test!(
            actual,
            expected,
            "derived query retains its projected columns:\n{sql}"
        );

        let sql = "SELECT (SELECT i.b FROM SERIES(i.c)) FROM Item i";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["b", "c"]))];
        test!(
            actual,
            expected,
            "series and nested projection reference outer columns:\n{sql}"
        );

        let sql = "
            SELECT
                (SELECT i.b FROM Item i),
                (SELECT i.c FROM Item i)
            FROM Item outer_item
        ";
        let actual = plan(&storage, sql);
        let expected = [
            ("outer_item", projected_scan(&[])),
            ("i", projected_scan(&["b"])),
            ("i", projected_scan(&["c"])),
        ];
        test!(
            actual,
            expected,
            "sibling subqueries keep reused aliases independent:\n{sql}"
        );

        let sql = "
            SELECT
                (SELECT COUNT(DISTINCT *) FROM Item i),
                (SELECT i.c FROM Item i)
            FROM Item outer_item
        ";
        let actual = plan(&storage, sql);
        let expected = [
            ("outer_item", FullScan),
            ("i", FullScan),
            ("i", projected_scan(&["c"])),
        ];
        test!(
            actual,
            expected,
            "distinct wildcard propagates outward without affecting siblings:\n{sql}"
        );

        let sql = "VALUES ((SELECT b FROM Item))";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b"]))];
        test!(actual, expected, "subquery inside values:\n{sql}");
    }

    #[test]
    fn excluded_sources_and_lookup_dependencies() {
        let storage = run("
            CREATE TABLE Item (a INT, b INT);
            CREATE TABLE Lookup (id INT PRIMARY KEY, payload TEXT);
            CREATE TABLE Logs;
        ");

        let sql = "SELECT i.b, l.name FROM Item i JOIN Logs l ON i.a = l.id";
        let actual = plan(&storage, sql);
        let expected = [("i", projected_scan(&["a", "b"])), ("l", FullScan)];
        test!(
            actual,
            expected,
            "schemaful source stays selective beside schemaless source:\n{sql}"
        );

        let sql = "SELECT COUNT(*) FROM Logs";
        let actual = plan(&storage, sql);
        let expected = [("Logs", FullScan)];
        test!(
            actual,
            expected,
            "schemaless source keeps all columns:\n{sql}"
        );

        let sql = "SELECT * FROM Missing";
        let actual = plan(&storage, sql);
        let expected = [("Missing", FullScan)];
        test!(actual, expected, "missing schema keeps all columns:\n{sql}");

        let input = table("Item")
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
        let input = plan_schema_map(&storage, input).unwrap();
        let actual = plan_scan_columns(input).unwrap();
        let expected = [("Item", projected_scan(&["a", "b"]))];
        test!(
            actual,
            expected,
            "primary-key lookup references outer source"
        );

        let input = table("Item")
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
        let input = plan_schema_map(&storage, input).unwrap();
        let actual = plan_scan_columns(input).unwrap();
        let expected = [("Item", projected_scan(&["a", "b"]))];
        test!(actual, expected, "index lookup references outer source");
    }

    #[test]
    fn statement_sources() {
        let storage = run("
            CREATE TABLE Item (a INT, b INT);
            CREATE TABLE Ref (ref INT, payload TEXT);
        ");

        let sql = "INSERT INTO Item SELECT ref, 1 FROM Ref";
        let actual = plan(&storage, sql);
        let expected = [("Ref", projected_scan(&["ref"]))];
        test!(actual, expected, "columns in INSERT source:\n{sql}");

        let sql = "CREATE TABLE Copy AS SELECT b FROM Item";
        let actual = plan(&storage, sql);
        let expected = [("Item", projected_scan(&["b"]))];
        test!(actual, expected, "columns in CREATE TABLE source:\n{sql}");

        let sql = "
            UPDATE Item
            SET a = (SELECT ref FROM Ref WHERE ref = Item.b)
            WHERE EXISTS (
                SELECT 1 FROM Ref WHERE ref = Item.a
            )
        ";
        let actual = plan(&storage, sql);
        let expected = [
            ("Ref", projected_scan(&["ref"])),
            ("Ref", projected_scan(&["ref"])),
        ];
        test!(
            actual,
            expected,
            "columns in UPDATE assignment and filter subqueries:\n{sql}"
        );

        let sql = "
            DELETE FROM Item
            WHERE EXISTS (
                SELECT ref
                FROM Ref
                WHERE ref = Item.b
            )
        ";
        let actual = plan(&storage, sql);
        let expected = [("Ref", projected_scan(&["ref"]))];
        test!(actual, expected, "columns in DELETE subquery:\n{sql}");

        let sql = "UPDATE Item SET a = 1";
        let actual = plan(&storage, sql);
        let expected = [];
        test!(
            actual,
            expected,
            "UPDATE target is not a scan source:\n{sql}"
        );

        let sql = "DELETE FROM Item";
        let actual = plan(&storage, sql);
        let expected = [];
        test!(
            actual,
            expected,
            "DELETE target is not a scan source:\n{sql}"
        );

        let sql = "BEGIN";
        let actual = plan(&storage, sql);
        let expected = [];
        test!(
            actual,
            expected,
            "transaction statement has no scan sources:\n{sql}"
        );
    }

    #[test]
    fn unplanned_schema_map() {
        let actual = plan_scan_columns(statement("SELECT * FROM Item"));
        let expected = Err(PlannerError::UnplannedSchemaMap.into());
        assert_eq!(actual, expected);
    }
}
