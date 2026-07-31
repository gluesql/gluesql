use {
    crate::{
        ast::{ColumnDef, ColumnUniqueOption},
        data::Schema,
        plan::{
            ExprPlan, FilterInputPlan, JoinInputPlan, JoinOperatorPlan, JoinPlan, SourcePlan,
            TableAccessPlan, TableAliasPlan,
        },
    },
    std::{collections::HashMap, hash::BuildHasher},
};

pub(super) struct PrimaryKeyLookupCandidate {
    target: PrimaryKeyLookupTarget,
    joined_relations: Vec<JoinedRelation>,
}

impl PrimaryKeyLookupCandidate {
    pub(super) fn new<S: BuildHasher>(
        schema_map: &HashMap<String, Schema, S>,
        input: &FilterInputPlan,
    ) -> Option<Self> {
        let target = PrimaryKeyLookupTarget::new(schema_map, base_source(input))?;
        let mut joined_relations = Vec::new();
        if let FilterInputPlan::Join(join) = input {
            collect_joined_relations(schema_map, join, &mut joined_relations);
        }

        Some(Self {
            target,
            joined_relations,
        })
    }

    pub(super) fn contains(&self, key: &ExprPlan) -> bool {
        match key {
            ExprPlan::Identifier(column) => {
                self.target.primary_key_column == *column
                    && self
                        .joined_relations
                        .iter()
                        .all(|relation| !relation.contains_column(column))
            }
            ExprPlan::CompoundIdentifier { alias, ident } => {
                self.target.matches(alias, ident)
                    && self
                        .joined_relations
                        .iter()
                        .all(|relation| !relation.contains_aliased_column(alias, ident))
            }
            _ => false,
        }
    }
}

fn base_source(input: &FilterInputPlan) -> &SourcePlan {
    match input {
        FilterInputPlan::Source(relation) => relation,
        FilterInputPlan::Join(join) => join_base_source(join),
    }
}

fn join_base_source(join: &JoinPlan) -> &SourcePlan {
    match &join.input {
        JoinInputPlan::Source(relation) => relation,
        JoinInputPlan::Join(join) => join_base_source(join),
    }
}

fn collect_joined_relations<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    join: &JoinPlan,
    joined_relations: &mut Vec<JoinedRelation>,
) {
    if let JoinInputPlan::Join(input) = &join.input {
        collect_joined_relations(schema_map, input, joined_relations);
    }
    validate_join_operator(&join.join_operator);
    joined_relations.push(JoinedRelation::new(schema_map, &join.right));
}

fn validate_join_operator(join_operator: &JoinOperatorPlan) {
    // Keep this exhaustive so new join types require an explicit lookup-safety decision.
    match join_operator {
        JoinOperatorPlan::Inner(_) | JoinOperatorPlan::LeftOuter(_) => {}
    }
}

struct PrimaryKeyLookupTarget {
    alias: String,
    primary_key_column: String,
}

impl PrimaryKeyLookupTarget {
    fn new<S: BuildHasher>(
        schema_map: &HashMap<String, Schema, S>,
        relation: &SourcePlan,
    ) -> Option<Self> {
        let SourcePlan::Table(table) = relation else {
            return None;
        };
        if table.access != TableAccessPlan::FullScan {
            return None;
        }
        let column_defs = schema_map.get(&table.name)?.column_defs.as_ref()?;
        let primary_key_index = column_defs.iter().position(|ColumnDef { unique, .. }| {
            unique == &Some(ColumnUniqueOption { is_primary: true })
        })?;
        let columns = effective_columns(column_defs, table.alias.as_ref())?;
        let primary_key_column = columns.get(primary_key_index)?;
        if columns
            .iter()
            .position(|column| column == primary_key_column)
            != Some(primary_key_index)
        {
            return None;
        }

        Some(Self {
            alias: relation.alias_name().to_owned(),
            primary_key_column: primary_key_column.clone(),
        })
    }

    fn matches(&self, alias: &str, column: &str) -> bool {
        self.alias == alias && self.primary_key_column == column
    }
}

struct JoinedRelation {
    alias: String,
    columns: RelationColumns,
}

impl JoinedRelation {
    fn new<S: BuildHasher>(schema_map: &HashMap<String, Schema, S>, relation: &SourcePlan) -> Self {
        let columns = match relation {
            SourcePlan::Table(table) => schema_map
                .get(&table.name)
                .and_then(|schema| schema.column_defs.as_deref())
                .and_then(|column_defs| effective_columns(column_defs, table.alias.as_ref()))
                .map_or(RelationColumns::Unknown, RelationColumns::Known),
            SourcePlan::Derived(_) | SourcePlan::Series(_) | SourcePlan::Dictionary(_) => {
                RelationColumns::Unknown
            }
        };

        Self {
            alias: relation.alias_name().to_owned(),
            columns,
        }
    }

    fn contains_column(&self, target: &str) -> bool {
        match &self.columns {
            RelationColumns::Known(columns) => columns.iter().any(|column| column == target),
            RelationColumns::Unknown => true,
        }
    }

    fn contains_aliased_column(&self, target_alias: &str, target_column: &str) -> bool {
        self.alias == target_alias && self.contains_column(target_column)
    }
}

enum RelationColumns {
    Known(Vec<String>),
    Unknown,
}

fn effective_columns(
    column_defs: &[ColumnDef],
    alias: Option<&TableAliasPlan>,
) -> Option<Vec<String>> {
    let mut columns = column_defs
        .iter()
        .map(|column_def| column_def.name.clone())
        .collect::<Vec<_>>();
    let Some(alias) = alias else {
        return Some(columns);
    };
    if alias.columns.len() > columns.len() {
        return None;
    }

    columns
        .iter_mut()
        .zip(alias.columns.iter())
        .for_each(|(column, alias)| column.clone_from(alias));

    Some(columns)
}

#[cfg(test)]
mod tests {
    use {
        super::PrimaryKeyLookupCandidate,
        crate::{
            ast::Literal,
            data::Schema,
            parse_sql::parse,
            plan::{
                AggregationInputPlan, ExprPlan, FilterInputPlan, ProjectInputPlan, QueryPlan,
                SourcePlan, StatementPlan, TableAccessPlan, TableSourcePlan,
            },
            translate::translate,
        },
        std::collections::HashMap,
    };

    fn schema_map(ddls: &[&str]) -> HashMap<String, Schema> {
        ddls.iter()
            .map(|ddl| {
                let schema = Schema::from_ddl(ddl).unwrap();

                (schema.table_name.clone(), schema)
            })
            .collect()
    }

    fn try_parse_from(sql: &str) -> Option<FilterInputPlan> {
        let parsed = parse(sql).unwrap().into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        match statement {
            StatementPlan::Query(QueryPlan::Project(project)) => Some(match project.input {
                ProjectInputPlan::Source(relation) => FilterInputPlan::Source(relation),
                ProjectInputPlan::Join(join) => FilterInputPlan::Join(join),
                ProjectInputPlan::Filter(filter) => filter.input,
                ProjectInputPlan::Aggregation(aggregation) => match aggregation.input {
                    AggregationInputPlan::Source(relation) => FilterInputPlan::Source(relation),
                    AggregationInputPlan::Join(join) => FilterInputPlan::Join(join),
                    AggregationInputPlan::Filter(filter) => filter.input,
                },
                ProjectInputPlan::Having(having) => match having.input.input {
                    AggregationInputPlan::Source(relation) => FilterInputPlan::Source(relation),
                    AggregationInputPlan::Join(join) => FilterInputPlan::Join(join),
                    AggregationInputPlan::Filter(filter) => filter.input,
                },
            }),
            _ => None,
        }
    }

    fn parse_from(sql: &str) -> FilterInputPlan {
        try_parse_from(sql).expect("expected select plan")
    }

    fn identifier(column: &str) -> ExprPlan {
        ExprPlan::Identifier(column.to_owned())
    }

    fn qualified(alias: &str, column: &str) -> ExprPlan {
        ExprPlan::CompoundIdentifier {
            alias: alias.to_owned(),
            ident: column.to_owned(),
        }
    }

    #[test]
    fn matches_qualified_and_unqualified_identifiers() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY, project_id INTEGER);",
            "CREATE TABLE Projects (project_id INTEGER PRIMARY KEY, name TEXT);",
        ]);
        let from = parse_from("SELECT * FROM Tasks t JOIN Projects p");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(candidate.contains(&identifier("id")));
        assert!(candidate.contains(&qualified("t", "id")));
        assert!(!candidate.contains(&qualified("p", "project_id")));
        assert!(!candidate.contains(&identifier("project_id")));
    }

    #[test]
    fn accepts_left_outer_join_that_preserves_the_lookup_target() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY, project_id INTEGER);",
            "CREATE TABLE Projects (project_id INTEGER PRIMARY KEY, name TEXT);",
        ]);
        let from =
            parse_from("SELECT * FROM Tasks t LEFT JOIN Projects p ON p.project_id = t.project_id");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(candidate.contains(&qualified("t", "id")));
    }

    #[test]
    fn rejects_non_select_test_inputs() {
        assert!(try_parse_from("VALUES (1)").is_none());
        assert!(try_parse_from("CREATE TABLE Tasks (id INTEGER)").is_none());
    }

    #[test]
    fn requires_an_installable_first_relation() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Logs (id INTEGER);",
        ]);
        let from = parse_from("SELECT * FROM Logs");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let from = FilterInputPlan::Source(SourcePlan::Table(TableSourcePlan {
            name: "Tasks".to_owned(),
            alias: None,
            access: TableAccessPlan::PrimaryKey {
                expr: ExprPlan::Value(crate::data::Value::I64(1)),
            },
        }));

        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());
    }

    #[test]
    fn rejects_joined_column_conflicts_and_unknown_columns() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY, project_id INTEGER);",
            "CREATE TABLE Links (task_id INTEGER);",
            "CREATE TABLE Projects (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Schemaless;",
        ]);
        let from = parse_from("SELECT * FROM Tasks t JOIN Links l JOIN Projects p");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(!candidate.contains(&identifier("id")));
        assert!(candidate.contains(&qualified("t", "id")));

        for sql in [
            "SELECT * FROM Tasks t JOIN UnknownRelation u",
            "SELECT * FROM Tasks t JOIN Schemaless s",
            "SELECT * FROM Tasks t JOIN (SELECT * FROM Tasks) d",
            "SELECT * FROM Tasks t JOIN SERIES(1) n",
            "SELECT * FROM Tasks t JOIN GLUE_TABLES g",
        ] {
            let from = parse_from(sql);
            let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

            assert!(!candidate.contains(&identifier("id")), "{sql}");
            assert!(candidate.contains(&qualified("t", "id")), "{sql}");
        }
    }

    #[test]
    fn uses_effective_positional_column_aliases() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (task_id INTEGER PRIMARY KEY, project_id INTEGER, done BOOLEAN);",
            "CREATE TABLE Projects (id INTEGER PRIMARY KEY, name TEXT);",
        ]);
        let from = parse_from(
            "SELECT * FROM Tasks AS t(id, project_id, done) \
             JOIN Projects AS p(task_id, name)",
        );
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(candidate.contains(&identifier("id")));
        assert!(candidate.contains(&qualified("t", "id")));
        assert!(!candidate.contains(&identifier("task_id")));
        assert!(!candidate.contains(&qualified("t", "task_id")));
        assert!(!candidate.contains(&qualified("p", "task_id")));
    }

    #[test]
    fn uses_partial_positional_column_aliases() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (task_id INTEGER PRIMARY KEY, project_id INTEGER, done BOOLEAN);",
            "CREATE TABLE Projects (id INTEGER PRIMARY KEY, name TEXT);",
        ]);
        let from = parse_from(
            "SELECT * FROM Tasks AS t(id) \
             JOIN Projects AS p(project_id)",
        );
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(candidate.contains(&identifier("id")));
        assert!(candidate.contains(&qualified("t", "id")));
        assert!(!candidate.contains(&identifier("project_id")));
    }

    #[test]
    fn rejects_unsupported_targets_and_keys() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Schemaless;",
        ]);
        let from = parse_from("SELECT * FROM (SELECT * FROM Tasks) AS t");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let from = parse_from("SELECT * FROM UnknownRelation");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let from = parse_from("SELECT * FROM Schemaless");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let from = parse_from("SELECT * FROM Tasks AS t(id, extra)");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let from = parse_from("SELECT * FROM Tasks");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();
        assert!(!candidate.contains(&ExprPlan::Literal(Literal::Number(1.into()))));
    }

    #[test]
    fn blocks_a_qualified_key_when_a_join_reuses_the_target_alias() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Projects (id INTEGER PRIMARY KEY);",
        ]);
        let from = parse_from("SELECT * FROM Tasks t JOIN Projects t");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(!candidate.contains(&qualified("t", "id")));
    }

    #[test]
    fn treats_invalid_joined_aliases_as_unknown_columns() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Projects (project_id INTEGER PRIMARY KEY);",
        ]);
        let from = parse_from("SELECT * FROM Tasks t JOIN Projects p(a, b)");
        let candidate = PrimaryKeyLookupCandidate::new(&schema_map, &from).unwrap();

        assert!(!candidate.contains(&identifier("id")));
        assert!(candidate.contains(&qualified("t", "id")));
    }

    #[test]
    fn rejects_a_primary_key_alias_shadowed_by_an_earlier_column() {
        let schema_map =
            schema_map(&["CREATE TABLE Tasks (project_id INTEGER, task_id INTEGER PRIMARY KEY);"]);
        let from = parse_from("SELECT * FROM Tasks AS t(id, id)");

        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());
    }
}
