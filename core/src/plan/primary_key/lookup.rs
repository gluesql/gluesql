use {
    crate::{
        ast::{ColumnDef, ColumnUniqueOption},
        data::Schema,
        plan::{ExprPlan, JoinOperatorPlan, TableAliasPlan, TableFactorPlan, TableWithJoinsPlan},
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
        from: &TableWithJoinsPlan,
    ) -> Option<Self> {
        if !from
            .joins
            .iter()
            .all(|join| preserves_lookup_target(&join.join_operator))
        {
            return None;
        }

        let target = PrimaryKeyLookupTarget::new(schema_map, &from.relation)?;
        let joined_relations = from
            .joins
            .iter()
            .map(|join| JoinedRelation::new(schema_map, &join.relation))
            .collect();

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

fn preserves_lookup_target(join_operator: &JoinOperatorPlan) -> bool {
    // Keep this exhaustive so new join types require an explicit lookup-safety decision.
    match join_operator {
        JoinOperatorPlan::Inner(_) | JoinOperatorPlan::LeftOuter(_) => true,
    }
}

struct PrimaryKeyLookupTarget {
    alias: String,
    primary_key_column: String,
}

impl PrimaryKeyLookupTarget {
    fn new<S: BuildHasher>(
        schema_map: &HashMap<String, Schema, S>,
        relation: &TableFactorPlan,
    ) -> Option<Self> {
        let TableFactorPlan::Table {
            name,
            alias,
            index: None,
        } = relation
        else {
            return None;
        };
        let column_defs = schema_map.get(name)?.column_defs.as_ref()?;
        let primary_key_index = column_defs.iter().position(|ColumnDef { unique, .. }| {
            unique == &Some(ColumnUniqueOption { is_primary: true })
        })?;
        let columns = effective_columns(column_defs, alias.as_ref())?;
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
    fn new<S: BuildHasher>(
        schema_map: &HashMap<String, Schema, S>,
        relation: &TableFactorPlan,
    ) -> Self {
        let columns = match relation {
            TableFactorPlan::Table { name, alias, .. } => schema_map
                .get(name)
                .and_then(|schema| schema.column_defs.as_deref())
                .and_then(|column_defs| effective_columns(column_defs, alias.as_ref()))
                .map_or(RelationColumns::Unknown, RelationColumns::Known),
            TableFactorPlan::Derived { .. }
            | TableFactorPlan::Series { .. }
            | TableFactorPlan::Dictionary { .. } => RelationColumns::Unknown,
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
                ExprPlan, IndexItemPlan, SetExprPlan, StatementPlan, TableFactorPlan,
                TableWithJoinsPlan,
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

    fn parse_from(sql: &str) -> TableWithJoinsPlan {
        let parsed = parse(sql).unwrap().into_iter().next().unwrap();
        let statement = StatementPlan::from(translate(&parsed).unwrap());
        let query = match statement {
            StatementPlan::Query(query) => Some(query),
            _ => None,
        }
        .expect("expected query plan");
        let select = match query.body {
            SetExprPlan::Select(select) => Some(select),
            SetExprPlan::Values(_) => None,
        }
        .expect("expected select plan");

        select.from
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
    fn requires_an_installable_first_relation() {
        let schema_map = schema_map(&[
            "CREATE TABLE Tasks (id INTEGER PRIMARY KEY);",
            "CREATE TABLE Logs (id INTEGER);",
        ]);
        let from = parse_from("SELECT * FROM Logs");
        assert!(PrimaryKeyLookupCandidate::new(&schema_map, &from).is_none());

        let mut from = parse_from("SELECT * FROM Tasks");
        let index = match &mut from.relation {
            TableFactorPlan::Table { index, .. } => Some(index),
            _ => None,
        }
        .expect("expected table relation");
        *index = Some(IndexItemPlan::PrimaryKey(ExprPlan::Literal(
            Literal::Number(1.into()),
        )));

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
