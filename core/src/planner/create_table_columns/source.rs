use {
    super::{
        Column, Context,
        expr::ExprType,
        query::{self, QueryColumns},
    },
    crate::{
        ast::{DataType, Dictionary},
        data::SCHEMALESS_DOC_COLUMN,
        plan::{
            ColumnDefPlan, FilterInputPlan, ProjectInputPlan, ProjectionPlan, QueryPlan,
            SelectItemPlan, SourcePlan, TableColumnsPlan,
        },
        planner::PlannerError,
        result::Result,
    },
    std::hash::BuildHasher,
};

pub(super) struct Relation {
    pub alias: String,
    pub columns: Vec<Column>,
    pub dynamic: bool,
}

#[derive(Default)]
pub(super) struct Scope<'a> {
    pub relations: Vec<Relation>,
    pub outer: Option<&'a Scope<'a>>,
}

impl Scope<'_> {
    pub fn column(&self, alias: Option<&str>, name: &str) -> Result<ExprType> {
        let mut found = None;
        let mut matched_alias = false;
        for relation in &self.relations {
            if alias.is_some_and(|alias| alias != relation.alias) {
                continue;
            }
            matched_alias = true;
            let columns = relation.columns.iter().filter(|column| column.name == name);
            let mut matched_column = false;
            for column in columns {
                if found.is_some() {
                    return Err(PlannerError::ColumnReferenceAmbiguous(name.to_owned()).into());
                }
                found = Some(column.expr_type.clone());
                matched_column = true;
            }
            if !matched_column && relation.dynamic {
                if found.is_some() {
                    return Err(PlannerError::ColumnReferenceAmbiguous(name.to_owned()).into());
                }
                found = Some(ExprType::Unknown);
            }
        }
        if let Some(found) = found {
            return Ok(found);
        }
        if (alias.is_none() || !matched_alias)
            && let Some(outer) = self.outer
        {
            return outer.column(alias, name);
        }
        Err(PlannerError::CtasColumnNotFound(
            alias.map_or_else(|| name.to_owned(), |alias| format!("{alias}.{name}")),
        )
        .into())
    }

    pub fn wildcard(&self, alias: Option<&str>) -> Result<Vec<Column>> {
        let mut columns = Vec::new();
        let mut found = false;
        for relation in &self.relations {
            if alias.is_some_and(|alias| alias != relation.alias) {
                continue;
            }
            found = true;
            if relation.dynamic {
                return Err(
                    PlannerError::CtasWildcardColumnsUnknown(relation.alias.clone()).into(),
                );
            }
            columns.extend(relation.columns.iter().cloned());
        }
        if !found && let Some(alias) = alias {
            return Err(PlannerError::CtasAliasNotFound(alias.to_owned()).into());
        }
        Ok(columns)
    }
}

pub(super) fn infer_scope<'a, S: BuildHasher>(
    context: &mut Context<'_, S>,
    query: &QueryPlan,
    outer: Option<&'a Scope<'a>>,
) -> Result<Scope<'a>> {
    let mut scope = Scope {
        relations: Vec::new(),
        outer,
    };
    if let Some(project) = query.project() {
        for source in
            std::iter::once(project.input.base_source()).chain(project.input.joined_sources())
        {
            let relation = infer_source(context, source, Some(&scope))?;
            scope.relations.push(relation);
        }
    }
    Ok(scope)
}

pub(super) fn infer_source<S: BuildHasher>(
    context: &mut Context<'_, S>,
    source: &SourcePlan,
    outer: Option<&Scope<'_>>,
) -> Result<Relation> {
    let (mut columns, dynamic, aliases) = match source {
        SourcePlan::Table(table) => {
            let schema = context
                .schemas
                .get(&table.name)
                .ok_or_else(|| PlannerError::CtasSourceTableNotFound(table.name.clone()))?;
            let columns = match &schema.column_defs {
                Some(columns) => columns
                    .iter()
                    .map(|column| Column::new(&column.name, column.data_type.clone()))
                    .collect(),
                None => vec![Column::new(SCHEMALESS_DOC_COLUMN, DataType::Map)],
            };
            (
                columns,
                false,
                table.alias.as_ref().map(|alias| alias.columns.as_slice()),
            )
        }
        SourcePlan::Derived(derived) => {
            let columns = match query::infer_query(context, &derived.query, outer)? {
                QueryColumns::Columns(columns) => columns,
                QueryColumns::Schemaless => vec![Column::new(SCHEMALESS_DOC_COLUMN, DataType::Map)],
            };
            (columns, false, Some(derived.alias.columns.as_slice()))
        }
        SourcePlan::Series(_) => (vec![Column::new("N", DataType::Int)], false, None),
        SourcePlan::Dictionary(dictionary) => {
            use DataType::{Boolean, Int, Text};
            let columns = match dictionary.dictionary {
                Dictionary::GlueTables => vec![
                    Column::new("TABLE_NAME", Text),
                    Column::new("COMMENT", Text),
                ],
                Dictionary::GlueTableColumns => vec![
                    Column::new("TABLE_NAME", Text),
                    Column::new("COLUMN_NAME", Text),
                    Column::new("COLUMN_ID", Int),
                    Column::new("NULLABLE", Boolean),
                    Column::new("KEY", Text),
                    Column::new("DEFAULT", Text),
                    Column::new("COMMENT", Text),
                ],
                Dictionary::GlueIndexes => vec![
                    Column::new("TABLE_NAME", Text),
                    Column::new("INDEX_NAME", Text),
                    Column::new("ORDER", Text),
                    Column::new("EXPRESSION", Text),
                    Column::new("UNIQUENESS", Boolean),
                ],
                Dictionary::GlueObjects => vec![
                    Column::new("OBJECT_NAME", Text),
                    Column::new("OBJECT_TYPE", Text),
                ],
            };
            (
                columns,
                dictionary.dictionary == Dictionary::GlueObjects,
                None,
            )
        }
    };
    if let Some(aliases) = aliases {
        if aliases.len() > columns.len() {
            return Err(PlannerError::CtasAliasColumnsOverflow {
                expected: columns.len(),
                found: aliases.len(),
            }
            .into());
        }
        for (column, alias) in columns.iter_mut().zip(aliases) {
            column.name.clone_from(alias);
        }
    }
    Ok(Relation {
        alias: source.alias_name().to_owned(),
        columns,
        dynamic,
    })
}

pub(super) fn copy_columns<S: BuildHasher>(
    context: &Context<'_, S>,
    query: &QueryPlan,
) -> Result<Option<TableColumnsPlan>> {
    let Some(project) = query.project() else {
        return Ok(None);
    };
    let source = match &project.input {
        ProjectInputPlan::Source(source) => source,
        ProjectInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Source(source) => source,
            FilterInputPlan::InnerJoin(_) | FilterInputPlan::LeftOuterJoin(_) => return Ok(None),
        },
        ProjectInputPlan::InnerJoin(_)
        | ProjectInputPlan::LeftOuterJoin(_)
        | ProjectInputPlan::Aggregation(_)
        | ProjectInputPlan::Having(_) => return Ok(None),
    };
    let whole_source = match &project.projection {
        ProjectionPlan::SchemalessMap => true,
        ProjectionPlan::SelectItems(items) => match items.as_slice() {
            [SelectItemPlan::Wildcard] => true,
            [SelectItemPlan::QualifiedWildcard(alias)] => alias == source.alias_name(),
            _ => false,
        },
    };
    if !whole_source {
        return Ok(None);
    }
    match source {
        SourcePlan::Table(table) => {
            let schema = context
                .schemas
                .get(&table.name)
                .ok_or_else(|| PlannerError::CtasSourceTableNotFound(table.name.clone()))?;
            let Some(columns) = &schema.column_defs else {
                return Ok(Some(TableColumnsPlan::Schemaless));
            };
            let mut columns: Vec<_> = columns.iter().cloned().map(ColumnDefPlan::from).collect();
            if let Some(alias) = &table.alias {
                if alias.columns.len() > columns.len() {
                    return Err(PlannerError::CtasAliasColumnsOverflow {
                        expected: columns.len(),
                        found: alias.columns.len(),
                    }
                    .into());
                }
                for (column, name) in columns.iter_mut().zip(&alias.columns) {
                    column.name.clone_from(name);
                }
            }
            Ok(Some(TableColumnsPlan::Columns(columns)))
        }
        SourcePlan::Series(_) => Ok(Some(TableColumnsPlan::Columns(vec![ColumnDefPlan {
            name: "N".to_owned(),
            data_type: DataType::Int,
            nullable: false,
            default: None,
            unique: None,
            comment: None,
        }]))),
        SourcePlan::Derived(_) | SourcePlan::Dictionary(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Column, Context, ExprType, Relation, Scope, copy_columns, infer_scope},
        crate::{
            ast::DataType,
            mock::run,
            parse_sql::parse,
            plan::{QueryPlan, StatementPlan, StatementPlanKind, TableColumnsPlan},
            planner::{PlannerError, fetch_schema_map, plan_aggregate},
            result::Error,
            translate::translate,
        },
    };

    fn statement(sql: &str) -> StatementPlan {
        plan_aggregate(translate(&parse(sql).expect(sql)[0]).expect(sql).into())
    }

    fn query(sql: &str) -> QueryPlan {
        let StatementPlanKind::Query(query) = statement(sql).kind else {
            panic!("{sql}")
        };
        query
    }

    #[test]
    fn source_columns() {
        let storage = run("CREATE TABLE src (id INT, label TEXT); CREATE TABLE docs;");
        let schemas =
            fetch_schema_map(&storage, &statement("SELECT * FROM src JOIN docs")).unwrap();
        let mut context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut test = |sql: &str, expected: Vec<Column>| {
            let scope = infer_scope(&mut context, &query(sql), None).unwrap();
            assert_eq!(scope.wildcard(None), Ok(expected), "{sql}");
        };
        test(
            "SELECT * FROM src s(renamed)",
            vec![
                Column::new("renamed", DataType::Int),
                Column::new("label", DataType::Text),
            ],
        );
        test(
            "SELECT * FROM docs",
            vec![Column::new("_doc", DataType::Map)],
        );
        test(
            "SELECT * FROM (SELECT id, label FROM src) d(x)",
            vec![
                Column::new("x", DataType::Int),
                Column::new("label", DataType::Text),
            ],
        );
        test(
            "SELECT * FROM SERIES(1) s(renamed)",
            vec![Column::new("N", DataType::Int)],
        );
        test(
            "SELECT * FROM GLUE_TABLES",
            vec![
                Column::new("TABLE_NAME", DataType::Text),
                Column::new("COMMENT", DataType::Text),
            ],
        );
        test(
            "SELECT * FROM GLUE_TABLE_COLUMNS",
            vec![
                Column::new("TABLE_NAME", DataType::Text),
                Column::new("COLUMN_NAME", DataType::Text),
                Column::new("COLUMN_ID", DataType::Int),
                Column::new("NULLABLE", DataType::Boolean),
                Column::new("KEY", DataType::Text),
                Column::new("DEFAULT", DataType::Text),
                Column::new("COMMENT", DataType::Text),
            ],
        );
        test(
            "SELECT * FROM GLUE_INDEXES",
            vec![
                Column::new("TABLE_NAME", DataType::Text),
                Column::new("INDEX_NAME", DataType::Text),
                Column::new("ORDER", DataType::Text),
                Column::new("EXPRESSION", DataType::Text),
                Column::new("UNIQUENESS", DataType::Boolean),
            ],
        );

        let scope = infer_scope(&mut context, &query("SELECT * FROM GLUE_OBJECTS"), None).unwrap();
        assert_eq!(
            scope.column(None, "OBJECT_NAME"),
            Ok(ExprType::Known(DataType::Text))
        );
        assert_eq!(scope.column(None, "metadata"), Ok(ExprType::Unknown));
        assert_eq!(
            scope.wildcard(None),
            Err(Error::Planner(PlannerError::CtasWildcardColumnsUnknown(
                "GLUE_OBJECTS".to_owned()
            )))
        );

        let actual = infer_scope(&mut context, &query("SELECT * FROM src s(a, b, c)"), None).err();
        let expected = Some(Error::Planner(PlannerError::CtasAliasColumnsOverflow {
            expected: 2,
            found: 3,
        }));
        assert_eq!(actual, expected, "excess table aliases");
        let actual = infer_scope(
            &mut context,
            &query("SELECT * FROM (SELECT id FROM src) d(a, b)"),
            None,
        )
        .err();
        let expected = Some(Error::Planner(PlannerError::CtasAliasColumnsOverflow {
            expected: 1,
            found: 2,
        }));
        assert_eq!(actual, expected, "excess derived aliases");
        let actual = infer_scope(&mut context, &query("SELECT * FROM missing"), None).err();
        assert_eq!(
            actual,
            Some(Error::Planner(PlannerError::CtasSourceTableNotFound(
                "missing".to_owned()
            )))
        );
    }

    #[test]
    fn column_resolution() {
        let outer = Scope {
            relations: vec![Relation {
                alias: "outer".to_owned(),
                columns: vec![
                    Column::new("id", DataType::Int),
                    Column::new("extra", DataType::Text),
                ],
                dynamic: false,
            }],
            outer: None,
        };
        let scope = Scope {
            relations: vec![Relation {
                alias: "inner".to_owned(),
                columns: vec![Column::new("id", DataType::Float)],
                dynamic: false,
            }],
            outer: Some(&outer),
        };
        assert_eq!(
            scope.column(None, "id"),
            Ok(ExprType::Known(DataType::Float)),
            "nearest scope"
        );
        assert_eq!(
            scope.column(Some("outer"), "id"),
            Ok(ExprType::Known(DataType::Int)),
            "qualified outer column"
        );
        assert_eq!(
            scope.column(None, "extra"),
            Ok(ExprType::Known(DataType::Text)),
            "outer fallback"
        );
        assert_eq!(
            scope.column(Some("inner"), "extra"),
            Err(Error::Planner(PlannerError::CtasColumnNotFound(
                "inner.extra".to_owned()
            ))),
            "known alias does not fall through"
        );
        assert_eq!(
            scope.column(None, "missing"),
            Err(Error::Planner(PlannerError::CtasColumnNotFound(
                "missing".to_owned()
            )))
        );
        assert_eq!(
            scope.wildcard(Some("inner")),
            Ok(vec![Column::new("id", DataType::Float)])
        );
        assert_eq!(
            scope.wildcard(Some("missing")),
            Err(Error::Planner(PlannerError::CtasAliasNotFound(
                "missing".to_owned()
            )))
        );

        let scope = Scope {
            relations: vec![
                Relation {
                    alias: "a".to_owned(),
                    columns: vec![Column::new("id", DataType::Int)],
                    dynamic: false,
                },
                Relation {
                    alias: "b".to_owned(),
                    columns: vec![Column::new("id", DataType::Float)],
                    dynamic: false,
                },
            ],
            outer: None,
        };
        assert_eq!(
            scope.column(None, "id"),
            Err(Error::Planner(PlannerError::ColumnReferenceAmbiguous(
                "id".to_owned()
            )))
        );
        assert_eq!(
            scope.wildcard(None),
            Ok(vec![
                Column::new("id", DataType::Int),
                Column::new("id", DataType::Float)
            ]),
            "wildcard source order"
        );
        assert_eq!(
            scope.column(Some("b"), "id"),
            Ok(ExprType::Known(DataType::Float))
        );

        let scope = Scope {
            relations: vec![
                Relation {
                    alias: "fixed".to_owned(),
                    columns: vec![Column::new("id", DataType::Int)],
                    dynamic: false,
                },
                Relation {
                    alias: "metadata".to_owned(),
                    columns: Vec::new(),
                    dynamic: true,
                },
            ],
            outer: None,
        };
        assert_eq!(
            scope.column(None, "id"),
            Err(Error::Planner(PlannerError::ColumnReferenceAmbiguous(
                "id".to_owned()
            ))),
            "a dynamic field can conflict with a fixed column"
        );
    }

    #[test]
    fn column_definition_copy() {
        let storage = run(
            "CREATE TABLE src (id INT PRIMARY KEY, label TEXT DEFAULT 'new'); CREATE TABLE docs;",
        );
        let schemas =
            fetch_schema_map(&storage, &statement("SELECT * FROM src JOIN docs")).unwrap();
        let context = Context {
            schemas: &schemas,
            functions: &[],
            visiting_functions: Vec::new(),
        };
        let mut expected =
            statement("CREATE TABLE dst (renamed INT PRIMARY KEY, label TEXT DEFAULT 'new')");
        let StatementPlanKind::CreateTable { columns, .. } = &mut expected.kind else {
            panic!("create")
        };
        assert_eq!(
            copy_columns(&context, &query("SELECT s.* FROM src s(renamed)")),
            Ok(Some(columns.clone()))
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM SERIES(2)")),
            Ok(Some(TableColumnsPlan::Columns(vec![
                crate::plan::ColumnDefPlan {
                    name: "N".to_owned(),
                    data_type: DataType::Int,
                    nullable: false,
                    default: None,
                    unique: None,
                    comment: None,
                }
            ])))
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM docs")),
            Ok(Some(TableColumnsPlan::Schemaless))
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT missing.* FROM src")),
            Ok(None),
            "wrong qualifier is not a copy"
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT *, * FROM src")),
            Ok(None),
            "repeated wildcard is not a copy"
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM src a JOIN src b")),
            Ok(None)
        );
        assert_eq!(
            copy_columns(
                &context,
                &query("SELECT * FROM src a LEFT JOIN src b ON a.id = b.id WHERE TRUE")
            ),
            Ok(None)
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT id FROM src GROUP BY id")),
            Ok(None)
        );
        assert_eq!(
            copy_columns(
                &context,
                &query("SELECT id FROM src GROUP BY id HAVING COUNT(*) > 0")
            ),
            Ok(None)
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM (SELECT * FROM src) d")),
            Ok(None)
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM GLUE_TABLES")),
            Ok(None)
        );
        assert_eq!(copy_columns(&context, &query("VALUES (1)")), Ok(None));
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM missing")),
            Err(Error::Planner(PlannerError::CtasSourceTableNotFound(
                "missing".to_owned()
            )))
        );
        assert_eq!(
            copy_columns(&context, &query("SELECT * FROM src s(a, b, c)")),
            Err(Error::Planner(PlannerError::CtasAliasColumnsOverflow {
                expected: 2,
                found: 3
            }))
        );
    }
}
