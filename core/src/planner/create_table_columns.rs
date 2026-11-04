use {
    super::PlannerError,
    crate::{
        ast::DataType,
        data::Schema,
        plan::{
            ColumnDefPlan, CreateTableColumnsPlan, FilterInputPlan, ProjectInputPlan, ProjectPlan,
            ProjectionPlan, SelectItemPlan, SourcePlan, StatementPlan, StatementPlanKind,
        },
        result::Result,
    },
    std::{collections::HashMap, hash::BuildHasher},
};

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    if let StatementPlanKind::CreateTable {
        columns, source, ..
    } = &mut statement.kind
        && let CreateTableColumnsPlan::Unplanned {
            columns: input_columns,
        } = columns
    {
        *columns = match source.as_deref() {
            Some(source) => match source.project().and_then(source_for_schema_copy) {
                Some(SourcePlan::Table(table)) => {
                    let schema = schema_map
                        .get(&table.name)
                        .ok_or_else(|| PlannerError::CtasSourceTableNotFound(table.name.clone()))?;

                    match &schema.column_defs {
                        Some(columns) => CreateTableColumnsPlan::Columns(
                            columns.iter().cloned().map(ColumnDefPlan::from).collect(),
                        ),
                        None => CreateTableColumnsPlan::Schemaless,
                    }
                }
                Some(SourcePlan::Series(_)) => {
                    CreateTableColumnsPlan::Columns(vec![ColumnDefPlan {
                        name: "N".into(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        unique: None,
                        comment: None,
                    }])
                }
                _ => CreateTableColumnsPlan::FromQuery,
            },
            None => input_columns.take().map_or(
                CreateTableColumnsPlan::Schemaless,
                CreateTableColumnsPlan::Columns,
            ),
        };
    }
    Ok(statement)
}

fn source_for_schema_copy(project: &ProjectPlan) -> Option<&SourcePlan> {
    let source = match &project.input {
        ProjectInputPlan::Source(relation) => relation,
        ProjectInputPlan::Filter(filter) => match &filter.input {
            FilterInputPlan::Source(relation) => relation,
            FilterInputPlan::InnerJoin(_) | FilterInputPlan::LeftOuterJoin(_) => return None,
        },
        ProjectInputPlan::InnerJoin(_)
        | ProjectInputPlan::LeftOuterJoin(_)
        | ProjectInputPlan::Aggregation(_)
        | ProjectInputPlan::Having(_) => return None,
    };

    match &project.projection {
        ProjectionPlan::SchemalessMap => true,
        ProjectionPlan::SelectItems(items) => items.iter().all(|item| {
            matches!(
                item,
                SelectItemPlan::Wildcard | SelectItemPlan::QualifiedWildcard(_)
            )
        }),
    }
    .then_some(source)
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::ColumnDef,
            mock::{MockStorage, run},
            parse_sql::{parse, parse_query},
            plan::{
                ColumnDefPlan, CreateTableColumnsPlan, ProjectInputPlan, ProjectPlan,
                ProjectionPlan, QueryPlan, SourcePlan, StatementPlan, StatementPlanKind,
                TableAccessPlan, TableSourcePlan,
            },
            planner::{PlannerError, fetch_schema_map},
            query_builder::ColumnDefNode,
            result::{Error, Result},
            translate::{NO_PARAMS, translate, translate_query},
        },
        std::collections::HashMap,
    };

    fn statement(sql: &str) -> StatementPlan {
        translate(&parse(sql).expect(sql)[0]).expect(sql).into()
    }

    fn query(sql: &str) -> QueryPlan {
        translate_query(&parse_query(sql).expect(sql), NO_PARAMS)
            .expect(sql)
            .into()
    }

    fn columns(definitions: &[&str]) -> Vec<ColumnDefPlan> {
        definitions
            .iter()
            .map(|definition| {
                ColumnDef::try_from(ColumnDefNode::from(*definition))
                    .expect(definition)
                    .into()
            })
            .collect()
    }

    fn create(source: Option<QueryPlan>, columns: CreateTableColumnsPlan) -> StatementPlan {
        StatementPlanKind::CreateTable {
            if_not_exists: false,
            name: "Target".to_owned(),
            columns,
            source: source.map(Box::new),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        }
        .into()
    }

    fn plan_columns(storage: &MockStorage, sql: &str) -> Result<StatementPlan> {
        let statement = statement(sql);
        let schema_map = fetch_schema_map(storage, &statement)?;
        plan(&schema_map, statement)
    }

    #[test]
    fn create_table() {
        let storage = run(
            "CREATE TABLE Item (id INT PRIMARY KEY DEFAULT 1 + 2 COMMENT 'id', label TEXT);
             CREATE TABLE Documents;",
        );
        let test = |sql: &str, expected, name: &str| {
            let actual = plan_columns(&storage, sql);
            assert_eq!(actual, Ok(expected), "{name}: {sql}");
        };

        let actual = "CREATE TABLE Target AS SELECT * FROM Item";
        let expected = create(
            Some(query("SELECT * FROM Item")),
            CreateTableColumnsPlan::Columns(columns(&[
                "id INT PRIMARY KEY DEFAULT 1 + 2 COMMENT 'id'",
                "label TEXT",
            ])),
        );
        test(actual, expected, "stored definitions and default plan");

        let actual = "CREATE TABLE Target AS SELECT * FROM Documents";
        let expected = create(
            Some(query("SELECT * FROM Documents")),
            CreateTableColumnsPlan::Schemaless,
        );
        test(actual, expected, "schemaless definitions");

        let actual = "CREATE TABLE Target AS SELECT * FROM SERIES(2)";
        let expected = create(
            Some(query("SELECT * FROM SERIES(2)")),
            CreateTableColumnsPlan::Columns(columns(&["N INT NOT NULL"])),
        );
        test(actual, expected, "fixed series definition");

        let actual = "CREATE TABLE Target AS SELECT id FROM Item";
        let expected = create(
            Some(query("SELECT id FROM Item")),
            CreateTableColumnsPlan::FromQuery,
        );
        test(actual, expected, "ordinary projection uses runtime values");

        let actual = "CREATE TABLE Target AS VALUES (1), (2)";
        let expected = create(
            Some(query("VALUES (1), (2)")),
            CreateTableColumnsPlan::FromQuery,
        );
        test(actual, expected, "values keep their execution path");

        let actual = "CREATE TABLE Target AS SELECT * FROM (SELECT id FROM Item) sub";
        let expected = create(
            Some(query("SELECT * FROM (SELECT id FROM Item) sub")),
            CreateTableColumnsPlan::FromQuery,
        );
        test(actual, expected, "derived source does not copy definitions");

        let actual = "CREATE TABLE Target AS SELECT * FROM GLUE_TABLES";
        let expected = create(
            Some(query("SELECT * FROM GLUE_TABLES")),
            CreateTableColumnsPlan::FromQuery,
        );
        test(actual, expected, "dictionary output uses runtime values");

        let actual = "CREATE TABLE Target (renamed TEXT) AS SELECT * FROM Item";
        let expected = create(
            Some(query("SELECT * FROM Item")),
            CreateTableColumnsPlan::Columns(columns(&[
                "id INT PRIMARY KEY DEFAULT 1 + 2 COMMENT 'id'",
                "label TEXT",
            ])),
        );
        test(
            actual,
            expected,
            "source definitions keep existing CTAS precedence",
        );

        let actual = "CREATE TABLE Target (renamed TEXT) AS VALUES (1)";
        let expected = create(Some(query("VALUES (1)")), CreateTableColumnsPlan::FromQuery);
        test(
            actual,
            expected,
            "runtime columns keep existing CTAS precedence",
        );

        let actual = plan_columns(&storage, "CREATE TABLE Target AS SELECT * FROM Missing");
        let expected = Err(Error::Planner(PlannerError::CtasSourceTableNotFound(
            "Missing".to_owned(),
        )));
        assert_eq!(actual, expected, "missing copy source");
    }

    #[test]
    fn unplanned_columns_without_source() {
        let input = create(
            None,
            CreateTableColumnsPlan::Unplanned {
                columns: Some(columns(&["id INT DEFAULT 7"])),
            },
        );
        let actual = plan(&HashMap::new(), input);
        let expected = Ok(create(
            None,
            CreateTableColumnsPlan::Columns(columns(&["id INT DEFAULT 7"])),
        ));
        assert_eq!(actual, expected, "explicit definitions without a source");

        let input = create(None, CreateTableColumnsPlan::Unplanned { columns: None });
        let actual = plan(&HashMap::new(), input);
        let expected = Ok(create(None, CreateTableColumnsPlan::Schemaless));
        assert_eq!(actual, expected, "schemaless without a source");
    }

    #[test]
    fn leaves_other_statements_and_prepared_columns() {
        let test = |input: StatementPlan, name: &str| {
            let actual = plan(&HashMap::new(), input.clone());
            let expected = Ok(input);
            assert_eq!(actual, expected, "{name}");
        };

        test(statement("SELECT 1"), "other statement");
        test(
            statement("CREATE TABLE Item (id INT DEFAULT 7)"),
            "no source",
        );
        test(
            create(
                Some(query("SELECT * FROM Missing")),
                CreateTableColumnsPlan::Columns(columns(&["id INT"])),
            ),
            "prepared definitions are not fetched again",
        );
        test(
            create(
                Some(query("SELECT * FROM Missing")),
                CreateTableColumnsPlan::Schemaless,
            ),
            "prepared schemaless copy",
        );
        test(
            create(
                Some(query("SELECT * FROM Missing")),
                CreateTableColumnsPlan::FromQuery,
            ),
            "prepared runtime columns are not planned again",
        );
    }

    #[test]
    fn source_for_schema_copy() {
        let source = SourcePlan::Table(TableSourcePlan {
            name: "Item".to_owned(),
            alias: None,
            access: TableAccessPlan::FullScan,
        });
        let test = |sql: &str, expected, name: &str| {
            let query = query(sql);
            let actual = query.project().and_then(super::source_for_schema_copy);
            assert_eq!(actual, expected, "{name}: {sql}");
        };

        test("SELECT * FROM Item", Some(&source), "wildcard");
        test(
            "SELECT Item.* FROM Item",
            Some(&source),
            "qualified wildcard",
        );
        test(
            "SELECT DISTINCT * FROM Item WHERE id > 0 ORDER BY id LIMIT 2 OFFSET 1",
            Some(&source),
            "row operations preserve the source definitions",
        );
        test("SELECT id FROM Item", None, "expression projection");
        test("SELECT * FROM Item JOIN Other", None, "inner join");
        test(
            "SELECT * FROM Item LEFT JOIN Other ON Item.id = Other.id",
            None,
            "left join",
        );
        test(
            "SELECT * FROM Item JOIN Other WHERE Item.id = Other.id",
            None,
            "filtered inner join",
        );
        test(
            "SELECT * FROM Item LEFT JOIN Other ON Item.id = Other.id WHERE Item.id > 0",
            None,
            "filtered left join",
        );
        test("SELECT * FROM Item GROUP BY id", None, "aggregation");
        test(
            "SELECT * FROM Item GROUP BY id HAVING COUNT(*) > 1",
            None,
            "having",
        );

        let project = ProjectPlan {
            input: ProjectInputPlan::Source(source.clone()),
            projection: ProjectionPlan::SchemalessMap,
        };
        let actual = super::source_for_schema_copy(&project);
        let expected = Some(&source);
        assert_eq!(actual, expected, "schemaless map projection");
    }
}
