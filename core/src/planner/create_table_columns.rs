mod expr;
mod query;
mod source;

use {
    super::PlannerError,
    crate::{
        ast::DataType,
        data::Schema,
        plan::{
            ColumnDefPlan, CustomFunctionPlan, StatementPlan, StatementPlanKind, TableColumnsPlan,
        },
        result::Result,
    },
    expr::ExprType,
    query::QueryColumns,
    std::{collections::HashMap, hash::BuildHasher},
};

struct Context<'a, S> {
    schemas: &'a HashMap<String, Schema, S>,
    functions: &'a [CustomFunctionPlan],
    visiting_functions: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
struct Column {
    name: String,
    expr_type: ExprType,
}

impl Column {
    fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            expr_type: ExprType::Known(data_type),
        }
    }
}

pub fn plan<S: BuildHasher>(
    schema_map: &HashMap<String, Schema, S>,
    mut statement: StatementPlan,
) -> Result<StatementPlan> {
    let StatementPlanKind::CreateTable {
        name,
        columns,
        source,
        foreign_keys,
        ..
    } = &mut statement.kind
    else {
        return Ok(statement);
    };
    let Some(source) = source else {
        if matches!(columns, TableColumnsPlan::Unplanned) {
            return Err(PlannerError::MalformedPlanCtasWithoutSource.into());
        }
        return Ok(statement);
    };
    if foreign_keys
        .iter()
        .any(|key| &key.referenced_table_name == name)
    {
        return Err(PlannerError::UnsupportedSelfReferencingForeignKeyInCtas(name.clone()).into());
    }

    let mut context = Context {
        schemas: schema_map,
        functions: &statement.functions,
        visiting_functions: Vec::new(),
    };
    if matches!(columns, TableColumnsPlan::Unplanned)
        && let Some(copied) = source::copy_columns(&context, source)?
    {
        *columns = copied;
    }
    let types = match columns {
        TableColumnsPlan::Columns(columns) => Some(
            columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>(),
        ),
        TableColumnsPlan::Unplanned | TableColumnsPlan::Schemaless => None,
    };
    let output = query::plan_query(&mut context, source, None, types.as_deref())?;
    match (&columns, output) {
        (TableColumnsPlan::Unplanned, QueryColumns::Columns(output)) => {
            *columns = TableColumnsPlan::Columns(
                output
                    .into_iter()
                    .map(|column| {
                        let data_type = column.expr_type.require_type(&column.name)?;
                        Ok(ColumnDefPlan {
                            name: column.name,
                            data_type,
                            nullable: true,
                            default: None,
                            unique: None,
                            comment: None,
                        })
                    })
                    .collect::<Result<_>>()?,
            );
        }
        (TableColumnsPlan::Unplanned, QueryColumns::Schemaless) => {
            *columns = TableColumnsPlan::Schemaless;
        }
        (TableColumnsPlan::Columns(_), QueryColumns::Schemaless)
        | (TableColumnsPlan::Schemaless, QueryColumns::Columns(_)) => {
            return Err(PlannerError::CtasSourceFormatMismatch.into());
        }
        (TableColumnsPlan::Columns(_), QueryColumns::Columns(_))
        | (TableColumnsPlan::Schemaless, QueryColumns::Schemaless) => {}
    }
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use {
        super::plan,
        crate::{
            ast::DataType,
            mock::{MockStorage, run},
            parse_sql::parse,
            plan::{
                ColumnDefPlan, ExprPlan, QueryPlan, StatementPlan, StatementPlanKind,
                TableColumnsPlan, ValuesPlan,
            },
            planner::{PlannerError, fetch_schema_map, plan_aggregate, plan_schemaless},
            result::{Error, Result},
            translate::translate,
        },
    };

    fn statement(sql: &str) -> StatementPlan {
        translate(&parse(sql).expect(sql)[0]).expect(sql).into()
    }

    fn plan_columns(storage: &MockStorage, sql: &str) -> Result<StatementPlan> {
        let statement = statement(sql);
        let schemas = fetch_schema_map(storage, &statement)?;
        let statement = plan_aggregate(plan_schemaless(&schemas, statement)?);
        plan(&schemas, statement)
    }

    #[test]
    fn create_table() {
        let storage =
            run("CREATE TABLE src (id INT PRIMARY KEY, label TEXT DEFAULT 'new' COMMENT 'label')");
        let test = |actual: &str, expected: &str, name: &str| {
            assert_eq!(
                plan_columns(&storage, actual),
                Ok(statement(expected)),
                "[{name}] {actual}"
            );
        };
        let actual = "CREATE TABLE dst (id INT DEFAULT 1)";
        let expected = "CREATE TABLE dst (id INT DEFAULT 1)";
        test(actual, expected, "definition without source");

        let actual = "CREATE TABLE dst";
        let expected = "CREATE TABLE dst";
        test(actual, expected, "schemaless without source");

        let actual = "SELECT * FROM src";
        let expected = "SELECT * FROM src";
        test(actual, expected, "other statement");

        let actual =
            "CREATE TABLE dst AS SELECT * FROM src WHERE id > 1 ORDER BY id LIMIT 2 OFFSET 1";
        let expected = "
            CREATE TABLE dst (id INT PRIMARY KEY, label TEXT DEFAULT 'new' COMMENT 'label')
            AS SELECT * FROM src WHERE id > 1 ORDER BY id LIMIT 2 OFFSET 1
        ";
        test(
            actual,
            expected,
            "whole-source definitions survive row operators",
        );

        let actual = "CREATE TABLE dst AS SELECT id, label FROM src";
        let expected =
            "CREATE TABLE dst (id INT NULL, label TEXT NULL) AS SELECT id, label FROM src";
        test(actual, expected, "ordinary output has only names and types");

        let actual = "CREATE TABLE dst (renamed INT NOT NULL, note TEXT DEFAULT 'other') AS SELECT id, label FROM src";
        let expected = actual;
        test(actual, expected, "explicit definitions take precedence");

        let planned = plan_columns(&storage, "CREATE TABLE dst AS SELECT * FROM src").unwrap();
        let schemas = fetch_schema_map(&storage, &planned).unwrap();
        assert_eq!(
            plan(&schemas, planned.clone()),
            Ok(planned),
            "planning prepared columns is stable"
        );
    }

    #[test]
    fn source_contract() {
        let storage =
            run("CREATE TABLE src (id INT); CREATE TABLE existing (id INT); CREATE TABLE docs;");
        let test = |sql: &str, expected: PlannerError, name: &str| {
            assert_eq!(
                plan_columns(&storage, sql),
                Err(Error::Planner(expected)),
                "[{name}] {sql}"
            );
        };
        test(
            "CREATE TABLE IF NOT EXISTS existing AS SELECT * FROM missing",
            PlannerError::CtasSourceTableNotFound("missing".to_owned()),
            "existing target does not skip planning",
        );
        test(
            "CREATE TABLE dst AS SELECT NULL AS unknown",
            PlannerError::CtasColumnTypeUnknown("unknown".to_owned()),
            "untyped output",
        );
        test(
            "CREATE TABLE dst (id INT, extra INT DEFAULT 1) AS SELECT id FROM src WHERE FALSE",
            PlannerError::CtasColumnCountMismatch {
                expected: 2,
                found: 1,
            },
            "empty source cannot fill missing columns",
        );
        test(
            "CREATE TABLE dst (id INT) AS VALUES (1, 2)",
            PlannerError::CtasColumnCountMismatch {
                expected: 1,
                found: 2,
            },
            "excess values",
        );
        test(
            "CREATE TABLE dst (id INT) AS SELECT * FROM docs",
            PlannerError::CtasSourceFormatMismatch,
            "schemaful target and schemaless source",
        );
        test(
            "CREATE TABLE dst (id INT PRIMARY KEY, parent INT, FOREIGN KEY (parent) REFERENCES dst(id)) AS VALUES (1, NULL)",
            PlannerError::UnsupportedSelfReferencingForeignKeyInCtas("dst".to_owned()),
            "self-reference with a source",
        );

        let sql = "CREATE TABLE dst (id INT PRIMARY KEY, parent INT, FOREIGN KEY (parent) REFERENCES dst(id))";
        assert_eq!(
            plan_columns(&storage, sql),
            Ok(statement(sql)),
            "source-less self-reference is unchanged"
        );

        let actual = plan_columns(&storage, "CREATE TABLE dst AS SELECT * FROM docs").unwrap();
        let mut expected = statement("CREATE TABLE dst AS SELECT * FROM docs");
        let StatementPlanKind::CreateTable {
            columns, source, ..
        } = &mut expected.kind
        else {
            panic!("create")
        };
        *columns = TableColumnsPlan::Schemaless;
        let Some(source) = source else {
            panic!("source")
        };
        let QueryPlan::Project(project) = source.as_mut() else {
            panic!("project")
        };
        project.projection = crate::plan::ProjectionPlan::SchemalessMap;
        assert_eq!(actual, expected, "prepared schemaless projection");
    }

    #[test]
    fn directly_constructed_plan() {
        let storage = MockStorage::default();
        let mut input = statement("CREATE TABLE dst");
        let StatementPlanKind::CreateTable { columns, .. } = &mut input.kind else {
            panic!("create")
        };
        *columns = TableColumnsPlan::Unplanned;
        let schemas = fetch_schema_map(&storage, &input).unwrap();
        assert_eq!(
            plan(&schemas, input),
            Err(Error::Planner(PlannerError::MalformedPlanCtasWithoutSource))
        );

        let mut input = statement("CREATE TABLE dst AS VALUES (1)");
        let StatementPlanKind::CreateTable {
            columns, source, ..
        } = &mut input.kind
        else {
            panic!("create")
        };
        *columns = TableColumnsPlan::Schemaless;
        *source = Some(Box::new(QueryPlan::Values(ValuesPlan(vec![vec![
            ExprPlan::Value(crate::data::Value::I64(1)),
        ]]))));
        assert_eq!(
            plan(&schemas, input),
            Err(Error::Planner(PlannerError::CtasSourceFormatMismatch))
        );

        let mut expected = statement("CREATE TABLE dst AS VALUES (1)");
        let StatementPlanKind::CreateTable {
            columns, source, ..
        } = &mut expected.kind
        else {
            panic!("create")
        };
        *columns = TableColumnsPlan::Columns(vec![ColumnDefPlan {
            name: "column1".to_owned(),
            data_type: DataType::Int,
            nullable: true,
            default: None,
            unique: None,
            comment: None,
        }]);
        *source = Some(Box::new(QueryPlan::Values(ValuesPlan(vec![vec![
            ExprPlan::Value(crate::data::Value::I64(1)),
        ]]))));
        assert_eq!(
            plan_columns(&storage, "CREATE TABLE dst AS VALUES (1)"),
            Ok(expected),
            "literal value and inferred column are prepared together"
        );
    }
}
