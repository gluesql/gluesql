use {
    super::{Build, SchemaNode},
    crate::{
        data::Schema,
        plan::{SchemaMap, SchemaMapPlan, StatementPlan},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct WithSchemaNode<B> {
    prev_node: B,
    schemas: Vec<SchemaNode>,
}

impl<B> WithSchemaNode<B> {
    pub(super) fn new(prev_node: B, schema: SchemaNode) -> Self {
        Self {
            prev_node,
            schemas: vec![schema],
        }
    }

    #[must_use]
    pub fn with_schema<S: Into<SchemaNode>>(mut self, schema: S) -> Self {
        self.schemas.push(schema.into());

        self
    }
}

impl<B: Build> Build for WithSchemaNode<B> {
    fn build(self) -> Result<StatementPlan> {
        let mut statement = self.prev_node.build()?;

        let mut schema_map = SchemaMap::new();

        for schema in self.schemas {
            let schema = Schema::try_from(schema)?;
            schema_map.insert(schema.table_name.clone(), schema.into());
        }

        statement.schema_map = SchemaMapPlan::Planned(schema_map);

        Ok(statement)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            data::{Schema, SchemaParseError},
            parse_sql::parse,
            plan::{SchemaMapPlan, StatementPlan},
            query_builder::{Build, QueryBuilderError, num, table},
            result::{Error, Result},
            translate::translate,
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: &Result<StatementPlan>, expected_sql: &str, expected_schemas: &[&str]) {
        let parsed = &parse(expected_sql).expect(expected_sql)[0];
        let mut expected = StatementPlan::from(translate(parsed).unwrap());

        let schema_map = expected_schemas
            .iter()
            .map(|ddl| {
                let schema = Schema::from_ddl(ddl).expect(ddl);

                (schema.table_name.clone(), schema.into())
            })
            .collect();
        expected.schema_map = SchemaMapPlan::Planned(schema_map);

        assert_eq!(*actual, Ok(expected));
    }

    #[test]
    fn with_schema() {
        // &str
        let actual = table("Foo")
            .select()
            .with_schema("CREATE TABLE Foo (id INT)")
            .build();
        let expected = "SELECT * FROM Foo";
        let schemas = ["CREATE TABLE Foo (id INT)"];
        test(&actual, expected, &schemas);

        // String
        let actual = table("Foo")
            .select()
            .with_schema(String::from("CREATE TABLE Foo (name TEXT)"))
            .build();
        let expected = "SELECT * FROM Foo";
        let schemas = ["CREATE TABLE Foo (name TEXT)"];
        test(&actual, expected, &schemas);

        // Schema
        let schema = Schema::from_ddl("CREATE TABLE Foo (active BOOLEAN)").unwrap();
        let actual = table("Foo").select().with_schema(schema).build();
        let expected = "SELECT * FROM Foo";
        let schemas = ["CREATE TABLE Foo (active BOOLEAN)"];
        test(&actual, expected, &schemas);

        // with schema node -> with schema node -> build
        let actual = table("Foo")
            .select()
            .with_schema("CREATE TABLE Foo (id INT)")
            .with_schema("CREATE TABLE Bar (name TEXT)")
            .build();
        let expected = "SELECT * FROM Foo";
        let schemas = ["CREATE TABLE Foo (id INT)", "CREATE TABLE Bar (name TEXT)"];
        test(&actual, expected, &schemas);

        // Replace a schema with the same table name.
        let actual = table("Foo")
            .select()
            .with_schema("CREATE TABLE Foo (id INT)")
            .with_schema("CREATE TABLE Bar (name TEXT)")
            .with_schema("CREATE TABLE Foo (name TEXT)")
            .build();
        let expected = "SELECT * FROM Foo";
        let schemas = [
            "CREATE TABLE Foo (name TEXT)",
            "CREATE TABLE Bar (name TEXT)",
        ];
        test(&actual, expected, &schemas);
    }

    #[test]
    fn build_error() {
        let actual = table("Foo").select().with_schema("SELECT 1").build();
        let expected = Err(Error::Schema(SchemaParseError::CannotParseDDL));
        assert_eq!(actual, expected);

        let actual = table("Foo")
            .select()
            .project(num(f64::NAN))
            .with_schema("CREATE TABLE Foo (id INT)")
            .build();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::FailedToParseNumeric(f64::NAN.to_string()),
        ));
        assert_eq!(actual, expected);
    }
}
