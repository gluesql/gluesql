use {
    super::Build,
    crate::{
        ast::Statement,
        ast_builder::{column_def::PrimaryKeyConstraintNode, ColumnDefNode},
        error::TranslateError,
        parse_sql::parse_column_def,
        result::Result,
        translate::translate_column_defs,
    },
};

#[derive(Clone, Debug)]
pub struct CreateTableNode {
    table_name: String,
    if_not_exists: bool,
    columns: Option<Vec<ColumnDefNode>>,
    primary_key_constraint: Option<PrimaryKeyConstraintNode>,
}

impl CreateTableNode {
    pub fn new(table_name: String, not_exists: bool) -> Self {
        Self {
            table_name,
            if_not_exists: not_exists,
            columns: None,
            primary_key_constraint: None,
        }
    }

    pub fn add_column<T: Into<ColumnDefNode>>(mut self, column: T) -> Self {
        match self.columns {
            Some(ref mut columns) => {
                columns.push(column.into());
            }
            None => {
                self.columns = Some(vec![column.into()]);
            }
        }

        self
    }

    pub fn primary_key<T: Into<PrimaryKeyConstraintNode>>(mut self, columns: T) -> Self {
        self.primary_key_constraint = Some(columns.into());
        self
    }
}

impl Build for CreateTableNode {
    fn build(self) -> Result<Statement> {
        let (columns, primary_key): (Vec<crate::ast::ColumnDef>, Option<Vec<usize>>) = self
            .columns
            .map(|columns| {
                translate_column_defs(
                    columns
                        .iter()
                        .map(|column| parse_column_def(column.as_ref())),
                )
            })
            .transpose()?
            .map(Result::Ok)
            .unwrap_or_else(|| Err(TranslateError::LackOfColumns(self.table_name.clone())))?;

        let primary_key = match (primary_key, self.primary_key_constraint) {
            (Some(_), Some(_)) => {
                return Err(TranslateError::MultiplePrimaryKeyNotSupported.into());
            }
            (Some(primary_key), None) => Some(primary_key),
            (None, Some(primary_key)) => Some(
                primary_key
                    .as_ref()
                    .iter()
                    .map(|column| {
                        columns
                            .iter()
                            .position(|c| &c.name == column)
                            .ok_or_else(|| {
                                TranslateError::ColumnNotFoundInTable(column.clone()).into()
                            })
                    })
                    .collect::<Result<Vec<usize>>>()?,
            ),
            (None, None) => None,
        };

        Ok(Statement::CreateTable {
            name: self.table_name,
            if_not_exists: self.if_not_exists,
            columns: Some(columns),
            source: None,
            engine: None,
            foreign_keys: Vec::new(),
            primary_key,
            comment: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast_builder::{table, test, Build},
        result::TranslateError,
    };

    #[test]
    fn create_table() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER NULL")
            .add_column("num INTEGER")
            .add_column("name TEXT")
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER NULL, num INTEGER, name TEXT)";
        test(actual, expected);

        let actual = table("Foo")
            .create_table_if_not_exists()
            .add_column("id UUID UNIQUE")
            .add_column("name TEXT")
            .build();
        let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE, name TEXT)";
        test(actual, expected);
    }

    #[test]
    fn test_create_table_without_columns() {
        let actual = table("Foo").create_table().build();
        assert_eq!(
            actual.unwrap_err(),
            TranslateError::LackOfColumns("Foo".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_primary_key() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER PRIMARY KEY")
            .add_column("name TEXT")
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER PRIMARY KEY, name TEXT)";
        test(actual, expected);
    }

    #[test]
    fn create_table_with_composite_primary_key() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["id", "name"])
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER, name TEXT, PRIMARY KEY (id, name))";
        test(actual, expected);
    }

    #[test]
    fn try_create_table_with_composite_primary_key_with_missing_columns() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["id", "age"])
            .build();
        assert_eq!(
            actual.unwrap_err(),
            TranslateError::ColumnNotFoundInTable("age".to_owned()).into()
        );
    }
}
