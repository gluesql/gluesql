use {
    super::Build,
    crate::{
        ast::{Statement, UniqueConstraint},
        ast_builder::{column_def::PrimaryKeyConstraintNode, ColumnDefNode},
        error::TranslateError,
        parse_sql::parse_column_def,
        result::Result,
        translate::translate_column_def,
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
        let table_name = self.table_name;
        let mut primary_key: Option<Vec<usize>> = None;
        let mut unique_constraints: Vec<UniqueConstraint> = Vec::new();
        let columns: Option<Vec<crate::ast::ColumnDef>> = match self.columns {
            Some(columns) => Some(
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(index, column_statement)| {
                        let (translated_column, is_primary, is_unique) =
                            translate_column_def(&parse_column_def(column_statement)?)?;
                        if is_primary {
                            {
                                match primary_key.as_mut() {
                                    Some(_) => {
                                        return Err(
                                            TranslateError::MultiplePrimaryKeyNotSupported.into()
                                        )
                                    }
                                    None => {
                                        primary_key = Some(vec![index]);
                                    }
                                }
                            }
                        }
                        if is_unique {
                            unique_constraints.push(UniqueConstraint::new_anonimous(vec![index]));
                        }
                        Ok(translated_column)
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            None => None,
        };

        if let Some(primary_key_constraint) = self.primary_key_constraint {
            if primary_key.is_some() {
                return Err(TranslateError::MultiplePrimaryKeyNotSupported.into());
            }

            let mut indices = Vec::new();

            if let Some(columns) = columns.as_ref() {
                for column in primary_key_constraint.as_ref() {
                    if let Some(index) = columns.iter().position(|c| &c.name == column) {
                        indices.push(index);
                    } else {
                        return Err(TranslateError::ColumnNotFoundInTable(column.clone()).into());
                    }
                }
            }

            primary_key = Some(indices);
        };

        Ok(Statement::CreateTable {
            name: table_name,
            if_not_exists: self.if_not_exists,
            columns,
            source: None,
            engine: None,
            foreign_keys: Vec::new(),
            primary_key,
            unique_constraints,
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

    #[test]
    fn create_table_without_column() {
        let actual = table("Foo").create_table().build();
        let expected = "CREATE TABLE Foo";
        test(actual, expected);
    }
}
