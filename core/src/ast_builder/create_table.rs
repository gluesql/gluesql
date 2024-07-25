use {
    super::{column_def::UniqueConstraintNode, Build},
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
    unique_constraints: Vec<UniqueConstraintNode>,
}

impl CreateTableNode {
    pub fn new(table_name: String, not_exists: bool) -> Self {
        Self {
            table_name,
            if_not_exists: not_exists,
            columns: None,
            primary_key_constraint: None,
            unique_constraints: Vec::new(),
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

    pub fn primary_key<T: Into<PrimaryKeyConstraintNode>>(mut self, columns: T) -> Result<Self> {
        if self.primary_key_constraint.is_some() {
            return Err(TranslateError::MultiplePrimaryKeyNotSupported.into());
        }

        let columns = columns.into();

        if columns.as_ref().is_empty() {
            return Err(TranslateError::EmptyPrimaryKeyConstraint.into());
        }

        // If one of the columns in the constraint appears more than once, we return an error.
        let duplicated_columns = columns
            .as_ref()
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if columns.as_ref().iter().skip(index + 1).any(|c| c == column) {
                    Some(column.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if !duplicated_columns.is_empty() {
            return Err(TranslateError::RepeatedColumnsInPrimaryKeyConstraint(
                duplicated_columns.join(", "),
            )
            .into());
        }

        self.primary_key_constraint = Some(columns);
        Ok(self)
    }

    pub fn unique<'a, I: IntoIterator<Item = &'a str>>(
        mut self,
        name: Option<&str>,
        columns: I,
    ) -> Result<Self> {
        let unique_constraint = UniqueConstraintNode::new(name, columns);

        if unique_constraint.columns().is_empty() {
            return Err(TranslateError::EmptyUniqueConstraintColumns.into());
        }

        // We check whether any of the columns in the unique constraint are duplicated.
        let duplicated_columns = unique_constraint
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if unique_constraint
                    .columns()
                    .iter()
                    .skip(index + 1)
                    .any(|c| c == column)
                {
                    Some(column.to_owned())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if !duplicated_columns.is_empty() {
            return Err(TranslateError::RepeatedColumnsInUniqueConstraint(
                duplicated_columns.join(", "),
            )
            .into());
        }

        // We check whether there exist already unique constraint nodes with the same columns.
        if self
            .unique_constraints
            .iter()
            .any(|unique| unique.columns() == unique_constraint.columns())
        {
            return Err(TranslateError::DuplicatedUniqueConstraint(
                unique_constraint.columns().join(", "),
            )
            .into());
        }

        // We check whether there exist already unique constraint nodes with the same name.
        if unique_constraint.is_named()
            && self
                .unique_constraints
                .iter()
                .any(|unique| unique.name() == unique_constraint.name())
        {
            return Err(TranslateError::DuplicatedUniqueConstraintName(
                unique_constraint.name().unwrap().to_owned(),
            )
            .into());
        }

        self.unique_constraints.push(unique_constraint);
        Ok(self)
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

            for column in primary_key_constraint.as_ref() {
                if let Some(index) = columns
                    .as_ref()
                    .and_then(|columns| columns.iter().position(|c| &c.name == column))
                {
                    indices.push(index);
                } else {
                    return Err(TranslateError::ColumnNotFoundInTable(column.clone()).into());
                }
            }

            primary_key = Some(indices);
        };

        for unique_constraint_node in self.unique_constraints.iter() {
            let indices = unique_constraint_node
                .columns()
                .iter()
                .map(|column| {
                    columns
                        .as_ref()
                        .and_then(|columns| columns.iter().position(|c| &c.name == column))
                        .ok_or_else(|| {
                            TranslateError::ColumnNotFoundInTable(column.to_owned()).into()
                        })
                })
                .collect::<Result<Vec<usize>>>()?;

            unique_constraints.push(UniqueConstraint::new(
                unique_constraint_node.name().map(|name| name.to_owned()),
                indices,
            ));
        }

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
            .unwrap()
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
            .unwrap()
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

    #[test]
    fn create_table_with_multiple_primary_key() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER PRIMARY KEY")
            .add_column("name TEXT PRIMARY KEY")
            .build();
        assert_eq!(
            actual.unwrap_err(),
            TranslateError::MultiplePrimaryKeyNotSupported.into()
        );
    }

    #[test]
    fn create_table_with_multiple_primary_key_with_composite_primary_key() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["id", "name"])
            .unwrap()
            .primary_key(["id", "name"])
            .unwrap_err();
        assert_eq!(error, TranslateError::MultiplePrimaryKeyNotSupported.into());
    }

    #[test]
    fn create_table_with_multiple_primary_key_with_primary_key() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["id", "name"])
            .unwrap()
            .add_column("age INTEGER PRIMARY KEY")
            .build();
        assert_eq!(
            actual.unwrap_err(),
            TranslateError::MultiplePrimaryKeyNotSupported.into()
        );
    }

    #[test]
    fn create_table_with_unknown_primary_key() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["age"])
            .unwrap()
            .build();
        assert_eq!(
            actual.unwrap_err(),
            TranslateError::ColumnNotFoundInTable("age".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_primary_key_constraint_and_no_columns() {
        let actual = table("Foo")
            .create_table()
            .primary_key(["id"])
            .unwrap()
            .build();

        assert_eq!(
            actual.unwrap_err(),
            TranslateError::ColumnNotFoundInTable("id".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_primary_key_constraint_and_empty_columns() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key::<[&str; 0]>([])
            .unwrap_err();

        assert_eq!(error, TranslateError::EmptyPrimaryKeyConstraint.into());
    }

    #[test]
    fn create_table_with_primary_key_constraint_and_duplicated_columns() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .primary_key(["id", "id"])
            .unwrap_err();

        assert_eq!(
            error,
            TranslateError::RepeatedColumnsInPrimaryKeyConstraint("id".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_unique_constraint() {
        let actual = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), ["id"])
            .unwrap()
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER, name TEXT, CONSTRAINT unique_id UNIQUE (id))";
        test(actual, expected);
    }

    #[test]
    fn create_table_with_unique_constraint_with_missing_columns() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), ["age"])
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            TranslateError::ColumnNotFoundInTable("age".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_unique_constraint_with_duplicated_columns() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), ["id", "id"])
            .unwrap_err();
        assert_eq!(
            error,
            TranslateError::RepeatedColumnsInUniqueConstraint("id".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_unique_constraint_with_duplicated_name() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), ["id"])
            .unwrap()
            .unique(Some("unique_id"), ["name"])
            .unwrap_err();
        assert_eq!(
            error,
            TranslateError::DuplicatedUniqueConstraintName("unique_id".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_duplicated_unique_constraint() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), ["id"])
            .unwrap()
            .unique(Some("unique2_id"), ["id"])
            .unwrap_err();
        assert_eq!(
            error,
            TranslateError::DuplicatedUniqueConstraint("id".to_owned()).into()
        );
    }

    #[test]
    fn create_table_with_unique_constraint_with_empty_columns() {
        let error = table("Foo")
            .create_table()
            .add_column("id INTEGER")
            .add_column("name TEXT")
            .unique(Some("unique_id"), [])
            .unwrap_err();
        assert_eq!(error, TranslateError::EmptyUniqueConstraintColumns.into());
    }
}
