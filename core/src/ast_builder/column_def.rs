#[derive(Clone, Debug)]
pub struct PrimaryKeyConstraintNode {
    columns: Vec<String>,
}

impl<I: IntoIterator<Item = S>, S: ToString> From<I> for PrimaryKeyConstraintNode {
    fn from(columns: I) -> Self {
        PrimaryKeyConstraintNode {
            columns: columns
                .into_iter()
                .map(|column| column.to_string())
                .collect(),
        }
    }
}

impl AsRef<[String]> for PrimaryKeyConstraintNode {
    fn as_ref(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Clone, Debug)]
pub enum ColumnDefNode {
    Text(String),
}

impl AsRef<str> for ColumnDefNode {
    fn as_ref(&self) -> &str {
        match self {
            ColumnDefNode::Text(column_def) => column_def,
        }
    }
}

impl From<&str> for ColumnDefNode {
    fn from(column_def: &str) -> Self {
        ColumnDefNode::Text(column_def.to_owned())
    }
}
