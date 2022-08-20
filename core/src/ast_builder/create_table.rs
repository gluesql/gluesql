use {
    super::column_def,
    crate::{
        ast::{DataType, ObjectName, Query, Statement},
        ast_builder::{ColumnDefNode, QueryNode},
        result::Result,
    },
};

#[derive(Clone)]
pub struct CreateTableNode {
    table_name: String,
    if_not_exists: bool,
    columns: Vec<ColumnDefNode>,
    source: Option<Box<QueryNode>>,
}

impl CreateTableNode {
    pub fn new(table_name: String, not_exists: bool) -> Self {
        Self {
            table_name,
            if_not_exists: not_exists,
            columns: Vec::new(),
            source: None,
        }
    }

    pub fn build(self) -> Result<Statement> {
        let table_name = ObjectName(vec![self.table_name]);
        let columns = self.columns.into_iter().map(TryInto::try_into()).collect();
        Ok(Statement::CreateTable {
            name: table_name,
            if_not_exists: self.if_not_exists,
            columns,
            source,
        })
    }

    pub fn set_col<T: Into<DataTypeNode>>(self, col_name: &str, datatype: T) -> ColumnDefNode {
        ColumnDefNode::new(self, col_name.to_string(), datatype.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::DataType,
        ast_builder::{col, table, test},
    };

    #[test]
    fn create_table() {
        let actual = table("Foo")
            .create_table()
            .set_col("id", "int") // 굳이 col()로 해야하나 ? ... 그럴 필요 없을 듯 ..
            .option("NULL")
            .set_col("num", DataType::Int)
            .set_col("name", "text")
            .build();
        let expected = "CREATE TABLE Foo (id INTEGER NULL, num INTEGER, name TEXT)";

        let actual = table("Foo")
            .create_table_if_not_exists()
            .set_col("id", "UUID")
            .option("UNIQUE")
            .build();
        let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE)";

        test(actual, expected);
    }
}
