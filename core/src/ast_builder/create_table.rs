use crate::{
    ast::{ObjectName, Statement},
    ast_builder::{ColumnDefNode, DataTypeNode, QueryNode},
    result::Result,
};

#[derive(Clone)]
pub struct CreateTableNode {
    table_name: String,
    if_not_exists: bool,
    columns: Vec<ColumnDefNode>,
    source: Option<Box<QueryNode>>, // TODO
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
        let columns = self.columns.into_iter().map(TryInto::try_into)?.collect();
        Ok(Statement::CreateTable {
            name: table_name,
            if_not_exists: self.if_not_exists,
            columns,
            source: None,
        })
    }

    // coldefnode -> option 설정 fsm 하려고 이렇게 한 건데 그러면 set_col().set_col() 가 안 됨..
    pub fn set_col<T: Into<DataTypeNode>>(mut self, col_name: &str, data_type: T) -> ColumnDefNode {
        ColumnDefNode::new(self, col_name.to_string(), data_type.into())
    }
    // column data 를 데리고 다니다가 마지막에 추가 .. ?
    pub fn push_col_node(mut self, col: ColumnDefNode) -> Self {
        self.columns.push(col);
        self
    }

    pub fn add_column<T: Into<ColumnDefNode>>(mut self, column: T) -> Self {
        self.columns.push(column.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{ColumnOption, DataType},
        ast_builder::{table, test},
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
        test(actual, expected);

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
            .set_col("id", "UUID")
            .option(vec![
                ColumnOption::Null,
                ColumnOption::Unique { is_primary: false },
            ])
            .build();
        let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE NULL)";
        test(actual, expected);

        let actual = table("Foo")
            .create_table_if_not_exists()
            .set_col("id", "UUID")
            .option("UNIQUE")
            .build();
        let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE)";
        test(actual, expected);
        //
        // let actual = table("Foo")
        //     .create_table_if_not_exists()
        //     .set_col("id", "UUID")
        //     .build();
        // let expected = "CREATE TABLE IF NOT EXISTS Foo (id UUID UNIQUE)";
        //
        // test(actual, expected);
    }
}
