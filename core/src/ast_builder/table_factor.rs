use {
    super::{DeleteNode, ExprNode, InsertNode, QueryNode, SelectNode, UpdateNode},
    crate::ast::Dictionary,
};

#[derive(Clone)]
pub enum TableType<'a> {
    Table,
    Series(ExprNode<'a>),
    Dictionary(Dictionary),
    Derived {
        subquery: Box<QueryNode<'a>>,
        alias: String,
    },
}

#[derive(Clone)]
pub struct TableFactorNode<'a> {
    pub table_name: String,
    pub table_type: TableType<'a>,
    pub table_alias: Option<String>,
}

impl<'a> TableFactorNode<'a> {
    pub fn select(self) -> SelectNode<'a> {
        SelectNode::new(self)
    }

    pub fn delete(self) -> DeleteNode<'static> {
        DeleteNode::new(self.table_name)
    }

    pub fn update(self) -> UpdateNode<'static> {
        UpdateNode::new(self.table_name)
    }

    pub fn insert(self) -> InsertNode {
        InsertNode::new(self.table_name)
    }
}

pub fn glue_objects() -> TableFactorNode<'static> {
    TableFactorNode {
        table_name: "GLUE_OBJECTS".to_owned(),
        table_type: TableType::Dictionary(Dictionary::GlueObjects),
        table_alias: None,
    }
}

pub fn glue_tables() -> TableFactorNode<'static> {
    TableFactorNode {
        table_name: "GLUE_TABLES".to_owned(),
        table_type: TableType::Dictionary(Dictionary::GlueTables),
        table_alias: None,
    }
}

pub fn glue_indexes() -> TableFactorNode<'static> {
    TableFactorNode {
        table_name: "GLUE_INDEXES".to_owned(),
        table_type: TableType::Dictionary(Dictionary::GlueIndexes),
        table_alias: None,
    }
}

pub fn glue_table_columns() -> TableFactorNode<'static> {
    TableFactorNode {
        table_name: "GLUE_TABLE_COLUMNS".to_owned(),
        table_type: TableType::Dictionary(Dictionary::GlueTableColumns),
        table_alias: None,
    }
}

pub fn series<'a, T: Into<ExprNode<'a>>>(args: T) -> TableFactorNode<'a> {
    TableFactorNode {
        table_name: "SERIES".to_owned(),
        table_type: TableType::Series(args.into()),
        table_alias: None,
    }
}
