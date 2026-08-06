use {
    super::{ExprNode, QueryNode, SelectNode, TableAccessNode},
    crate::ast::Dictionary,
};

#[derive(Clone, Debug)]
pub enum SourceNode<'a> {
    Table {
        name: String,
        alias: Option<String>,
        access: TableAccessNode<'a>,
    },
    Series {
        size: ExprNode<'a>,
        alias: String,
    },
    Dictionary {
        dictionary: Dictionary,
        alias: String,
    },
    Derived {
        query: Box<QueryNode<'a>>,
        alias: String,
    },
}

impl<'a> SourceNode<'a> {
    pub fn select(self) -> SelectNode<'a> {
        SelectNode::new(self)
    }
}

pub fn glue_objects() -> SourceNode<'static> {
    SourceNode::Dictionary {
        dictionary: Dictionary::GlueObjects,
        alias: "GLUE_OBJECTS".to_owned(),
    }
}

pub fn glue_tables() -> SourceNode<'static> {
    SourceNode::Dictionary {
        dictionary: Dictionary::GlueTables,
        alias: "GLUE_TABLES".to_owned(),
    }
}

pub fn glue_indexes() -> SourceNode<'static> {
    SourceNode::Dictionary {
        dictionary: Dictionary::GlueIndexes,
        alias: "GLUE_INDEXES".to_owned(),
    }
}

pub fn glue_table_columns() -> SourceNode<'static> {
    SourceNode::Dictionary {
        dictionary: Dictionary::GlueTableColumns,
        alias: "GLUE_TABLE_COLUMNS".to_owned(),
    }
}

pub fn series<'a, T: Into<ExprNode<'a>>>(args: T) -> SourceNode<'a> {
    SourceNode::Series {
        size: args.into(),
        alias: "SERIES".to_owned(),
    }
}
