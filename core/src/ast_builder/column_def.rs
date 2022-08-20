use crate::parse_sql::parse_column_option_def;
use {
    super::create_table::CreateTableNode,
    crate::{
        ast::{ColumnOption, ColumnOptionDef, DataType},
        parse_sql::parse_column_option,
        result::{Error, Result},
        translate::translate_column_option_def,
    },
};

#[derive(Clone)]
pub struct ColumnDefNode {
    prev_node: CreateTableNode,
    name: String,
    data_type: DataType,
    options: Vec<ColumnOptionDef>,
}

impl ColumnDefNode {
    pub fn new(prev_node: CreateTableNode, name: String, data_type: DataType) -> Self {
        Self {
            prev_node,
            name,
            data_type,
            options: Vec::new(),
        }
    }

    pub fn option<T: Into<ColumnOptionDefNode>>(self, options: T) -> CreateTableNode {
        self.prev_node // TODO
    }
}

#[derive(Clone)]
pub enum ColumnOptionDefNode {
    Text(String),
    ColumnOptionDef(ColumnOptionDef), //Options(Vec<ColumnOptionDef>),
}

impl From<&str> for ColumnOptionDefNode {
    fn from(option: &str) -> Self {
        ColumnOptionDefNode::Text(option.to_owned())
    }
}

impl From<ColumnOptionDef> for ColumnOptionDefNode {
    fn from(option: ColumnOptionDef) -> Self {
        ColumnOptionDefNode::ColumnOptionDef(option)
    }
}

// Vec<ColumnOptionDef> ??
impl TryFrom<ColumnOptionDefNode> for ColumnOptionDef {
    type Error = Error;

    fn try_from(column_option_def_node: ColumnOptionDefNode) -> Result<Self> {
        match column_option_def_node {
            ColumnOptionDefNode::Text(column_option) => parse_column_option_def(column_option)
                .and_then(|option| translate_column_option_def(&option)),

            ColumnOptionDefNode::ColumnOptionDef(node) => node.into(),
        }
    }
}

// TODO test ColumnOptionDefNode
