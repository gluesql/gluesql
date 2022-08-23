use {
    super::create_table::CreateTableNode,
    crate::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef},
        ast_builder::DataTypeNode,
        parse_sql::parse_column_def,
        result::{Error, Result},
        translate::{translate_column_def, translate_column_option_def},
    },
};

#[derive(Clone)]
pub enum ColumnDefNode {
    Text(String),
}

// #[derive(Clone)]
// pub struct ColumnDefNode {
//     create_table_node: CreateTableNode,
//     name: String,
//     data_type: DataTypeNode,
//     options: ColumnOptionDefList,
// }

impl ColumnDefNode {
    pub fn new(create_table_node: CreateTableNode, name: String, data_type: DataTypeNode) -> Self {
        Self {
            create_table_node,
            name,
            data_type,
            options: ColumnOptionDefList::ColumnOptionDefs(vec![]),
        }
    }

    pub fn set_col<T: Into<DataTypeNode>>(self, col_name: &str, data_type: T) -> CreateTableNode {
        let new_column = ColumnDefNode::new(
            self.create_table_node,
            col_name.to_string(),
            data_type.into(),
        );
        self.create_table_node.push_col_node(new_column)
    }

    pub fn option<T: Into<ColumnOptionDefList>>(mut self, options: T) -> CreateTableNode {
        let col_option_defs = options.into(); // column def 노드 만들어서 push 해주고 넣어주기..
        self.options = col_option_defs;
        self.create_table_node.push_col_node(self)
    }
}

// impl TryFrom<ColumnDefNode> for ColumnDef {
//     type Error = Error;
//
//     fn try_from(column_def_node: ColumnDefNode) -> Result<ColumnDef> {
//         let name = column_def_node.name;
//         let data_type = column_def_node.data_type.try_into()?;
//         let options = column_def_node.options;
//         Ok(ColumnDef {
//             name,
//             data_type,
//             options,
//         })
//     }
// }

impl TryFrom<ColumnDefNode> for ColumnDef {
    type Error = Error;

    fn try_from(column_def_node: ColumnDefNode) -> Result<ColumnDef> {
        match column_def_node {
            ColumnDefNode::Text(column_def) => parse_column_def(column_def)
                .and_then(|column_def| translate_column_def(&column_def)),
        }
    }
}

#[derive(Clone)]
pub enum ColumnOptionDefList {
    Text(String),
    ColumnOptionDefs(Vec<ColumnOptionDef>), // Vec<ColumnOption>??
}

impl From<&str> for ColumnOptionDefList {
    fn from(option: &str) -> Self {
        ColumnOptionDefList::Text(option.to_owned())
    }
}

impl From<ColumnOption> for ColumnOptionDefList {
    fn from(option: ColumnOption) -> Self {
        ColumnOptionDefList::ColumnOptionDefs(vec![ColumnOptionDef { name: None, option }])
    }
}

impl From<Vec<ColumnOption>> for ColumnOptionDefList {
    fn from(options: Vec<ColumnOption>) -> Self {
        ColumnOptionDefList::ColumnOptionDefs(
            options
                .into_iter()
                .map(|option| ColumnOptionDef { name: None, option })
                .collect(),
        )
    }
}

impl From<ColumnOptionDef> for ColumnOptionDefList {
    fn from(option: ColumnOptionDef) -> Self {
        ColumnOptionDefList::ColumnOptionDefs(vec![option])
    }
}

impl From<Vec<ColumnOptionDef>> for ColumnOptionDefList {
    fn from(options: Vec<ColumnOptionDef>) -> Self {
        ColumnOptionDefList::ColumnOptionDefs(options)
    }
}

impl TryFrom<ColumnOptionDefList> for Vec<ColumnOptionDef> {
    type Error = Error;

    fn try_from(column_option_def_list: ColumnOptionDefList) -> Result<Self> {
        match column_option_def_list {
            ColumnOptionDefList::Text(column_option) => parse_column_option_def(column_option)
                .and_then(|option| translate_column_option_def(&option)),

            ColumnOptionDefList::ColumnOptionDefs(options) => Ok(options),
        }
    }
}

// TODO test 추가, 파일 쪼개기
