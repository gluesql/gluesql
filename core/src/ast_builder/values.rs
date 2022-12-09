use crate::ast::{Query, Statement};

use super::{
    select::{NodeData, Prebuild},
    ExprList, QueryNode,
};

#[derive(Clone, Debug)]
pub struct ValuesNode<'a> {
    pub values: QueryNode<'a>,
}

impl<'a> ValuesNode<'a> {
    pub fn build(self) {
        Statement::Query(Query {
            body: crate::ast::SetExpr::Values(self.values.try_into()?),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
    }
}

pub fn values<'a, T: Into<ExprList<'a>>>(values: Vec<T>) -> ValuesNode<'a> {
    let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

    ValuesNode {
        values: QueryNode::Values(values),
    }
}

// impl<'a> Prebuild for ValuesNode<'a> {
//     fn prebuild(self) -> Result<NodeData> {

//             source: QueryNode::Values(values),
//         self.values
//     }
// }
