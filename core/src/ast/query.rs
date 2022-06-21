use {
    super::{Expr, IndexOperator, ObjectName},
    // crate::{data::Table, result::Result},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Query {
    pub body: SetExpr,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetExpr {
    Select(Box<Select>),
    Values(Values),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SelectItem {
    /// An expression
    Expr { expr: Expr, label: String },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexItem {
    pub name: String,
    pub asc: Option<bool>,
    pub cmp_expr: Option<(IndexOperator, Expr)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Query planner result
        index: Option<IndexItem>,
    },
    Derived {
        subquery: Query,
        alias: TableAlias,
    },
}
// impl TableFactor {
//     pub(crate) fn get_name(&self) -> Result<&String> {
//         match self {
//             Self::Table { .. } => Ok(Table::new(self)?.get_name()),
//             Self::Derived { alias, .. } => Ok(&alias.name),
//         }
//     }
//     pub(crate) fn get_alias(&self) -> Result<&String> {
//         match self {
//             Self::Table { .. } => Ok(Table::new(self)?.get_alias()),
//             Self::Derived { alias, .. } => Ok(&alias.name),
//         }
//     }
// }

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
    pub join_executor: JoinExecutor,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinExecutor {
    NestedLoop,
    Hash {
        key_expr: Expr,
        value_expr: Expr,
        where_clause: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(Expr),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Values(pub Vec<Vec<Expr>>);
