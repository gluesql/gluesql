mod columns;

pub use columns::NativeQueryColumnsNode;
use {
    super::{DataTypeNode, SourceNode, ValueNode},
    crate::data::Value,
};

#[derive(Clone, Debug)]
pub struct NativeQueryNode {
    query: String,
    params: Vec<Value>,
}

impl NativeQueryNode {
    /// Appends typed bindings in iterator order.
    /// For mixed Rust types, specify `ValueNode` as the element type:
    ///
    /// ```
    /// use gluesql_core::query_builder::{ValueNode, native_query};
    ///
    /// let query = native_query("SELECT ?1, ?2")
    ///     .params::<ValueNode>([10_i64.into(), "apple".into()]);
    /// ```
    #[must_use]
    pub fn params<T: Into<ValueNode>>(mut self, params: impl IntoIterator<Item = T>) -> Self {
        self.params
            .extend(params.into_iter().map(|value| value.into().0));

        self
    }

    /// Finishes parameter binding and names returned columns by position.
    pub fn columns<'a, T: Into<DataTypeNode>>(
        self,
        columns: impl IntoIterator<Item = (&'a str, T)>,
    ) -> NativeQueryColumnsNode {
        NativeQueryColumnsNode::new(self).columns(columns)
    }

    pub fn alias_as(self, alias: &str) -> SourceNode<'static> {
        NativeQueryColumnsNode::new(self).alias_as(alias)
    }
}

/// Starts a native source for an execution plan.
/// The storage interprets the query and validates its parameters and result shape.
pub fn native_query(query: &str) -> NativeQueryNode {
    NativeQueryNode {
        query: query.to_owned(),
        params: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::DataType,
            data::Value,
            plan::{NativeColumnPlan, NativeSourcePlan, SourcePlan},
            query_builder::{DataTypeNode, ValueNode, native_query, select::BuildSourcePlan},
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn declares_columns_without_parameters() {
        let actual = native_query("SELECT name FROM Item")
            .columns([("label", "TEXT")])
            .alias_as("i")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: "SELECT name FROM Item".to_owned(),
            params: vec![],
            alias: "i".to_owned(),
            columns: vec![NativeColumnPlan {
                name: "label".to_owned(),
                data_type: DataType::Text,
            }],
        });
        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn appends_parameter_lists_in_order() {
        let sql = "SELECT name FROM Item WHERE qty >= ?1 AND qty < ?2 AND name <> ?3";
        let params = vec![Value::Str("hidden".to_owned())];
        let actual = native_query(sql)
            .params([10_i64, 20_i64])
            .params(params)
            .columns([("label", "TEXT")])
            .alias_as("i")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: sql.to_owned(),
            params: vec![
                Value::I64(10),
                Value::I64(20),
                Value::Str("hidden".to_owned()),
            ],
            alias: "i".to_owned(),
            columns: vec![NativeColumnPlan {
                name: "label".to_owned(),
                data_type: DataType::Text,
            }],
        });
        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn accepts_empty_metadata_without_parsing_the_native_query() {
        let actual = native_query("backend-specific query")
            .params::<ValueNode>([])
            .columns::<DataTypeNode>([])
            .alias_as("source")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: "backend-specific query".to_owned(),
            params: vec![],
            alias: "source".to_owned(),
            columns: vec![],
        });
        assert_eq!(actual, Ok(expected));

        // A source with no output columns still retains its bindings.
        let actual = native_query("SELECT 1 WHERE ?1")
            .params([true])
            .alias_as("source")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: "SELECT 1 WHERE ?1".to_owned(),
            params: vec![Value::Bool(true)],
            alias: "source".to_owned(),
            columns: vec![],
        });
        assert_eq!(actual, Ok(expected));
    }
}
