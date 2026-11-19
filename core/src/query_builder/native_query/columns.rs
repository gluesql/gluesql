use {
    super::NativeQueryNode,
    crate::{
        plan::{NativeColumnPlan, NativeSourcePlan},
        query_builder::{DataTypeNode, SourceNode},
        result::Result,
    },
};

/// Declares native output columns after parameter binding is complete.
/// Further parameters cannot be added once column declarations have started:
///
/// ```compile_fail
/// use gluesql_core::query_builder::native_query;
///
/// native_query("SELECT ?1")
///     .columns([("value", "INT")])
///     .params([1_i64]);
/// ```
#[derive(Clone, Debug)]
pub struct NativeQueryColumnsNode {
    prev_node: NativeQueryNode,
    columns: Vec<(String, DataTypeNode)>,
}

impl NativeQueryColumnsNode {
    pub(super) fn new(prev_node: NativeQueryNode) -> Self {
        Self {
            prev_node,
            columns: Vec::new(),
        }
    }

    /// Appends `GlueSQL` names and types for returned columns in iterator order.
    /// Names do not need to match the native query's column names or aliases.
    #[must_use]
    pub fn columns<'a, T: Into<DataTypeNode>>(
        mut self,
        columns: impl IntoIterator<Item = (&'a str, T)>,
    ) -> Self {
        self.columns.extend(
            columns
                .into_iter()
                .map(|(name, data_type)| (name.to_owned(), data_type.into())),
        );

        self
    }

    pub fn alias_as(self, alias: &str) -> SourceNode<'static> {
        SourceNode::Native {
            query: self,
            alias: alias.to_owned(),
        }
    }

    pub(in crate::query_builder) fn build_native_source_plan(
        self,
        alias: String,
    ) -> Result<NativeSourcePlan> {
        let columns = self
            .columns
            .into_iter()
            .map(|(name, data_type)| {
                Ok(NativeColumnPlan {
                    name,
                    data_type: data_type.try_into()?,
                })
            })
            .collect::<Result<_>>()?;

        Ok(NativeSourcePlan {
            query: self.prev_node.query,
            params: self.prev_node.params,
            alias,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            ast::DataType,
            data::Value,
            plan::{NativeColumnPlan, NativeSourcePlan, SourcePlan},
            query_builder::{
                Build, DataTypeNode, ValueNode, native_query, select::BuildSourcePlan,
            },
            result::Error,
            translate::TranslateError,
        },
        pretty_assertions::assert_eq,
    };

    #[test]
    fn builds_native_source_with_mixed_bindings_and_column_types() {
        let sql = "SELECT name AS tt, qty FROM Item WHERE qty >= ?1 AND name <> ?2";
        let actual = native_query(sql)
            .params::<ValueNode>([10_i64.into(), "hidden".into()])
            .columns::<DataTypeNode>([("label", "TEXT".into()), ("amount", DataType::Int.into())])
            .alias_as("i")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: sql.to_owned(),
            params: vec![Value::I64(10), Value::Str("hidden".to_owned())],
            alias: "i".to_owned(),
            columns: vec![
                NativeColumnPlan {
                    name: "label".to_owned(),
                    data_type: DataType::Text,
                },
                NativeColumnPlan {
                    name: "amount".to_owned(),
                    data_type: DataType::Int,
                },
            ],
        });
        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn appends_column_lists_in_result_order() {
        let sql = "SELECT id, name AS tt, qty FROM Item";
        let actual = native_query(sql)
            .columns([("key", DataType::Int)])
            .columns([("label", "TEXT"), ("amount", "INT")])
            .alias_as("i")
            .select()
            .build_source_plan();
        let expected = SourcePlan::Native(NativeSourcePlan {
            query: sql.to_owned(),
            params: vec![],
            alias: "i".to_owned(),
            columns: vec![
                NativeColumnPlan {
                    name: "key".to_owned(),
                    data_type: DataType::Int,
                },
                NativeColumnPlan {
                    name: "label".to_owned(),
                    data_type: DataType::Text,
                },
                NativeColumnPlan {
                    name: "amount".to_owned(),
                    data_type: DataType::Int,
                },
            ],
        });
        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn propagates_output_type_errors() {
        let actual = native_query("SELECT id, name FROM Item")
            .columns([("id", "INT"), ("name", "MYSTERY")])
            .alias_as("i")
            .select()
            .build();
        let expected = Err(Error::Translate(TranslateError::UnsupportedDataType(
            "MYSTERY".to_owned(),
        )));
        assert_eq!(actual, expected);
    }
}
