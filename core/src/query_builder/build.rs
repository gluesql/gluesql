use {
    super::{SchemaNode, WithSchemaNode},
    crate::{plan::StatementPlan, result::Result},
};

pub trait Build {
    fn build(self) -> Result<StatementPlan>;

    /// Supplies a schema for this statement's complete metadata snapshot.
    ///
    /// Accepts CREATE TABLE DDL or a `data::Schema`. Repeat `with_schema` for
    /// additional tables; the last input for a table name wins. Inputs are parsed
    /// by `build`. Planning uses this snapshot without fetching missing schemas.
    ///
    /// ```
    /// use gluesql_core::query_builder::{Build, table};
    /// let plan = table("Item")
    ///     .insert()
    ///     .as_select(table("Source").select())
    ///     .with_schema("CREATE TABLE Item (id INT)")
    ///     .with_schema("CREATE TABLE Source (id INT)")
    ///     .build()?;
    /// # Ok::<(), gluesql_core::error::Error>(())
    /// ```
    fn with_schema<S: Into<SchemaNode>>(self, schema: S) -> WithSchemaNode<Self>
    where
        Self: Sized,
    {
        WithSchemaNode::new(self, schema.into())
    }
}
