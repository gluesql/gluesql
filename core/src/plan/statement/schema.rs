use {
    super::ColumnDefPlan,
    crate::{
        ast::ForeignKey,
        data::{Schema, SchemaIndex},
    },
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// Prepared schemas keyed by original table name, independent of query aliases.
pub type SchemaMap = BTreeMap<String, SchemaPlan>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SchemaMapPlan {
    /// Schema dependencies have not been prepared.
    #[default]
    Unplanned,

    /// Complete snapshot, including the empty snapshot. Missing keys mean absent tables.
    Planned(SchemaMap),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaPlan {
    pub column_defs: Option<Vec<ColumnDefPlan>>,
    pub indexes: Vec<SchemaIndex>,
    pub engine: Option<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub comment: Option<String>,
}

impl From<Schema> for SchemaPlan {
    fn from(schema: Schema) -> Self {
        let Schema {
            column_defs,
            indexes,
            engine,
            foreign_keys,
            comment,
            ..
        } = schema;

        Self {
            column_defs: column_defs.map(|columns| columns.into_iter().map(Into::into).collect()),
            indexes,
            engine,
            foreign_keys,
            comment,
        }
    }
}

impl SchemaPlan {
    /// Reconstructs the storage representation, preserving original default expressions.
    pub fn to_schema(&self, table_name: &str) -> Schema {
        Schema {
            table_name: table_name.to_owned(),
            column_defs: self
                .column_defs
                .as_ref()
                .map(|columns| columns.iter().map(ColumnDefPlan::to_column_def).collect()),
            indexes: self.indexes.clone(),
            engine: self.engine.clone(),
            foreign_keys: self.foreign_keys.clone(),
            comment: self.comment.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::SchemaPlan,
        crate::{
            ast::{
                ColumnDef, ColumnUniqueOption, DataType, Expr, ForeignKey, Literal,
                ReferentialAction,
            },
            data::{Schema, SchemaIndex, SchemaIndexOrd},
            plan::{ColumnDefPlan, ExprPlan},
        },
    };

    #[test]
    fn from_schema() {
        let column = ColumnDef {
            name: "id".to_owned(),
            data_type: DataType::Int,
            nullable: false,
            default: Some(Expr::Literal(Literal::Number(7.into()))),
            unique: Some(ColumnUniqueOption { is_primary: true }),
            comment: Some("key".to_owned()),
        };

        let index = SchemaIndex {
            name: "by_id".to_owned(),
            expr: Expr::Identifier("id".to_owned()),
            order: SchemaIndexOrd::Desc,
            created: chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        };

        let foreign_key = ForeignKey {
            name: "parent".to_owned(),
            referencing_column_name: "id".to_owned(),
            referenced_table_name: "Parent".to_owned(),
            referenced_column_name: "id".to_owned(),
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        };

        let schema = Schema {
            table_name: "Item".to_owned(),
            column_defs: Some(vec![column.clone()]),
            indexes: vec![index.clone()],
            engine: Some("memory".to_owned()),
            foreign_keys: vec![foreign_key.clone()],
            comment: Some("items".to_owned()),
        };
        let actual = SchemaPlan::from(schema);
        let expected = SchemaPlan {
            column_defs: Some(vec![ColumnDefPlan::from(column)]),
            indexes: vec![index],
            engine: Some("memory".to_owned()),
            foreign_keys: vec![foreign_key],
            comment: Some("items".to_owned()),
        };
        assert_eq!(actual, expected);
        assert_eq!(
            actual.column_defs.as_ref().unwrap()[0]
                .default
                .as_ref()
                .unwrap()
                .planned(),
            &ExprPlan::Literal(Literal::Number(7.into()))
        );

        let actual = SchemaPlan::from(Schema::from_ddl("CREATE TABLE Doc").unwrap());
        let expected = SchemaPlan {
            column_defs: None,
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn to_schema() {
        let schema = Schema::from_ddl(
            "
            CREATE TABLE Item (
                id INT PRIMARY KEY DEFAULT 7 COMMENT 'key',
                parent INT,
                FOREIGN KEY (parent) REFERENCES Parent(id)
            ) ENGINE = memory COMMENT = 'items';
            CREATE INDEX by_id ON Item(id);
        ",
        )
        .unwrap();
        let input = SchemaPlan::from(schema.clone());
        let actual = input.to_schema("Renamed");
        let expected = Schema {
            table_name: "Renamed".to_owned(),
            ..schema
        };
        assert_eq!(actual, expected);

        let input = SchemaPlan {
            column_defs: None,
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        };
        let actual = input.to_schema("Doc");
        let expected = Schema::from_ddl("CREATE TABLE Doc").unwrap();
        assert_eq!(actual, expected);
    }
}
