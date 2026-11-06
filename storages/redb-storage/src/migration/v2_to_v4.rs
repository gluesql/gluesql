//! v2 -> v4: restore v1 schemas and upgrade the redb file format.
//! The v1 -> v2 migration changed rows and preserved v0.19.0 schema bytes.
//! Row payloads are already v2 and are preserved.
//!
//! The old schema decoder restores shifted Expr tags in defaults, functions and indexes,
//! wraps the old Projection vector, and restores removed join executor conditions.
//! Removed index access plans require explicit recovery.

mod expr;
mod function;
mod query;

use {
    super::{
        MigrationError, MigrationReport, StorageResult, commit_v4, decode,
        error::SchemaDecodeError, schema_rewrites::SchemaRewrites, v2_to_v3,
    },
    expr::Expr,
    gluesql_core::{
        ast::{self, ColumnUniqueOption, DataType, ForeignKey},
        chrono::NaiveDateTime,
        data::{self, SchemaIndexOrd},
    },
    redb::Database,
    serde::Deserialize,
};

// v1 -> v2 changed rows only, so v2 schemas still have the v0.19.0 layout.
#[derive(Deserialize)]
struct V1Schema {
    table_name: String,
    column_defs: Option<Vec<V1ColumnDef>>,
    indexes: Vec<V1SchemaIndex>,
    engine: Option<String>,
    foreign_keys: Vec<ForeignKey>,
    comment: Option<String>,
}

impl V1Schema {
    fn into_current(self) -> Result<data::Schema, SchemaDecodeError> {
        Ok(data::Schema {
            table_name: self.table_name,
            column_defs: self
                .column_defs
                .map(|columns| columns.into_iter().map(V1ColumnDef::into_current).collect())
                .transpose()?,
            indexes: self
                .indexes
                .into_iter()
                .map(V1SchemaIndex::into_current)
                .collect::<Result<_, _>>()?,
            engine: self.engine,
            foreign_keys: self.foreign_keys,
            comment: self.comment,
        })
    }
}

#[derive(Deserialize)]
struct V1ColumnDef {
    name: String,
    data_type: DataType,
    nullable: bool,
    default: Option<Expr>,
    unique: Option<ColumnUniqueOption>,
    comment: Option<String>,
}

impl V1ColumnDef {
    fn into_current(self) -> Result<ast::ColumnDef, SchemaDecodeError> {
        Ok(ast::ColumnDef {
            name: self.name,
            data_type: self.data_type,
            nullable: self.nullable,
            default: self.default.map(Expr::into_current).transpose()?,
            unique: self.unique,
            comment: self.comment,
        })
    }
}

#[derive(Deserialize)]
struct V1SchemaIndex {
    name: String,
    expr: Expr,
    order: SchemaIndexOrd,
    created: NaiveDateTime,
}

impl V1SchemaIndex {
    fn into_current(self) -> Result<data::SchemaIndex, SchemaDecodeError> {
        Ok(data::SchemaIndex {
            name: self.name,
            expr: self.expr.into_current()?,
            order: self.order,
            created: self.created,
        })
    }
}

// v3 also needs this decoder for schemas inherited from v1/v2. Selection of
// old/current candidates remains in v3_to_v4.
pub(super) fn decode_v1_schema(payload: &[u8]) -> Result<data::Schema, SchemaDecodeError> {
    // Read the entire schema, including EOF, before any conversion can reject it.
    decode::deserialize::<V1Schema>(payload)?.into_current()
}

pub(super) fn migrate(db: &mut Database) -> StorageResult<MigrationReport> {
    let schemas = SchemaRewrites::inspect(db, rewrite_schema)?;
    v2_to_v3::migrate(db)?;

    let txn = db.begin_write()?;
    let report = schemas.apply(&txn)?;
    commit_v4(txn)?;
    Ok(report)
}

// v1 -> v2 preserved schema bytes, so v1 preflight uses this same schema step
// before converting its rows. The source layout is known in both entry paths.
pub(super) fn rewrite_schema(table_name: &str, payload: &[u8]) -> StorageResult<Option<Vec<u8>>> {
    let schema =
        decode_v1_schema(payload).map_err(|source| MigrationError::InvalidSchemaPayload {
            table_name: table_name.to_owned(),
            source,
        })?;
    if schema.table_name != table_name {
        return Err(MigrationError::InvalidSchemaPayload {
            table_name: table_name.to_owned(),
            source: SchemaDecodeError::TableNameMismatch,
        }
        .into());
    }

    let migrated = bincode::serialize(&schema)?;
    Ok((migrated != payload).then_some(migrated))
}

#[cfg(test)]
mod tests {
    use {
        super::{
            MigrationError, SchemaDecodeError, SchemaIndexOrd, V1Schema, ast, data,
            decode_v1_schema, rewrite_schema,
        },
        crate::{
            error::StorageError,
            migration::decode::{self, DecodeError},
        },
        gluesql_core::{
            ast::{ColumnUniqueOption, DataType, ForeignKey, Literal},
            data::{Schema, SchemaIndex},
        },
    };

    fn schema_with_v1_literal_default() -> Vec<u8> {
        // v1 -> v2 preserved DEFAULT 7 with Expr::Literal tag 12 (now 13).
        // Use the v0.19 Schema field order. Only unchanged
        // leaf types are serialized; no migration Schema/Expr serializer is used.
        bincode::serialize(&(
            "Item",
            Some(vec![(
                "id",
                DataType::Int,
                true,
                Some((12_u32, Literal::Number(7.into()))),
                None::<ColumnUniqueOption>,
                None::<String>,
            )]),
            Vec::<SchemaIndex>::new(),
            None::<String>,
            Vec::<ForeignKey>::new(),
            None::<String>,
        ))
        .unwrap()
    }

    #[test]
    fn rewrites_literal_default_after_regex_shifts_its_tag() {
        let expected = Schema::from_ddl("CREATE TABLE Item (id INT DEFAULT 7);").unwrap();
        let payload = schema_with_v1_literal_default();
        let rewritten = rewrite_schema("Item", &payload).unwrap().unwrap();
        assert_ne!(rewritten, payload);
        assert_eq!(
            bincode::deserialize::<Schema>(&rewritten).unwrap(),
            expected
        );
    }

    #[test]
    fn unchanged_layout_does_not_need_a_rewrite() {
        let plain = Schema::from_ddl("CREATE TABLE Item (id INT);").unwrap();
        assert_eq!(
            rewrite_schema("Item", &bincode::serialize(&plain).unwrap()).unwrap(),
            None
        );
    }

    #[test]
    fn rejects_a_mismatched_table_key() {
        assert!(
            matches!(rewrite_schema("Other", &schema_with_v1_literal_default()),
                Err(StorageError::Migration(MigrationError::InvalidSchemaPayload {
                    table_name,
                    source: SchemaDecodeError::TableNameMismatch,
                })) if table_name == "Other"
            )
        );
    }

    #[test]
    fn rejects_incomplete_or_trailing_schema_payloads() {
        let mut trailing = schema_with_v1_literal_default();
        trailing.push(0);
        let mut truncated = schema_with_v1_literal_default();
        truncated.pop();
        for payload in [trailing, truncated, vec![255]] {
            assert!(matches!(rewrite_schema("Item", &payload),
                Err(StorageError::Migration(MigrationError::InvalidSchemaPayload {
                    table_name,
                    source: SchemaDecodeError::Payload(DecodeError::Invalid(_)),
                })) if table_name == "Item"
            ));
        }
    }

    #[test]
    fn restores_shifted_expr_tags_in_index_expressions() {
        // Frozen __SCHEMA__[Queries] bytes from the v0.19.0 release writer
        // (3b26cb7a), generate.rs "index-payload": id + 1, timestamp fixed to epoch.
        // This unit reads a payload directly, independently of any redb file.
        let payload = &[
            7, 0, 0, 0, 0, 0, 0, 0, 81, 117, 101, 114, 105, 101, 115, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0, 105, 100, 4, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 0,
            0, 0, 0, 0, 0, 0, 101, 120, 112, 114, 101, 115, 115, 105, 111, 110, 95, 105, 110, 100,
            101, 120, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 105, 100, 0, 0, 0, 0, 12, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 2, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 49,
            57, 55, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 48, 58, 48, 48, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ];
        let mut expected = data::Schema::from_ddl("CREATE TABLE Queries (id INT);").unwrap();
        expected.indexes.push(data::SchemaIndex {
            name: "expression_index".into(),
            expr: ast::Expr::BinaryOp {
                left: Box::new(ast::Expr::Identifier("id".into())),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expr::Literal(ast::Literal::Number(1.into()))),
            },
            order: SchemaIndexOrd::Both,
            created: "1970-01-01T00:00:00".parse().unwrap(),
        });
        assert_eq!(
            decode::deserialize::<V1Schema>(payload)
                .unwrap()
                .into_current()
                .unwrap(),
            expected
        );
    }

    fn schema_with_index_plan() -> Vec<u8> {
        // v0.19.0 wire fields, independent of the Deserialize-only types:
        // PrimaryKey(Identifier("id")), Table("Source"), Select([Wildcard]).
        let index = (0_u32, (0_u32, "id"));
        let relation = (0_u32, "Source", None::<()>, Some(index));
        let from = (relation, Vec::<()>::new());
        let select = (
            false,
            vec![2_u32],
            from,
            None::<()>,
            Vec::<()>::new(),
            None::<()>,
        );
        let query = ((0_u32, select), Vec::<()>::new(), None::<()>, None::<()>);
        // Expr::Subquery=18; column unique/comment and remaining schema fields absent.
        let column = (
            "id",
            DataType::Int,
            true,
            Some((18_u32, query)),
            None::<()>,
            None::<()>,
        );
        bincode::serialize(&(
            "Item",
            Some(vec![column]),
            Vec::<()>::new(),
            None::<()>,
            Vec::<()>::new(),
            None::<()>,
        ))
        .unwrap()
    }

    #[test]
    fn completely_decoded_index_plan_requires_recovery() {
        assert!(matches!(
            decode_v1_schema(&schema_with_index_plan()),
            Err(SchemaDecodeError::UnrecoverableIndexAccessPlan)
        ));
    }

    #[test]
    fn partial_plan_does_not_hide_a_truncated_or_trailing_payload() {
        let valid = schema_with_index_plan();
        let mut trailing = valid.clone();
        trailing.push(0);
        for payload in [&valid[..valid.len() - 1], trailing.as_slice()] {
            assert!(matches!(
                decode_v1_schema(payload),
                Err(SchemaDecodeError::Payload(DecodeError::Invalid(_)))
            ));
        }
    }
}
