//! v3 -> v4: compare schemas inherited from v0.19.0 with current core schemas.
//! v0.20.0 preserved old schemas during migration and wrote new ones with core types.
//! Only unambiguous schemas are upgraded; row payloads are preserved.

use {
    super::{
        MigrationError, MigrationReport, StorageResult, commit_v4,
        decode::{self, DecodeError},
        error::SchemaDecodeError,
        schema_rewrites::SchemaRewrites,
        v2_to_v4::decode_v1_schema,
    },
    gluesql_core::data::Schema as CurrentSchema,
    redb::Database,
};

pub(super) fn migrate(db: &mut Database) -> StorageResult<MigrationReport> {
    // Complete inspection before starting the write transaction. A successful
    // current decode alone is not enough to choose the meaning of a v3 schema.
    let schemas = SchemaRewrites::inspect(db, rewrite_schema)?;
    let txn = db.begin_write()?;
    let report = schemas.apply(&txn)?;
    commit_v4(txn)?;
    Ok(report)
}

fn rewrite_schema(table_name: &str, payload: &[u8]) -> StorageResult<Option<Vec<u8>>> {
    let check_table_name = |schema: CurrentSchema| {
        if schema.table_name == table_name {
            Ok(schema)
        } else {
            Err(SchemaDecodeError::TableNameMismatch)
        }
    };
    let v1_schema = decode_v1_schema(payload).and_then(check_table_name);
    let current_schema = decode::deserialize::<CurrentSchema>(payload)
        .map_err(SchemaDecodeError::from)
        .and_then(check_table_name);

    select_schema(table_name, payload, v1_schema, current_schema)
}

fn select_schema(
    table_name: &str,
    payload: &[u8],
    v1_schema: Result<CurrentSchema, SchemaDecodeError>,
    current_schema: Result<CurrentSchema, SchemaDecodeError>,
) -> StorageResult<Option<Vec<u8>>> {
    match (v1_schema, current_schema) {
        // These failures cannot rule out a source layout. A successful other
        // candidate must not override a depth limit or an unrecoverable v1 plan.
        (
            Err(
                error @ (SchemaDecodeError::UnrecoverableIndexAccessPlan
                | SchemaDecodeError::Payload(DecodeError::NestingLimit)),
            ),
            _,
        )
        | (_, Err(error @ SchemaDecodeError::Payload(DecodeError::NestingLimit))) => {
            Err(MigrationError::InvalidSchemaPayload {
                table_name: table_name.to_owned(),
                source: error,
            }
            .into())
        }

        // Both layouts decode: compare the complete restored schemas,
        // including Decimal scale and float bits. Different meanings stop.
        (Ok(v1), Ok(current)) => {
            if bincode::serialize(&v1)? != bincode::serialize(&current)? {
                return Err(MigrationError::AmbiguousSchemaPayload(table_name.to_owned()).into());
            }
            Ok(None)
        }

        // Only v1 decodes: restore its schema to the current wire format.
        (Ok(v1), Err(_)) => {
            let migrated = bincode::serialize(&v1)?;
            Ok((migrated != payload).then_some(migrated))
        }

        // Only current decodes: preserve its schema bytes.
        (Err(_), Ok(_)) => Ok(None),

        // Neither layout decodes: retain both reasons for the affected table.
        (Err(v1), Err(v3)) => Err(MigrationError::BothSchemaCandidatesFailed {
            table_name: table_name.to_owned(),
            v1,
            v3,
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            CurrentSchema, DecodeError, MigrationError, SchemaDecodeError, rewrite_schema,
            select_schema,
        },
        crate::error::StorageError,
        gluesql_core::{ast::Expr, data::Value},
    };

    fn schema() -> CurrentSchema {
        CurrentSchema::from_ddl("CREATE TABLE Item (id INT);").unwrap()
    }

    #[test]
    fn only_historical_success_rewrites_to_the_restored_schema() {
        let restored = schema();
        assert_eq!(
            select_schema(
                "Item",
                b"historical bytes",
                Ok(restored.clone()),
                Err(SchemaDecodeError::TableNameMismatch)
            )
            .unwrap(),
            Some(bincode::serialize(&restored).unwrap())
        );
        let unchanged = bincode::serialize(&restored).unwrap();
        assert_eq!(
            select_schema(
                "Item",
                &unchanged,
                Ok(restored),
                Err(SchemaDecodeError::TableNameMismatch)
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn only_current_success_preserves_the_original_payload() {
        for error in [
            SchemaDecodeError::Payload(DecodeError::Invalid(Box::new(
                bincode::ErrorKind::InvalidBoolEncoding(2),
            ))),
            SchemaDecodeError::TableNameMismatch,
        ] {
            assert_eq!(
                select_schema("Item", b"original", Err(error), Ok(schema())).unwrap(),
                None
            );
        }
    }

    #[test]
    fn identical_candidates_preserve_the_original_payload() {
        assert_eq!(
            select_schema("Item", b"original", Ok(schema()), Ok(schema())).unwrap(),
            None
        );
    }

    #[test]
    fn different_candidates_are_ambiguous() {
        let mut different = schema();
        different.comment = Some("a distinct stored field".into());
        assert!(matches!(
            select_schema("Item", b"original", Ok(schema()), Ok(different)),
            Err(StorageError::Migration(MigrationError::AmbiguousSchemaPayload(name))) if name == "Item"
        ));
    }

    #[test]
    fn decimal_scale_and_float_bits_are_part_of_candidate_identity() {
        for (left, right) in [
            (
                Value::Decimal("1.0".parse().unwrap()),
                Value::Decimal("1.00".parse().unwrap()),
            ),
            (Value::F64(0.0), Value::F64(-0.0)),
        ] {
            let mut historical = schema();
            historical.column_defs.as_mut().unwrap()[0].default = Some(Expr::Value(left));
            let mut current = schema();
            current.column_defs.as_mut().unwrap()[0].default = Some(Expr::Value(right));
            assert!(matches!(
                select_schema("Item", b"original", Ok(historical), Ok(current)),
                Err(StorageError::Migration(
                    MigrationError::AmbiguousSchemaPayload(name)
                )) if name == "Item"
            ));
        }
    }

    #[test]
    fn two_failures_report_both_candidates_and_the_table() {
        let error = select_schema(
            "Item",
            b"original",
            Err(SchemaDecodeError::Payload(DecodeError::Invalid(Box::new(
                bincode::ErrorKind::InvalidBoolEncoding(2),
            )))),
            Err(SchemaDecodeError::TableNameMismatch),
        )
        .unwrap_err();
        assert!(matches!(error,
            StorageError::Migration(MigrationError::BothSchemaCandidatesFailed {
                table_name,
                v1: SchemaDecodeError::Payload(DecodeError::Invalid(error)),
                v3: SchemaDecodeError::TableNameMismatch,
            })
            if table_name == "Item"
                && matches!(*error, bincode::ErrorKind::InvalidBoolEncoding(2))
        ));
    }

    #[test]
    fn historical_depth_limit_overrides_current_success() {
        let error = select_schema(
            "Item",
            b"original",
            Err(SchemaDecodeError::Payload(DecodeError::NestingLimit)),
            Ok(schema()),
        )
        .unwrap_err();
        assert!(matches!(error,
            StorageError::Migration(MigrationError::InvalidSchemaPayload {
                table_name,
                source: SchemaDecodeError::Payload(DecodeError::NestingLimit),
            }) if table_name == "Item"
        ));
    }

    #[test]
    fn current_depth_limit_overrides_historical_success() {
        let error = select_schema(
            "Item",
            b"original",
            Ok(schema()),
            Err(SchemaDecodeError::Payload(DecodeError::NestingLimit)),
        )
        .unwrap_err();
        assert!(matches!(error,
            StorageError::Migration(MigrationError::InvalidSchemaPayload {
                table_name,
                source: SchemaDecodeError::Payload(DecodeError::NestingLimit),
            }) if table_name == "Item"
        ));
    }

    #[test]
    fn historical_recovery_requirement_overrides_current_success() {
        let error = select_schema(
            "Item",
            b"original",
            Err(SchemaDecodeError::UnrecoverableIndexAccessPlan),
            Ok(schema()),
        )
        .unwrap_err();
        assert!(matches!(error,
            StorageError::Migration(MigrationError::InvalidSchemaPayload {
                table_name,
                source: SchemaDecodeError::UnrecoverableIndexAccessPlan,
            }) if table_name == "Item"
        ));
    }

    #[test]
    fn current_query_is_not_rejected_by_a_partially_decoded_historical_plan() {
        let current = CurrentSchema::from_ddl(
            "CREATE TABLE Item (id INT DEFAULT (
            SELECT A.id FROM Numbers AS A INNER JOIN Numbers AS B ON A.id = B.id
        ));",
        )
        .unwrap();
        let payload = bincode::serialize(&current).unwrap();
        assert_eq!(rewrite_schema("Item", &payload).unwrap(), None);
    }

    #[test]
    fn decoder_depth_failure_reaches_the_schema_policy() {
        let mut current = schema();
        let mut expr = Expr::Value(Value::I64(7));
        for _ in 0..300 {
            expr = Expr::Nested(Box::new(expr));
        }
        current.column_defs.as_mut().unwrap()[0].default = Some(expr);
        let payload = bincode::serialize(&current).unwrap();
        assert!(matches!(rewrite_schema("Item", &payload),
            Err(StorageError::Migration(MigrationError::InvalidSchemaPayload {
                table_name,
                source: SchemaDecodeError::Payload(DecodeError::NestingLimit),
            })) if table_name == "Item"
        ));
    }

    #[test]
    fn both_decoders_validate_the_schema_table_key() {
        let payload = bincode::serialize(&schema()).unwrap();
        assert_eq!(rewrite_schema("Item", &payload).unwrap(), None);
        assert!(matches!(rewrite_schema("Other", &payload),
            Err(StorageError::Migration(MigrationError::BothSchemaCandidatesFailed {
                table_name,
                v1: SchemaDecodeError::TableNameMismatch,
                v3: SchemaDecodeError::TableNameMismatch,
            })) if table_name == "Other"
        ));
    }
}
