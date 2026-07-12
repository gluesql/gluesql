use super::*;

test_case!(add_drop, {
    let g = get_tester!();

    let test_cases = [
        ("CREATE TABLE Foo (id INTEGER);", Ok(Payload::Create)),
        ("INSERT INTO Foo VALUES (1), (2);", Ok(Payload::Insert(2))),
        ("SELECT * FROM Foo;", Ok(select!(id; I64; 1; 2))),
        (
            "ALTER TABLE Foo ADD COLUMN amount INTEGER NOT NULL",
            Err(AlterTableError::DefaultValueRequired(ColumnDef {
                name: "amount".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                unique: None,
                comment: None,
            })
            .into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN id INTEGER",
            Err(AlterTableError::AlreadyExistingColumn("id".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select!(id | amount; I64 | I64; 1 10; 2 10)),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | amount  | opt;
                I64(1)   I64(10)   Null;
                I64(2)   I64(10)   Null
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | amount  | opt  | opt2;
                I64(1)   I64(10)   Null   Bool(true);
                I64(2)   I64(10)   Null   Bool(true)
            )),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something BOOLEAN DEFAULT EXISTS (SELECT id FROM Bar LIMIT 1)",
            Err(EvaluateError::ExistsSubqueryNotAllowedInStatelessExpr.into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something SOMEWHAT",
            Err(TranslateError::UnsupportedDataType("SOMEWHAT".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo ADD COLUMN something FLOAT UNIQUE",
            Err(AlterError::UnsupportedDataTypeForUniqueColumn(
                "something".to_owned(),
                DataType::Float,
            )
            .into()),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS something;",
            Ok(Payload::AlterTable),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN something;",
            Err(AlterTableError::DroppingColumnNotFound("something".to_owned()).into()),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN amount;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | opt  | opt2;
                I64(1)   Null   Bool(true);
                I64(2)   Null   Bool(true)
            )),
        ),
        (
            "ALTER TABLE Foo DROP COLUMN IF EXISTS opt2;",
            Ok(Payload::AlterTable),
        ),
        (
            "SELECT * FROM Foo;",
            Ok(select_with_null!(
                id     | opt;
                I64(1)   Null;
                I64(2)   Null
            )),
        ),
        (
            r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);"#,
            Err(TranslateError::UnsupportedAlterTableOperation(
                r#"ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#.to_owned(),
            )
            .into()),
        ),
        (
            "ALTER TABLE Foo ADD CONSTRAINT hello UNIQUE (id)",
            Err(TranslateError::UnsupportedAlterTableOperation(
                "ADD CONSTRAINT hello UNIQUE (id)".to_owned(),
            )
            .into()),
        ),
        (
            "CREATE TABLE Referenced (id INTEGER PRIMARY KEY);",
            Ok(Payload::Create),
        ),
        (
            "CREATE TABLE Referencing (
                id INTEGER,
                referenced_id INTEGER,
                FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
          );",
            Ok(Payload::Create),
        ),
        (
            "ALTER TABLE Referenced DROP COLUMN id",
            Err(AlterError::CannotAlterReferencedColumn {
                referencing: Referencing {
                    table_name: "Referencing".to_owned(),
                    foreign_key: ForeignKey {
                        name: "FK_referenced_id-Referenced_id".to_owned(),
                        referencing_column_name: "referenced_id".to_owned(),
                        referenced_table_name: "Referenced".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
            }
            .into()),
        ),
        (
            "ALTER TABLE Referenced RENAME COLUMN id to new_id",
            Err(AlterError::CannotAlterReferencedColumn {
                referencing: Referencing {
                    table_name: "Referencing".to_owned(),
                    foreign_key: ForeignKey {
                        name: "FK_referenced_id-Referenced_id".to_owned(),
                        referencing_column_name: "referenced_id".to_owned(),
                        referenced_table_name: "Referenced".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
            }
            .into()),
        ),
        (
            "ALTER TABLE Referencing DROP COLUMN referenced_id",
            Err(AlterError::CannotAlterReferencingColumn {
                referencing: Referencing {
                    table_name: "Referencing".to_owned(),
                    foreign_key: ForeignKey {
                        name: "FK_referenced_id-Referenced_id".to_owned(),
                        referencing_column_name: "referenced_id".to_owned(),
                        referenced_table_name: "Referenced".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
            }
            .into()),
        ),
        (
            "ALTER TABLE Referencing RENAME COLUMN referenced_id to new_id",
            Err(AlterError::CannotAlterReferencingColumn {
                referencing: Referencing {
                    table_name: "Referencing".to_owned(),
                    foreign_key: ForeignKey {
                        name: "FK_referenced_id-Referenced_id".to_owned(),
                        referencing_column_name: "referenced_id".to_owned(),
                        referenced_table_name: "Referenced".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected);
    }
});
