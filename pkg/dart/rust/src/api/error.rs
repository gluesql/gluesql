pub use flutter_rust_bridge::frb;
pub use gluesql_core::{
    ast::{Aggregate, BinaryOperator, ColumnDef, DataType, DateTimeField, Expr, ToSql},
    chrono::{NaiveTime, ParseError},
    data::{
        ConvertError, Interval, IntervalError, Key, KeyError, LiteralError, NumericBinaryOperator,
        RowError, SchemaParseError, StringExtError, TableError, Value, ValueError,
    },
    error::{
        AggregateError, AlterError, AstBuilderError, Error, EvaluateError, ExecuteError,
        FetchError, InsertError, SelectError, SortError, TranslateError, UpdateError,
        ValidateError,
    },
    plan::PlanError,
    store::{AlterTableError, IndexError},
};
use serde::Serializer;
pub use {serde::Serialize, std::fmt::Debug, thiserror::Error as ThisError};

#[frb(mirror(Error), non_opaque)]
pub enum _Error {
    StorageMsg(String),
    Parser(String),
    Translate(TranslateError),
    AstBuilder(AstBuilderError),
    AlterTable(AlterTableError),
    Index(IndexError),
    Execute(ExecuteError),
    Alter(AlterError),
    Fetch(FetchError),
    Select(SelectError),
    Evaluate(EvaluateError),
    Aggregate(AggregateError),
    Sort(SortError),
    Insert(InsertError),
    Update(UpdateError),
    Table(TableError),
    Validate(ValidateError),
    Row(RowError),
    Key(KeyError),
    Value(ValueError),
    Convert(ConvertError),
    Literal(LiteralError),
    Interval(IntervalError),
    StringExt(StringExtError),
    Plan(PlanError),
    Schema(SchemaParseError),
}

#[frb(mirror(TranslateError), non_opaque)]
#[derive(ThisError, Debug)]
pub enum _TranslateError {
    #[error("unimplemented - select on two or more than tables are not supported")]
    TooManyTables,

    #[error("unimplemented - select distinct is not supported")]
    SelectDistinctNotSupported,

    #[error("unimplemented - composite index is not supported")]
    CompositeIndexNotSupported,

    #[error("unimplemented - join on update not supported")]
    JoinOnUpdateNotSupported,

    #[error("unimplemented - compound identifier on update not supported: {0}")]
    CompoundIdentOnUpdateNotSupported(String),

    #[error("too many params in drop index")]
    TooManyParamsInDropIndex,

    #[error("invalid params in drop index, expected: table_name.index_name")]
    InvalidParamsInDropIndex,

    #[error("function args.length not matching: {name}, expected: {expected}, found: {found}")]
    FunctionArgsLengthNotMatching {
        name: String,
        expected: usize,
        found: usize,
    },

    #[error("function {name} requires at least {expected_minimum} argument(s), found: {found}")]
    FunctionArgsLengthNotMatchingMin {
        name: String,
        expected_minimum: usize,
        found: usize,
    },

    #[error("function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}")]
    FunctionArgsLengthNotWithinRange {
        name: String,
        expected_minimum: usize,
        expected_maximum: usize,
        found: usize,
    },

    #[error("named function arg is not supported")]
    NamedFunctionArgNotSupported,

    #[error("unnamed function arg is not supported")]
    UnNamedFunctionArgNotSupported,

    #[error("INSERT INTO {0} DEFAULT VALUES is not supported")]
    DefaultValuesOnInsertNotSupported(String),

    #[error("empty function body is not supported")]
    UnsupportedEmptyFunctionBody,

    #[error("unsupported unnamed index")]
    UnsupportedUnnamedIndex,

    #[error("unsupported trim chars: expected: `TRIM((BOTH | LEADING | TRAILING) <text> FROM <expr>)`, got: `TRIM(<expr> [<chars>, ..])` syntax")]
    UnsupportedTrimChars,

    #[error("unsupported CAST format: {0}")]
    UnsupportedCastFormat(String),

    #[error("unsupported multiple alter table operations, expected: `ALTER TABLE <table> <operation>`, got: `ALTER TABLE <table> <operation>, <operation>, ..`")]
    UnsupportedMultipleAlterTableOperations,

    #[error("unreachable empty alter table operation")]
    UnreachableEmptyAlterTableOperation,

    #[error("unsupported `GROUP BY (ALL)`")]
    UnsupportedGroupByAll,

    #[error("wildcard function arg is not accepted")]
    WildcardFunctionArgNotAccepted,

    #[error("qualified wildcard is not supported - COUNT({0})")]
    QualifiedWildcardInCountNotSupported(String),

    #[error("order by - NULLS (FIRST | LAST) is not supported")]
    OrderByNullsFirstOrLastNotSupported,

    #[error("unsupported SHOW VARIABLE keyword: {0}")]
    UnsupportedShowVariableKeyword(String),

    #[error("unsupported SHOW VARIABLE statement: {0}")]
    UnsupportedShowVariableStatement(String),

    #[error("unsupported statement: {0}")]
    UnsupportedStatement(String),

    #[error("unsupported expr: {0}")]
    UnsupportedExpr(String),

    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),

    #[error("unsupported datetime field: {0}")]
    UnsupportedDateTimeField(String),

    #[error("unsupported ast literal: {0}")]
    UnsupportedAstLiteral(String),

    #[error("unreachable unary operator: {0}")]
    UnreachableUnaryOperator(String),

    #[error("unreachable empty ident")]
    UnreachableEmptyIdent,

    #[error("unsupported binary operator: {0}")]
    UnsupportedBinaryOperator(String),

    #[error("unsupported query set expr: {0}")]
    UnsupportedQuerySetExpr(String),

    #[error("unsupported query table factor: {0}")]
    UnsupportedQueryTableFactor(String),

    #[error("unsupported join constraint: {0}")]
    UnsupportedJoinConstraint(String),

    #[error("unsupported join operator: {0}")]
    UnsupportedJoinOperator(String),

    #[error("unsupported column option: {0}")]
    UnsupportedColumnOption(String),

    #[error("unsupported alter table operation: {0}")]
    UnsupportedAlterTableOperation(String),

    #[error("unsupported table factor: {0}")]
    UnsupportedTableFactor(String),

    #[error("Every derived table must have its own alias")]
    LackOfAlias,

    #[error("Series should have size")]
    LackOfArgs,

    #[error("unreachable empty object")]
    UnreachableEmptyObject,

    #[error("unreachable empty table")]
    UnreachableEmptyTable,

    #[error("unimplemented - compound object is supported: {0}")]
    CompoundObjectNotSupported(String),

    #[error("cannot create index with reserved name: {0}")]
    ReservedIndexName(String),

    #[error("cannot drop primary index")]
    CannotDropPrimary,
}

#[frb(mirror(AstBuilderError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _AstBuilderError {
    #[error("failed to parse numeric value: {0}")]
    FailedToParseNumeric(String),
}

#[frb(mirror(AlterTableError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _AlterTableError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Renaming column not found")]
    RenamingColumnNotFound,

    #[error("Default value is required: {0:#?}")]
    DefaultValueRequired(ColumnDef),

    #[error("Already existing column: {0}")]
    AlreadyExistingColumn(String),

    #[error("Dropping column not found: {0}")]
    DroppingColumnNotFound(String),

    #[error("Schemaless table does not support ALTER TABLE: {0}")]
    SchemalessTableFound(String),
}

#[frb(mirror(IndexError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _IndexError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("index name already exists: {0}")]
    IndexNameAlreadyExists(String),

    #[error("index name does not exist: {0}")]
    IndexNameDoesNotExist(String),

    #[error("conflict - table not found: {0}")]
    ConflictTableNotFound(String),

    #[error("conflict - update failed - index value")]
    ConflictOnEmptyIndexValueUpdate,

    #[error("conflict - delete failed - index value")]
    ConflictOnEmptyIndexValueDelete,

    #[error("conflict - scan failed - index value")]
    ConflictOnEmptyIndexValueScan,

    #[error("conflict - index sync - delete index data")]
    ConflictOnIndexDataDeleteSync,
}

#[frb(mirror(ExecuteError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _ExecuteError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[frb(mirror(AlterError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _AlterError {
    // CREATE TABLE
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("function already exists: {0}")]
    FunctionAlreadyExists(String),

    #[error("function does not exist: {0}")]
    FunctionNotFound(String),

    // CREATE INDEX, DROP TABLE
    #[error("table does not exist: {0}")]
    TableNotFound(String),

    #[error("CTAS source table does not exist: {0}")]
    CtasSourceTableNotFound(String),

    // validate column def
    #[error("column '{0}' of data type '{1:?}' is unsupported for unique constraint")]
    UnsupportedDataTypeForUniqueColumn(String, DataType),

    // validate index expr
    #[error("unsupported index expr: {0:#?}")]
    UnsupportedIndexExpr(Expr),

    // validate index expr
    #[error("unsupported unnamed argument")]
    UnsupportedUnnamedArg,

    #[error("identifier not found: {0:#?}")]
    IdentifierNotFound(Expr),

    #[error("duplicate column name: {0}")]
    DuplicateColumnName(String),

    #[error("duplicate arg name: {0}")]
    DuplicateArgName(String),

    #[error("non-default argument should not follow the default argument")]
    NonDefaultArgumentFollowsDefaultArgument,
}

#[frb(mirror(FetchError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table alias not found: {0}")]
    TableAliasNotFound(String),

    #[error("SERIES has wrong size: {0}")]
    SeriesSizeWrong(i64),

    #[error("table '{0}' has {1} columns available but {2} column aliases specified")]
    TooManyColumnAliases(String, usize, usize),
}

#[frb(mirror(SelectError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _SelectError {
    #[error("VALUES lists must all be the same length")]
    NumberOfValuesDifferent,
}

#[frb(mirror(EvaluateError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _EvaluateError {
    #[error(transparent)]
    #[serde(serialize_with = "error_serialize")]
    FormatParseError(#[from] ParseError),

    #[error("literal add on non-numeric")]
    LiteralAddOnNonNumeric,

    #[error("function requires string value: {0}")]
    FunctionRequiresStringValue(String),

    #[error("function requires integer value: {0}")]
    FunctionRequiresIntegerValue(String),

    #[error("function requires float or integer value: {0}")]
    FunctionRequiresFloatOrIntegerValue(String),

    #[error("function requires usize value: {0}")]
    FunctionRequiresUSizeValue(String),

    #[error("function requires float value: {0}")]
    FunctionRequiresFloatValue(String),

    #[error("extract format does not support value: {0}")]
    ExtractFormatNotMatched(String),

    #[error("function requires map value: {0}")]
    FunctionRequiresMapValue(String),

    #[error("function requires point value: {0}")]
    FunctionRequiresPointValue(String),

    #[error("function requires date or datetime value: {0}")]
    FunctionRequiresDateOrDateTimeValue(String),

    #[error("function requires one of string, list, map types: {0}")]
    FunctionRequiresStrOrListOrMapValue(String),

    #[error("value not found: {0}")]
    ValueNotFound(String),

    #[error("only boolean value is accepted: {0}")]
    BooleanTypeRequired(String),

    #[error("expr requires map or list value")]
    MapOrListTypeRequired,

    #[error("expr requires map value")]
    MapTypeRequired,

    #[error("expr requires list value")]
    ListTypeRequired,

    #[error("all elements in the list must be comparable to each other")]
    InvalidSortType,

    #[error("sort order must be either ASC or DESC")]
    InvalidSortOrder,

    #[error("map or string value required for json map conversion: {0}")]
    MapOrStringValueRequired(String),

    #[error("text literal required for json map conversion: {0}")]
    TextLiteralRequired(String),

    #[error("unsupported stateless expression: {}", .0.to_sql())]
    UnsupportedStatelessExpr(Expr),

    #[error("context is required for identifier evaluation: {}", .0.to_sql())]
    ContextRequiredForIdentEvaluation(Expr),

    #[error("unreachable empty aggregate value: {0:?}")]
    UnreachableEmptyAggregateValue(Aggregate),

    #[error("incompatible bit operation between {0} and {1}")]
    IncompatibleBitOperation(String, String),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("negative substring length not allowed")]
    NegativeSubstrLenNotAllowed,

    #[error("subquery returns more than one row")]
    MoreThanOneRowReturned,

    #[error("subquery returns more than one column")]
    MoreThanOneColumnReturned,

    #[error("schemaless projection is not allowed for IN (subquery)")]
    SchemalessProjectionForInSubQuery,

    #[error("schemaless projection is not allowed for subquery")]
    SchemalessProjectionForSubQuery,

    #[error("format function does not support following data_type: {0}")]
    UnsupportedExprForFormatFunction(String),

    #[error("support single character only")]
    AsciiFunctionRequiresSingleCharacterValue,

    #[error("non-ascii character not allowed")]
    NonAsciiCharacterNotAllowed,

    #[error("function requires integer value in range")]
    ChrFunctionRequiresIntegerValueInRange0To255,

    #[error("unsupported evaluate binary operation {} {} {}", .left, .op.to_sql(), .right)]
    UnsupportedBinaryOperation {
        left: String,
        op: BinaryOperator,
        right: String,
    },

    #[error("unsupported evaluate string unary plus: {0}")]
    UnsupportedUnaryPlus(String),

    #[error("unsupported evaluate string unary minus: {0}")]
    UnsupportedUnaryMinus(String),

    #[error("unsupported evaluate string unary factorial: {0}")]
    UnsupportedUnaryFactorial(String),

    #[error("incompatible bit operation ~{0}")]
    IncompatibleUnaryBitwiseNotOperation(String),

    #[error("unsupported custom function in subqueries")]
    UnsupportedCustomFunction,

    #[error(r#"The function "{function_name}" requires at least {required_minimum} argument(s), but {found} were provided."#)]
    FunctionRequiresMoreArguments {
        function_name: String,
        required_minimum: usize,
        found: usize,
    },

    #[error("function args.length not matching: {name}, expected: {expected_minimum} ~ {expected_maximum}, found: {found}")]
    FunctionArgsLengthNotWithinRange {
        name: String,
        expected_minimum: usize,
        expected_maximum: usize,
        found: usize,
    },

    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("The provided arguments are non-comparable: {0}")]
    NonComparableArgumentError(String),

    #[error("function requires at least one argument: {0}")]
    FunctionRequiresAtLeastOneArgument(String),
}
fn error_serialize<S>(error: &ParseError, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let display = format!("{}", error);
    serializer.serialize_str(&display)
}

#[frb(mirror(AggregateError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _AggregateError {
    #[error("unreachable rc unwrap failure")]
    UnreachableRcUnwrapFailure,
}

#[frb(mirror(SortError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _SortError {
    #[error("ORDER BY COLUMN_INDEX must be within SELECT-list but: {0}")]
    ColumnIndexOutOfRange(usize),
    #[error("Unreachable ORDER BY Clause")]
    Unreachable,
}

#[frb(mirror(InsertError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _InsertError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("lack of required column: {0}")]
    LackOfRequiredColumn(String),

    #[error("wrong column name: {0}")]
    WrongColumnName(String),

    #[error("column and values not matched")]
    ColumnAndValuesNotMatched,

    #[error("literals have more values than target columns")]
    TooManyValues,

    #[error("only single value accepted for schemaless row insert")]
    OnlySingleValueAcceptedForSchemalessRow,

    #[error("map type required: {0}")]
    MapTypeValueRequired(String),
}

#[frb(mirror(UpdateError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _UpdateError {
    #[error("column not found {0}")]
    ColumnNotFound(String),

    #[error("update on primary key is not supported: {0}")]
    UpdateOnPrimaryKeyNotSupported(String),

    #[error("conflict on schema, row data does not fit to schema")]
    ConflictOnSchema,
}

#[frb(mirror(TableError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _TableError {
    #[error("unreachable")]
    Unreachable,
}

#[frb(mirror(ValidateError), non_opaque)]
#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum _ValidateError {
    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("conflict! schemaless row found in schema based data")]
    ConflictOnUnexpectedSchemalessRowFound,

    #[error("duplicate entry '{}' for unique column '{1}'", String::from(.0))]
    DuplicateEntryOnUniqueField(Value, String),

    #[error("duplicate entry '{0:?}' for primary_key field")]
    DuplicateEntryOnPrimaryKeyField(Key),
}

#[frb(mirror(RowError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _RowError {
    #[error("conflict - vec expected but map row found")]
    ConflictOnUnexpectedMapRowFound,

    #[error("conflict - map expected but vec row found")]
    ConflictOnUnexpectedVecRowFound,
}

#[frb(mirror(KeyError), non_opaque)]
#[derive(ThisError, Debug, PartialEq, Eq, Serialize)]
pub enum _KeyError {
    #[error("FLOAT data type cannot be converted to Big-Endian bytes for comparison")]
    FloatToCmpBigEndianNotSupported,

    #[error("MAP data type cannot be used as Key")]
    MapTypeKeyNotSupported,

    #[error("LIST data type cannot be used as Key")]
    ListTypeKeyNotSupported,

    #[error("POINT data type cannot be used as Key")]
    PointTypeKeyNotSupported,
}

#[frb(mirror(ValueError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum _ValueError {
    #[error("literal: {literal} is incompatible with data type: {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },

    #[error("incompatible data type, data type: {data_type:#?}, value: {value:#?}")]
    IncompatibleDataType { data_type: DataType, value: Value },

    #[error("null value on not null field")]
    NullValueOnNotNullField,

    #[error("failed to parse number")]
    FailedToParseNumber,

    #[error("failed to convert Float to Decimal: {0}")]
    FloatToDecimalConversionFailure(f64),

    #[error("failed to parse date: {0}")]
    FailedToParseDate(String),

    #[error("failed to parse timestamp: {0}")]
    FailedToParseTimestamp(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to UUID: {0}")]
    FailedToParseUUID(String),

    #[error("failed to parse point: {0}")]
    FailedToParsePoint(String),

    #[error("failed to parse Decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse hex string: {0}")]
    FailedToParseHexString(String),

    #[error("failed to parse inet string: {0}")]
    FailedToParseInetString(String),

    #[error("non-numeric values {lhs:?} {operator} {rhs:?}")]
    NonNumericMathOperation {
        lhs: Value,
        rhs: Value,
        operator: NumericBinaryOperator,
    },

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("unary plus operation for non numeric value")]
    UnaryPlusOnNonNumeric,

    #[error("unary minus operation for non numeric value")]
    UnaryMinusOnNonNumeric,

    #[error("unary factorial operation for non numeric value")]
    FactorialOnNonNumeric,

    #[error("unary factorial operation for non integer value")]
    FactorialOnNonInteger,

    #[error("unary factorial operation for negative numeric value")]
    FactorialOnNegativeNumeric,

    #[error("unary factorial operation overflow")]
    FactorialOverflow,

    #[error("GCD or LCM calculation overflowed on trying to get the absolute value of {0}")]
    GcdLcmOverflow(i64),

    #[error("LCM calculation resulted in a value out of the i64 range")]
    LcmResultOutOfRange,

    #[error("unary bit_not operation for non numeric value")]
    UnaryBitwiseNotOnNonNumeric,

    #[error("unary bit_not operation for non integer value")]
    UnaryBitwiseNotOnNonInteger,

    #[error("unreachable failure on parsing number")]
    UnreachableNumberParsing,

    #[error("unimplemented cast: {value:?} as {data_type}")]
    UnimplementedCast { value: Value, data_type: DataType },

    #[error("failed to cast from hex string to bytea: {0}")]
    CastFromHexToByteaFailed(String),

    #[error("function CONCAT requires at least 1 argument")]
    EmptyArgNotAllowedInConcat,

    // Cast errors from literal to value
    #[error("literal cast failed from text to integer: {0}")]
    LiteralCastFromTextToIntegerFailed(String),

    #[error("literal cast failed from text to Unsigned Int(8): {0}")]
    LiteralCastFromTextToUnsignedInt8Failed(String),

    #[error("literal cast failed from text to UINT16: {0}")]
    LiteralCastFromTextToUint16Failed(String),

    #[error("literal cast failed from text to UINT32: {0}")]
    LiteralCastFromTextToUint32Failed(String),

    #[error("literal cast failed from text to UINT64: {0}")]
    LiteralCastFromTextToUint64Failed(String),

    #[error("literal cast failed from text to UINT128: {0}")]
    LiteralCastFromTextToUint128Failed(String),

    #[error("literal cast failed from text to float: {0}")]
    LiteralCastFromTextToFloatFailed(String),

    #[error("literal cast failed from text to decimal: {0}")]
    LiteralCastFromTextToDecimalFailed(String),

    #[error("literal cast failed to boolean: {0}")]
    LiteralCastToBooleanFailed(String),

    #[error("literal cast failed to date: {0}")]
    LiteralCastToDateFailed(String),

    #[error("literal cast from {1} to {0} failed")]
    LiteralCastToDataTypeFailed(DataType, String),

    #[error("literal cast failed to Int(8): {0}")]
    LiteralCastToInt8Failed(String),

    #[error("literal cast failed to Unsigned Int(8): {0}")]
    LiteralCastToUnsignedInt8Failed(String),

    #[error("literal cast failed to UINT16: {0}")]
    LiteralCastToUint16Failed(String),

    #[error("literal cast failed to UNIT32: {0}")]
    LiteralCastToUint32Failed(String),

    #[error("literal cast failed to UNIT64: {0}")]
    LiteralCastToUint64Failed(String),

    #[error("literal cast failed to UNIT128: {0}")]
    LiteralCastToUint128Failed(String),

    #[error("literal cast failed to time: {0}")]
    LiteralCastToTimeFailed(String),

    #[error("literal cast failed to timestamp: {0}")]
    LiteralCastToTimestampFailed(String),

    #[error("unreachable literal cast from number to integer: {0}")]
    UnreachableLiteralCastFromNumberToInteger(String),

    #[error("unreachable literal cast from number to float: {0}")]
    UnreachableLiteralCastFromNumberToFloat(String),

    #[error("unimplemented literal cast: {literal} as {data_type:?}")]
    UnimplementedLiteralCast {
        data_type: DataType,
        literal: String,
    },

    #[error("unreachable integer overflow: {0}")]
    UnreachableIntegerOverflow(String),

    #[error("operator doesn't exist: {base:?} {case} {pattern:?}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: Value,
        pattern: Value,
        case_sensitive: bool,
    },

    #[error("extract format not matched: {value:?} FROM {field:?})")]
    ExtractFormatNotMatched { value: Value, field: DateTimeField },

    #[error("big endian export not supported for {0} type")]
    BigEndianExportNotSupported(String),

    #[error("invalid json string: {0}")]
    InvalidJsonString(String),

    #[error("json object type is required")]
    JsonObjectTypeRequired,

    #[error("json array type is required")]
    JsonArrayTypeRequired,

    #[error("unreachable - failed to parse json number: {0}")]
    UnreachableJsonNumberParseFailure(String),

    #[error("selector requires MAP or LIST types")]
    SelectorRequiresMapOrListTypes,

    #[error("overflow occurred: {lhs:?} {operator} {rhs:?}")]
    BinaryOperationOverflow {
        lhs: Value,
        rhs: Value,
        operator: NumericBinaryOperator,
    },

    #[error("non numeric value in sqrt {0:?}")]
    SqrtOnNonNumeric(Value),

    #[error("non-string parameter in position: {} IN {}", String::from(.from), String::from(.sub))]
    NonStringParameterInPosition { from: Value, sub: Value },

    #[error("non-string parameter in find idx: {}, {}", String::from(.sub), String::from(.from))]
    NonStringParameterInFindIdx { sub: Value, from: Value },

    #[error("non positive offset in find idx: {0}")]
    NonPositiveIntegerOffsetInFindIdx(String),

    #[error("failed to convert Value to Expr")]
    ValueToExprConversionFailure,

    #[error("failed to convert Value to u32: {0}")]
    I64ToU32ConversionFailure(String),
}
#[frb(mirror(ConvertError), non_opaque)]
#[derive(Debug, Serialize, ThisError, PartialEq)]
#[error("failed to convert value({value:?}) to data type({data_type})")]
pub struct _ConvertError {
    pub value: Value,
    pub data_type: DataType,
}

#[frb(mirror(LiteralError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _LiteralError {
    #[error("unsupported literal binary operation {} {} {}", .left, .op.to_sql(), .right)]
    UnsupportedBinaryOperation {
        left: String,
        op: BinaryOperator,
        right: String,
    },

    #[error("a operand '{0}' is not integer type")]
    BitwiseNonIntegerOperand(String),

    #[error("given operands is not Number literal type")]
    BitwiseNonNumberLiteral,

    #[error("overflow occured while running bitwise operation")]
    BitwiseOperationOverflow,

    #[error("impossible conversion from {0} to {1} type")]
    ImpossibleConversion(String, String),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("literal unary operation on non-numeric")]
    UnaryOperationOnNonNumeric,

    #[error("unreachable literal binary arithmetic")]
    UnreachableBinaryArithmetic,

    #[error("unreachable literal unary operation")]
    UnreachableUnaryOperation,

    #[error("failed to decode hex string: {0}")]
    FailedToDecodeHexString(String),

    #[error("operator doesn't exist: {base:?} {case} {pattern:?}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: String,
        pattern: String,
        case_sensitive: bool,
    },
}

#[frb(mirror(IntervalError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _IntervalError {
    #[error("unsupported interval range: {0} to {1}")]
    UnsupportedRange(String, String),

    #[error("cannot add between YEAR TO MONTH and HOUR TO SECOND")]
    AddBetweenYearToMonthAndHourToSecond,

    #[error("cannot subtract between YEAR TO MONTH and HOUR TO SECOND")]
    SubtractBetweenYearToMonthAndHourToSecond,

    #[error("cannot add year or month to TIME: {time} + {interval}", time = time.to_string(), interval = interval.to_sql_str())]
    AddYearOrMonthToTime { time: NaiveTime, interval: Interval },

    #[error("cannot subtract year or month to TIME: {time} - {interval}", time = time.to_string(), interval = interval.to_sql_str())]
    SubtractYearOrMonthToTime { time: NaiveTime, interval: Interval },

    #[error("failed to parse integer: {0}")]
    FailedToParseInteger(String),

    #[error("failed to parse decimal: {0}")]
    FailedToParseDecimal(String),

    #[error("failed to parse time: {0}")]
    FailedToParseTime(String),

    #[error("failed to parse YEAR TO MONTH (year-month, ex. 2-8): {0}")]
    FailedToParseYearToMonth(String),

    #[error("failed to parse DAY TO HOUR (day hour, ex. 1 23): {0}")]
    FailedToParseDayToHour(String),

    #[error("failed to parse DAY TO MINUTE (day hh:mm, ex. 1 12:34): {0}")]
    FailedToParseDayToMinute(String),

    #[error("failed to parse DAY TO SECOND (day hh:mm:ss, ex. 1 12:34:55): {0}")]
    FailedToParseDayToSecond(String),

    #[error("date overflow: {year}-{month}")]
    DateOverflow { year: i32, month: i32 },

    #[error("failed to get extract from interval")]
    FailedToExtract,

    #[error("parse supported only literal, expected: \"'1 1' DAY TO HOUR\", but got: {expr}", expr = expr.to_sql())]
    ParseSupportedOnlyLiteral { expr: Expr },

    #[error("unreachable")]
    Unreachable,
}

#[frb(mirror(StringExtError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _StringExtError {
    #[error("unreachable literal unary operation")]
    UnreachablePatternParsing,
}

#[frb(mirror(PlanError), non_opaque)]
#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum _PlanError {
    /// Error that that omits when user projects common column name from multiple tables in `JOIN`
    /// situation.
    #[error("column reference {0} is ambiguous, please specify the table name")]
    ColumnReferenceAmbiguous(String),
}

#[frb(mirror(SchemaParseError), non_opaque)]
#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum _SchemaParseError {
    #[error("cannot parse ddl")]
    CannotParseDDL,
}

#[frb(mirror(NumericBinaryOperator), non_opaque)]
pub enum _NumericBinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    BitwiseAnd,
    BitwiseShiftLeft,
    BitwiseShiftRight,
}
