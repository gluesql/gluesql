//! Expected layouts of the current core types persisted by redb.
//!
//! Review storage compatibility before changing these declarations. Compatible
//! changes only need a declaration update; incompatible changes also need a
//! migration. Keep one current baseline here; historical types belong to migration.
//! These checks cover type structure, not changes to custom Serde implementations
//! or the comparison-key encoding.

mod macros;
mod serde_metadata;

use {
    self::macros::{enum_layout, struct_layout},
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::{
            Aggregate, AggregateFunction, BinaryOperator, ColumnDef, ColumnUniqueOption,
            CountArgExpr, DataType, DateTimeField, Dictionary, Expr, ForeignKey, Function, Join,
            JoinConstraint, JoinOperator, Literal, OrderByExpr, Projection, Query,
            ReferentialAction, Select, SelectItem, SetExpr, TableAlias, TableFactor,
            TableWithJoins, TrimWhereField, UnaryOperator, Values,
        },
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        data::{Interval, Key, Point, Schema, SchemaIndex, SchemaIndexOrd, Value},
    },
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    std::{collections::BTreeMap, net::IpAddr},
};

// Schema and column definitions

struct_layout! {
    schema_layout: Schema {
        table_name: String,
        column_defs: Option<Vec<ColumnDef>>,
        indexes: Vec<SchemaIndex>,
        engine: Option<String>,
        foreign_keys: Vec<ForeignKey>,
        comment: Option<String>,
    }
}

struct_layout! {
    schema_index_layout: SchemaIndex {
        name: String,
        expr: Expr,
        order: SchemaIndexOrd,
        created: NaiveDateTime,
    }
}

enum_layout! {
    schema_index_ord_layout: SchemaIndexOrd {
        Asc,
        Desc,
        Both,
    }
}

struct_layout! {
    column_def_layout: ColumnDef {
        name: String,
        data_type: DataType,
        nullable: bool,
        default: Option<Expr>,
        unique: Option<ColumnUniqueOption>,
        comment: Option<String>,
    }
}

struct_layout! {
    column_unique_option_layout: ColumnUniqueOption {
        is_primary: bool,
    }
}

struct_layout! {
    foreign_key_layout: ForeignKey {
        name: String,
        referencing_column_name: String,
        referenced_table_name: String,
        referenced_column_name: String,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    }
}

enum_layout! {
    referential_action_layout: ReferentialAction {
        NoAction,
    }
}

// Expressions and scalar types

enum_layout! {
    expr_layout: Expr {
        Identifier(value: String),
        CompoundIdentifier {
            alias: String,
            ident: String,
        },
        IsNull(value: Box<Expr>),
        IsNotNull(value: Box<Expr>),
        InList {
            expr: Box<Expr>,
            list: Vec<Expr>,
            negated: bool,
        },
        InSubquery {
            expr: Box<Expr>,
            subquery: Box<Query>,
            negated: bool,
        },
        Between {
            expr: Box<Expr>,
            negated: bool,
            low: Box<Expr>,
            high: Box<Expr>,
        },
        Like {
            expr: Box<Expr>,
            negated: bool,
            pattern: Box<Expr>,
        },
        ILike {
            expr: Box<Expr>,
            negated: bool,
            pattern: Box<Expr>,
        },
        Regex {
            expr: Box<Expr>,
            negated: bool,
            pattern: Box<Expr>,
            case_sensitive: bool,
        },
        BinaryOp {
            left: Box<Expr>,
            op: BinaryOperator,
            right: Box<Expr>,
        },
        UnaryOp {
            op: UnaryOperator,
            expr: Box<Expr>,
        },
        Nested(value: Box<Expr>),
        Literal(value: Literal),
        Value(value: Value),
        TypedString {
            data_type: DataType,
            value: String,
        },
        Function(value: Box<Function>),
        Aggregate(value: Box<Aggregate>),
        Exists {
            subquery: Box<Query>,
            negated: bool,
        },
        Subquery(value: Box<Query>),
        Case {
            operand: Option<Box<Expr>>,
            when_then: Vec<(Expr, Expr)>,
            else_result: Option<Box<Expr>>,
        },
        ArrayIndex {
            obj: Box<Expr>,
            indexes: Vec<Expr>,
        },
        Interval {
            expr: Box<Expr>,
            leading_field: Option<DateTimeField>,
            last_field: Option<DateTimeField>,
        },
        Array {
            elem: Vec<Expr>,
        },
    }
}

enum_layout! {
    function_layout: Function {
        Abs(value: Expr),
        AddMonth {
            expr: Expr,
            size: Expr,
        },
        Lower(value: Expr),
        Initcap(value: Expr),
        Upper(value: Expr),
        Left {
            expr: Expr,
            size: Expr,
        },
        Right {
            expr: Expr,
            size: Expr,
        },
        Asin(value: Expr),
        Acos(value: Expr),
        Atan(value: Expr),
        Lpad {
            expr: Expr,
            size: Expr,
            fill: Option<Expr>,
        },
        Rpad {
            expr: Expr,
            size: Expr,
            fill: Option<Expr>,
        },
        Replace {
            expr: Expr,
            old: Expr,
            new: Expr,
        },
        Cast {
            expr: Expr,
            data_type: DataType,
        },
        Ceil(value: Expr),
        Coalesce(value: Vec<Expr>),
        Concat(value: Vec<Expr>),
        ConcatWs {
            separator: Expr,
            exprs: Vec<Expr>,
        },
        Custom {
            name: String,
            exprs: Vec<Expr>,
        },
        IfNull {
            expr: Expr,
            then: Expr,
        },
        NullIf {
            expr1: Expr,
            expr2: Expr,
        },
        Rand(value: Option<Expr>),
        Round(value: Expr),
        Trunc(value: Expr),
        Floor(value: Expr),
        Trim {
            expr: Expr,
            filter_chars: Option<Expr>,
            trim_where_field: Option<TrimWhereField>,
        },
        Exp(value: Expr),
        Extract {
            field: DateTimeField,
            expr: Expr,
        },
        Ln(value: Expr),
        Log {
            antilog: Expr,
            base: Expr,
        },
        Log2(value: Expr),
        Log10(value: Expr),
        Div {
            dividend: Expr,
            divisor: Expr,
        },
        Mod {
            dividend: Expr,
            divisor: Expr,
        },
        Gcd {
            left: Expr,
            right: Expr,
        },
        Lcm {
            left: Expr,
            right: Expr,
        },
        Sin(value: Expr),
        Cos(value: Expr),
        Tan(value: Expr),
        Sqrt(value: Expr),
        Power {
            expr: Expr,
            power: Expr,
        },
        Radians(value: Expr),
        Degrees(value: Expr),
        Now(),
        CurrentDate(),
        CurrentTime(),
        CurrentTimestamp(),
        Pi(),
        LastDay(value: Expr),
        Ltrim {
            expr: Expr,
            chars: Option<Expr>,
        },
        Rtrim {
            expr: Expr,
            chars: Option<Expr>,
        },
        Reverse(value: Expr),
        Repeat {
            expr: Expr,
            num: Expr,
        },
        Sign(value: Expr),
        Substr {
            expr: Expr,
            start: Expr,
            count: Option<Expr>,
        },
        Unwrap {
            expr: Expr,
            selector: Expr,
        },
        GenerateUuid(),
        Greatest(value: Vec<Expr>),
        Format {
            expr: Expr,
            format: Expr,
        },
        ToDate {
            expr: Expr,
            format: Expr,
        },
        ToTimestamp {
            expr: Expr,
            format: Expr,
        },
        ToTime {
            expr: Expr,
            format: Expr,
        },
        Position {
            from_expr: Expr,
            sub_expr: Expr,
        },
        FindIdx {
            from_expr: Expr,
            sub_expr: Expr,
            start: Option<Expr>,
        },
        Ascii(value: Expr),
        Chr(value: Expr),
        Md5(value: Expr),
        Hex(value: Expr),
        Append {
            expr: Expr,
            value: Expr,
        },
        Sort {
            expr: Expr,
            order: Option<Expr>,
        },
        Slice {
            expr: Expr,
            start: Expr,
            length: Expr,
        },
        Prepend {
            expr: Expr,
            value: Expr,
        },
        Skip {
            expr: Expr,
            size: Expr,
        },
        Take {
            expr: Expr,
            size: Expr,
        },
        GetX(value: Expr),
        GetY(value: Expr),
        Point {
            x: Expr,
            y: Expr,
        },
        CalcDistance {
            geometry1: Expr,
            geometry2: Expr,
        },
        IsEmpty(value: Expr),
        Length(value: Expr),
        Entries(value: Expr),
        Keys(value: Expr),
        Values(value: Expr),
        Splice {
            list_data: Expr,
            begin_index: Expr,
            end_index: Expr,
            values: Option<Expr>,
        },
        Dedup(value: Expr),
    }
}

struct_layout! {
    aggregate_layout: Aggregate {
        func: AggregateFunction,
        distinct: bool,
    }
}

enum_layout! {
    aggregate_function_layout: AggregateFunction {
        Count(value: CountArgExpr),
        Sum(value: Expr),
        Max(value: Expr),
        Min(value: Expr),
        Avg(value: Expr),
        Variance(value: Expr),
        Stdev(value: Expr),
    }
}

enum_layout! {
    count_arg_expr_layout: CountArgExpr {
        Expr(value: Expr),
        Wildcard,
    }
}

enum_layout! {
    literal_layout: Literal {
        Number(value: BigDecimal),
        QuotedString(value: String),
    }
}

enum_layout! {
    data_type_layout: DataType {
        Boolean,
        Int8,
        Int16,
        Int32,
        Int,
        Int128,
        Uint8,
        Uint16,
        Uint32,
        Uint64,
        Uint128,
        Float32,
        Float,
        Text,
        Bytea,
        Inet,
        Date,
        Timestamp,
        Time,
        Interval,
        Uuid,
        Map,
        List,
        Decimal,
        Point,
    }
}

enum_layout! {
    unary_operator_layout: UnaryOperator {
        Plus,
        Minus,
        Not,
        Factorial,
        BitwiseNot,
    }
}

enum_layout! {
    binary_operator_layout: BinaryOperator {
        Plus,
        Minus,
        Multiply,
        Divide,
        Modulo,
        StringConcat,
        Gt,
        Lt,
        GtEq,
        LtEq,
        Eq,
        NotEq,
        And,
        Or,
        Xor,
        BitwiseAnd,
        BitwiseShiftLeft,
        BitwiseShiftRight,
        Arrow,
        LongArrow,
    }
}

enum_layout! {
    date_time_field_layout: DateTimeField {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second,
    }
}

enum_layout! {
    trim_where_field_layout: TrimWhereField {
        Both,
        Leading,
        Trailing,
    }
}

// Queries stored inside expressions

struct_layout! {
    query_layout: Query {
        body: SetExpr,
        order_by: Vec<OrderByExpr>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    }
}

enum_layout! {
    set_expr_layout: SetExpr {
        Select(value: Box<Select>),
        Values(value: Values),
    }
}

enum_layout! {
    projection_layout: Projection {
        SelectItems(value: Vec<SelectItem>),
    }
}

struct_layout! {
    select_layout: Select {
        distinct: bool,
        projection: Projection,
        from: TableWithJoins,
        selection: Option<Expr>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    }
}

enum_layout! {
    select_item_layout: SelectItem {
        Expr { expr: Expr, label: String },
        QualifiedWildcard(value: String),
        Wildcard,
    }
}

struct_layout! {
    table_with_joins_layout: TableWithJoins {
        relation: TableFactor,
        joins: Vec<Join>,
    }
}

enum_layout! {
    table_factor_layout: TableFactor {
        Table {
            name: String,
            alias: Option<TableAlias>,
        },
        Derived {
            subquery: Query,
            alias: TableAlias,
        },
        Series {
            alias: TableAlias,
            size: Expr,
        },
        Dictionary {
            dict: Dictionary,
            alias: TableAlias,
        },
    }
}

enum_layout! {
    dictionary_layout: Dictionary {
        GlueTables,
        GlueTableColumns,
        GlueIndexes,
        GlueObjects,
    }
}

struct_layout! {
    table_alias_layout: TableAlias {
        name: String,
        columns: Vec<String>,
    }
}

struct_layout! {
    join_layout: Join {
        relation: TableFactor,
        join_operator: JoinOperator,
    }
}

enum_layout! {
    join_operator_layout: JoinOperator {
        Inner(value: JoinConstraint),
        LeftOuter(value: JoinConstraint),
    }
}

enum_layout! {
    join_constraint_layout: JoinConstraint {
        On(value: Expr),
        None,
    }
}

struct_layout! {
    order_by_expr_layout: OrderByExpr {
        expr: Expr,
        asc: Option<bool>,
    }
}

struct_layout! {
    values_layout: Values(rows: Vec<Vec<Expr>>)
}

// Row keys and values

enum_layout! {
    key_layout: Key {
        I8(value: i8),
        I16(value: i16),
        I32(value: i32),
        I64(value: i64),
        I128(value: i128),
        U8(value: u8),
        U16(value: u16),
        U32(value: u32),
        U64(value: u64),
        U128(value: u128),
        F32(value: OrderedFloat<f32>),
        F64(value: OrderedFloat<f64>),
        Decimal(value: Decimal),
        Bool(value: bool),
        Str(value: String),
        Bytea(value: Vec<u8>),
        Date(value: NaiveDate),
        Timestamp(value: NaiveDateTime),
        Time(value: NaiveTime),
        Interval(value: Interval),
        Uuid(value: u128),
        Inet(value: IpAddr),
        None,
    }
}

enum_layout! {
    value_layout: Value {
        Bool(value: bool),
        I8(value: i8),
        I16(value: i16),
        I32(value: i32),
        I64(value: i64),
        I128(value: i128),
        U8(value: u8),
        U16(value: u16),
        U32(value: u32),
        U64(value: u64),
        U128(value: u128),
        F32(value: f32),
        F64(value: f64),
        Decimal(value: Decimal),
        Str(value: String),
        Bytea(value: Vec<u8>),
        Inet(value: IpAddr),
        Date(value: NaiveDate),
        Timestamp(value: NaiveDateTime),
        Time(value: NaiveTime),
        Interval(value: Interval),
        Uuid(value: u128),
        Map(value: BTreeMap<String, Value>),
        List(value: Vec<Value>),
        Point(value: Point),
        Null,
    }
}

enum_layout! {
    interval_layout: Interval {
        Month(value: i32),
        Microsecond(value: i64),
    }
}

struct_layout! {
    point_layout: Point {
        x: f64,
        y: f64,
    }
}
