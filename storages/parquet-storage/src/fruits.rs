use parquet::schema::types::{LogicalType, Type};
use parquet::schema::{CompressionCodec, SchemaDescriptor};

fn get_fruits_schema() -> SchemaDescriptor {
    let mut fields = Vec::new();

    let name_field = Type::primitive_type_builder("name", LogicalType::UTF8)
        .with_repetition(Type::Repetition::REQUIRED)
        .build()
        .unwrap();
    fields.push(name_field);

    let stock_field = Type::group_type_builder("stock")
        .with_repetition(Type::Repetition::REQUIRED)
        .with_codec(CompressionCodec::SNAPPY)
        .with_logical_type(LogicalType::LIST(Default::default()))
        .with_fields(vec![Type::group_type_builder("element")
            .with_repetition(Type::Repetition::REQUIRED)
            .with_fields(vec![Type::primitive_type_builder(
                "price",
                LogicalType::DOUBLE,
            )
            .with_repetition(Type::Repetition::REQUIRED)
            .build()
            .unwrap()])
            .build()
            .unwrap()])
        .build()
        .unwrap();
    fields.push(stock_field);

    SchemaDescriptor::new(fields)
}
