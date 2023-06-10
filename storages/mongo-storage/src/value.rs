use {gluesql_core::data::Value, mongodb::bson::Bson};

pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Bson {
    fn into_value(self) -> Value {
        match self {
            Bson::Null => Value::Null,
            Bson::Double(num) => Value::F64(num),
            Bson::String(string) => Value::Str(string),
            Bson::Array(array) => {
                let values = array.into_iter().map(|bson| bson.into_value()).collect();

                Value::List(values)
            }
            Bson::Document(d) => Value::Map(
                d.into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_value()))
                    .collect(),
            ),
            Bson::Boolean(b) => Value::Bool(b),
            Bson::RegularExpression(regex) => {
                let pattern = regex.pattern.clone();
                let options = regex.options.clone();
                Value::Str(format!("/{}/{}", pattern, options))
            }
            Bson::JavaScriptCode(code) => Value::Str(code),
            Bson::JavaScriptCodeWithScope(code) => {
                // let mut map = Map::new();
                // for (key, bson) in code.scope {
                //     map.insert(key, Value::try_from(bson).unwrap());
                // }
                // Value::Object(map)
                todo!();
            }
            Bson::Int32(i) => Value::I32(i),
            Bson::Int64(i) => Value::I64(i),
            // Bson::Timestamp(ts) => Value::Timestamp(ts.to_string().into()),
            // Bson::Binary(bin) => Value::Binary(bin.bytes().to_vec()),
            Bson::ObjectId(oid) => Value::Str(oid.to_hex()),
            // Bson::DateTime(dt) => Value::Date(dt),
            Bson::Symbol(sym) => Value::Str(sym),
            // Bson::Decimal128(dec) => Value::Decimal(dec),
            Bson::Undefined => Value::Null,
            Bson::MaxKey => Value::Null,
            Bson::MinKey => Value::Null,
            Bson::DbPointer(_) => todo!("Handle DbPointer type"),
            _ => todo!(),
        }
    }
}
