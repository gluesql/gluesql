#[cfg(feature = "gluesql_memory_storage")]
mod json_arrow {
    use gluesql::prelude::{Glue, MemoryStorage, Payload, Value};

    pub async fn run() {
        let storage = MemoryStorage::default();
        let mut glue = Glue::new(storage);

        glue.execute(
            r#"
            CREATE TABLE samples (object MAP, array LIST);
            INSERT INTO samples VALUES (
                '{"id":1,"b":2,"name":"Han","price":3.13,"active":true,"nested":{"role":"admin"}}',
                '[1,"two",true,3.13,null]'
            );
            "#,
        )
        .await
        .expect("prepare sample data");

        let payloads = glue
            .execute(
                r#"
                SELECT
                    object->'b' AS object_int,
                    object->'name' AS object_text,
                    object->'price' AS object_float,
                    object->'active' AS object_bool,
                    object->'nested' AS object_map,
                    array->0 AS array_int,
                    array->1 AS array_text,
                    array->2 AS array_bool,
                    array->3 AS array_float,
                    array->4 AS array_null
                FROM samples;
                "#,
            )
            .await
            .expect("run JSON arrow query");

        let payload = payloads.first().expect("expected select payload");

        if let Payload::Select { rows, .. } = payload {
            let row = rows.first().expect("expected a single row to be returned");

            assert_eq!(row[0], Value::I64(2));
            assert_eq!(row[1], Value::Str("Han".to_owned()));
            assert_eq!(row[2], Value::F64(3.13));
            assert_eq!(row[3], Value::Bool(true));
            assert!(matches!(row[4], Value::Map(_)));
            assert_eq!(row[5], Value::I64(1));
            assert_eq!(row[6], Value::Str("two".to_owned()));
            assert_eq!(row[7], Value::Bool(true));
            assert_eq!(row[8], Value::F64(3.13));
            assert!(matches!(row[9], Value::Null));

            println!("JSON arrow samples: {:?}", row);
        } else {
            panic!("expected select payload, got {payload:?}");
        }
    }
}

fn main() {
    #[cfg(feature = "gluesql_memory_storage")]
    futures::executor::block_on(json_arrow::run());
}
