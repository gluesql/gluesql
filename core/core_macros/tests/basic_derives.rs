use {
    gluesql_core_macros::basic_derives,
    std::{collections::hash_map::DefaultHasher, hash::Hash},
};

#[basic_derives]
struct Query {
    body: String,
    limit: Option<String>,
}

fn main() {
    let query = Query {
        body: "body".to_string(),
        limit: Some("limit".to_string()),
    };

    let query_variant = Query {
        body: "body".to_string(),
        limit: Some("limit".to_string()),
    };

    let query_variant_another = Query {
        body: "body".to_string(),
        limit: Some("limit".to_string()),
    };

    let query_invariant = Query {
        body: "head".to_string(),
        limit: Some("limitless".to_string()),
    };

    // Test `Debug`
    assert_eq!(
        format!("{:?}", query),
        r#"Query { body: "body", limit: Some("limit") }"#
    );
    // Test `Clone`
    assert_eq!(
        query.clone(),
        Query {
            body: "body".to_string(),
            limit: Some("limit".to_string()),
        }
    );
    // Test `PartialEq`
    assert!(query == query_variant);
    assert!(query != query_invariant);
    // Test `Eq`
    assert!(query == query);
    assert_eq!(query == query_variant, query_variant == query);
    if query == query_variant && query_variant == query_variant_another {
        assert!(query == query_variant_another);
    }
    // Test `Hash`
    let mut hasher = DefaultHasher::new();
    assert_eq!(query.hash(&mut hasher), query_variant.hash(&mut hasher));
    // Test `serde::Serialize`
    let serialized = serde_json::to_string(&query).unwrap();
    assert_eq!(&serialized, r#"{"body":"body","limit":"limit"}"#);
    // Test `serde::Deserialize`
    let deserialized: Query = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, query);
}
