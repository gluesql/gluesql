// use bson::{doc, Bson};
// use mongodb::{options::CollectionOptions, Client, Collection};

// #[tokio::test]
// async fn create_collection_with_validator() {
//     let client = Client::with_uri_str("mongodb://localhost:27017/")
//         .await
//         .unwrap();
//     let db = client.database("mydb");

//     // Define the validation schema
//     let validator = doc! {
//         "$jsonSchema": {
//             "bsonType": "object",
//             "required": ["js_code_field"],
//             "properties": {
//                 "js_code_field": {
//                     "bsonType": "javascript",
//                     "description": "must be a JavaScript code type"
//                 }
//             }
//         }
//     };

//     // Create the collection with the validation schema
//     let options = CollectionOptions::builder().validator(validator).build();
//     let coll: Collection<Bson> = db.create_collection("mycollection", options).await.unwrap();

//     // Insert a document with a JavaScriptCode field
//     let js_code = Bson::JavaScriptCode("function() { return 'Hello, world!'; }".to_string());
//     let doc = doc! { "js_code_field": js_code };
//     coll.insert_one(doc, None).await.unwrap();
// }
