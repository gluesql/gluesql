use gluesql_core::{
    prelude::*,
    data::{Value, FloatVector},
    ast::{DataType, Literal}
};

fn main() {
    // Test if DataType::FloatVector is working correctly
    println!("Testing DataType::FloatVector...");
    
    // Test try_from_literal
    let literal = Literal::Text("\"[1.0, 2.0, 3.0]\"".into());
    let result = Value::try_from_literal(&DataType::FloatVector, &literal);
    println!("try_from_literal result: {:?}", result);
    
    // Test try_cast_from_literal  
    let result2 = Value::try_cast_from_literal(&DataType::FloatVector, &literal);
    println!("try_cast_from_literal result: {:?}", result2);
    
    // Test direct FloatVector creation
    let vec_result = FloatVector::new(vec![1.0, 2.0, 3.0]);
    println!("FloatVector::new result: {:?}", vec_result);
    
    // Test JSON parsing
    let json_result = Value::parse_json_vector("[1.0, 2.0, 3.0]");
    println!("parse_json_vector result: {:?}", json_result);
}
