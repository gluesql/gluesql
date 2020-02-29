use crate::data::Value;
use nom_sql::{Column, FieldDefinitionExpression};

pub struct Blend<'a> {
    pub fields: &'a Vec<FieldDefinitionExpression>,
}

impl Blend<'_> {
    pub fn check(&self, item: &(Column, Value)) -> bool {
        self.fields.iter().any(|expr| match expr {
            FieldDefinitionExpression::All => true,
            FieldDefinitionExpression::AllInTable(_) => unimplemented!(),
            FieldDefinitionExpression::Col(column) => column.name == item.0.name,
            FieldDefinitionExpression::Value(_) => unimplemented!(),
        })
    }
}
