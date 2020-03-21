use crate::Row;
use nom_sql::{Column, FieldDefinitionExpression};

pub struct Blend<'a> {
    fields: &'a Vec<FieldDefinitionExpression>,
}

impl<'a> Blend<'a> {
    pub fn new(fields: &'a Vec<FieldDefinitionExpression>) -> Self {
        Self { fields }
    }

    pub fn apply(&self, columns: &Vec<Column>, row: Row) -> Row {
        let Row(items) = row;
        let items = items
            .into_iter()
            .enumerate()
            .filter(|(i, _)| self.check(&columns, *i))
            .map(|(_, item)| item)
            .collect();

        Row(items)
    }

    pub fn check(&self, columns: &Vec<Column>, index: usize) -> bool {
        self.fields.iter().any(|expr| match expr {
            FieldDefinitionExpression::All => true,
            FieldDefinitionExpression::AllInTable(_) => unimplemented!(),
            FieldDefinitionExpression::Col(column) => column.name == columns[index].name,
            FieldDefinitionExpression::Value(_) => unimplemented!(),
        })
    }
}
