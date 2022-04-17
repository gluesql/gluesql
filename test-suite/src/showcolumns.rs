use crate::*;

macro_rules! row {
    ( $( $p:path )* ; $( $v:expr )* ) => (
        ($( $p($v) ),*)
    )
}

macro_rules! showcolumns {
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ; $( $( $v2: expr )+ );+) => ({
        let mut rows = vec![
            row!($( $t )+ ; $( $v )+),
        ];

        gluesql_core::executor::Payload::ShowColumns (
          //  labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
           concat_with!(rows ; $( $t )+ ; $( $( $v2 )+ );+)
        )
    });
    ( $( $c: tt )|+ $( ; )? $( $t: path )|+ ; $( $v: expr )+ ) => (
        gluesql_core::executor::Payload::ShowColumns (
           // labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            vec![row!($( $t )+ ; $( $v )+ )],
        )
    );
    ( $( $c: tt )|+ $( ; )?) => (
        gluesql_core::executor::Payload::ShowColumns (
            //labels: vec![$( stringify!($c).to_owned().replace("\"", "")),+],
            vec![],
        )
    );
}

test_case!(showcolumns, async move {
    use gluesql_core::ast::DataType;
    use gluesql_core::{executor::ExecuteError, prelude::Value::*};

    run!(
        "
        CREATE TABLE mytable (
            id8 INT(8),
            id INTEGER,
            rate FLOAT,
            dec  decimal,
            flag BOOLEAN,
            text TEXT,
            DOB  Date,
            Tm   Time,
            ival Interval,
            tstamp Timestamp,
            uid    Uuid,
            hash   Map,
            glist  List,
        );
    "
    );

    test!(
        Ok(showcolumns!(
            Field               | Type
            Str                 | Str;
            "id8".to_owned()      DataType::Int8.to_string();
            "id".to_owned()       DataType::Int.to_string();
            "rate".to_owned()     DataType::Float.to_string();
            "dec".to_owned()      DataType::Decimal.to_string();
            "flag".to_owned()     DataType::Boolean.to_string();
            "text".to_owned()     DataType::Text.to_string();
            "DOB".to_owned()      DataType::Date.to_string();
            "Tm".to_owned()       DataType::Time.to_string();
            "ival".to_owned()     DataType::Interval.to_string();
            "tstamp".to_owned()   DataType::Timestamp.to_string();
            "uid".to_owned()      DataType::Uuid.to_string();
            "hash".to_owned()     DataType::Map.to_string();
            "glist".to_owned()    DataType::List.to_string()
        )),
        r#"Show columns from mytable"#
    );

    test!(
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into()),
        r#"Show columns from mytable1"#
    );
});
