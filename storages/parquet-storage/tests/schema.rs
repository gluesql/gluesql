use test_suite::data_type::bytea;

use {
    gluesql_core::{
        data::{value::HashMapJsonExt, Interval},
        prelude::{
            Glue,
            Value::{self, *},
        },
    },
    gluesql_parquet_storage::ParquetStorage,
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
    },
    test_suite::{concat_with, row, select, stringify_label, test},
    uuid::Uuid as UUID,
};

#[test]
async fn parquet_schema() {
    let path = "./tests/samples/";
    let parquet_storage = ParquetStorage::new(path).unwrap();
    let mut glue = Glue::new(parquet_storage);

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }

    let parse_uuid = |v| UUID::parse_str(v).unwrap().as_u128();
    //    let bytea = |v| hex::decode(v).unwrap();
    let ip = |a, b, c, d| IpAddr::V4(Ipv4Addr::new(a, b, c, d));
    let m = |s: &str| HashMap::parse_json_object(s).unwrap();
    let l = |values: [&str; 3]| {
        values
            .iter()
            .map(|str| Value::Str(str.to_string()))
            .collect::<Vec<_>>()
    };

    let cases = vec![(
        glue.execute("SELECT boolean, int8, int16, int32, int64, uint8 FROM Schema")
            .await,
        Ok(select!(
          boolean | int8 | int16 | int32      | int64               | uint8
          Bool    | I8   | I16   | I32        | I64                 | U8;
          false     44     12500   1398486491   6843542416722343000   179;
          false     120    25269   40556486     2332015357713582000   92;
          true      49     4821    2007327410   487898043248887500    92;
          false     80     7235    1604644769   2854360423787302000   147;
          false     26     32740   766408542    5159015894945299000   174;
          true      94     32547   1645422225   4690433930230266000   21;
          false     19     32632   1636850638   3150782249474742300   183;
          false     24     26963   642584730    205655629028309660    241;
          false     19     13123   1225214579   7027423886567483000   53;
          false     21     17753   526033318    4966342914812151000   88
        )),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
