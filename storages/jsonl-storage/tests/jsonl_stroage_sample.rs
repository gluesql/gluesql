use {
    gluesql_core::{
        data::{value::HashMapJsonExt, Interval, SchemaParseError, ValueError},
        prelude::{
            Glue,
            {
                Payload,
                Value::{self, *},
            },
        },
        result::Error,
    },
    gluesql_jsonl_storage::JsonlStorage,
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
    },
    test_suite::{concat_with, row, select, stringify_label, test},
    uuid::Uuid as UUID,
};

#[test]
fn jsonl_storage_sample() {
    let path = "./tests/samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let actual = glue.execute("SELECT * FROM Schemaless").unwrap();
    let actual = actual.get(0).unwrap();
    let expected = Payload::SelectMap(vec![
        [("id".to_owned(), Value::I64(1))].into_iter().collect(),
        [("name".to_owned(), Value::Str("Glue".to_owned()))]
            .into_iter()
            .collect(),
        [
            ("id".to_owned(), Value::I64(3)),
            ("name".to_owned(), Value::Str("SQL".to_owned())),
        ]
        .into_iter()
        .collect(),
    ]);
    assert_eq!(actual, &expected);

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }
    let parse_uuid = |v| UUID::parse_str(v).unwrap().as_u128();
    let bytea = |v| hex::decode(v).unwrap();
    let ip = |a, b, c, d| IpAddr::V4(Ipv4Addr::new(a, b, c, d));
    let m = |s: &str| HashMap::parse_json_object(s).unwrap();
    let l = |s: &str| {
        let list = Value::parse_json_list(s).unwrap();

        match list {
            List(values) => values,
            _ => panic!("not a list"),
        }
    };

    let cases = vec![
        (
            glue.execute("SELECT * FROM Schema"),
            Ok(select!(
              boolean | int8 | int16 | int32      | int64               | uint8 | text                                | bytea               | inet                  | date                | timestamp                    | time              | interval                                                                   | uuid                                               | map                                             | list
              Bool    | I8   | I16   | I32        | I64                 | U8    | Str                                 | Bytea               | Inet                  | Date                | Timestamp                    | Time              | Interval                                                                   | Uuid                                               | Map                                             | List;
              false     44     12500   1398486491   6843542416722343000   179     "Dr. Delia Christiansen".to_owned()   bytea("57E6aC3aAa")   ip(16, 199, 176, 40)    date!("2022-12-09")   date!("2022-03-25T00:51:20")   date!("04:35:35")   Interval::hours(-86)                                                         parse_uuid("7e11a009-4392-4bb8-92a9-884252542bdc")   m(r#"{"age": 84, "city": "Armstrongfurt"}"#)      l(r#"["olive", "turquoise", "plum"]"#);
              false     120    25269   40556486     2332015357713582000   92      "Jody Stracke".to_owned()             bytea("f43249Da4d")   ip(120, 137, 6, 4)      date!("2022-12-17")   date!("2022-03-31T19:15:31")   date!("12:07:22")   Interval::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400)   parse_uuid("a3b66bfb-bf15-42f5-baf1-15910992e099")   m(r#"{"age": 34, "city": "Fort Randiview"}"#)     l(r#"["red", "sky blue", "grey"]"#);
              true      49     4821    2007327410   487898043248887500    92      "Arnold Mraz".to_owned()              bytea("996ed6bC9f")   ip(104, 17, 25, 44)     date!("2022-04-26")   date!("2022-09-12T07:57:02")   date!("00:19:49")   Interval::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400)   parse_uuid("84147534-82fd-46c3-bdc2-a70a1c6beaa0")   m(r#"{"age": 58, "city": "Cambridge"}"#)          l(r#"["indigo", "turquoise", "indigo"]"#);
              false     80     7235    1604644769   2854360423787302000   147     "Dr. Lila Pagac Jr.".to_owned()       bytea("D271DeD0B7")   ip(43, 18, 41, 224)     date!("2022-08-18")   date!("2023-02-18T19:38:19")   date!("13:27:34")   Interval::hours(35)                                                          parse_uuid("e5350af3-a575-44bb-a78a-8ea8a94a767b")   m(r#"{"age": 77, "city": "Gloverburgh"}"#)        l(r#"["black", "turquoise", "purple"]"#);
              false     26     32740   766408542    5159015894945299000   174     "Gina Green".to_owned()               bytea("4C5f01dA5E")   ip(210, 26, 180, 136)   date!("2023-02-20")   date!("2022-10-16T09:41:12")   date!("14:49:10")   Interval::seconds(-(12 * 3600 + 30 * 60 + 12))                               parse_uuid("b604018e-7287-4bdc-96cb-4f4b5a2e675d")   m(r#"{"age": 45, "city": "Taunton"}"#)            l(r#"["turquoise", "lavender", "red"]"#);
              true      94     32547   1645422225   4690433930230266000   21      "Wesley Trantow".to_owned()           bytea("2a22a81c3D")   ip(131, 50, 245, 4)     date!("2022-03-15")   date!("2023-02-12T17:32:52")   date!("01:54:59")   Interval::minutes(84 * 60 + 30)                                              parse_uuid("725b9cfc-6d10-4fcf-b063-948be71e4b40")   m(r#"{"age": 41, "city": "West Rhiannaview"}"#)   l(r#"["turquoise", "magenta", "salmon"]"#);
              false     19     32632   1636850638   3150782249474742300   183     "Andrew Bogan".to_owned()             bytea("44e285db9e")   ip(35, 223, 208, 13)    date!("2022-12-09")   date!("2022-05-05T12:20:55")   date!("13:57:30")   Interval::months(14)                                                         parse_uuid("59e69126-0a57-46b7-8c54-a106b836d436")   m(r#"{"age": 47, "city": "North Chaddtown"}"#)    l(r#"["maroon", "violet", "lavender"]"#);
              false     24     26963   642584730    205655629028309660    241     "Whitney Lueilwitz".to_owned()        bytea("3e704141C9")   ip(34, 213, 220, 130)   date!("2022-07-04")   date!("2022-08-08T02:46:25")   date!("16:22:34")   Interval::minutes(84 * 60 + 30)                                              parse_uuid("d99e7f87-7f64-4992-b2dd-25241b03cd30")   m(r#"{"age": 11, "city": "Joeyborough"}"#)        l(r#"["pink", "teal", "indigo"]"#);
              false     19     13123   1225214579   7027423886567483000   53      "Colin Bergstrom".to_owned()          bytea("19dc6FD2bA")   ip(242, 202, 162, 243)  date!("2022-04-29")   date!("2022-06-01T03:33:01")   date!("23:02:03")   Interval::seconds(-(30 * 60 + 11))                                           parse_uuid("8070582a-7906-4201-971d-3323b3ef79c6")   m(r#"{"age": 86, "city": "Lake Quintenberg"}"#)   l(r#"["black", "red", "purple"]"#);
              false     21     17753   526033318    4966342914812151000   88      "Sylvia Nienow PhD".to_owned()        bytea("b70dC8703A")   ip(153, 83, 159, 41)    date!("2022-06-21")   date!("2022-04-11T06:34:02")   date!("22:59:49")   Interval::minutes(84 * 60 + 30)                                              parse_uuid("d4223eea-9514-4366-833d-933a1221d8e3")   m(r#"{"age": 53, "city": "Jazmyneville"}"#)       l(r#"["pink", "indigo", "plum"]"#)
            )),
        ),
        (
            glue.execute("SELECT * FROM WrongFormat"),
            Err(ValueError::InvalidJsonString("{".to_owned()).into()),
        ),
        (
            glue.execute("SELECT * FROM WrongSchema"),
            Err(Error::Schema(SchemaParseError::CannotParseDDL)),
        ),
        (
            glue.execute("DELETE FROM SchemaWithPK;"),
            Ok(Payload::Delete(0)),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(2, 'b')"),
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("INSERT INTO SchemaWithPK VALUES(1, 'a')"),
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("SELECT * FROM SchemaWithPK"),
            Ok(select!(
                id | name
                I64 | Str;
                1 "a".to_owned();
                2 "b".to_owned()
            )),
        ),
        (
            glue.execute("DELETE FROM SchemaWithPK;"),
            Ok(Payload::Delete(2)),
        ),
    ];

    for (actual, expected) in cases {
        test(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
