use {
    gluesql_core::{
        data::{value::HashMapJsonExt, Interval},
        prelude::{
            Glue,
            Value::{self, *},
        },
    },
    gluesql_json_storage::JsonStorage,
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
    },
    test_suite::{concat_with, concat_with_null, row, select, select_with_null, stringify_label},
    uuid::Uuid as UUID,
};

#[tokio::test]
async fn json_schema() {
    let path = "./tests/samples/";
    let json_storage = JsonStorage::new(path).unwrap();
    let mut glue = Glue::new(json_storage);

    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }
    let parse_uuid = |v| UUID::parse_str(v).unwrap().as_u128();
    let bytea = |v| hex::decode(v).unwrap();
    let ip = |a, b, c, d| IpAddr::V4(Ipv4Addr::new(a, b, c, d));
    let m = |s: &str| HashMap::parse_json_object(s).unwrap();
    let l = |s: &str| Value::parse_json_list(s).unwrap();

    let cases = vec![
        (
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
        ),
        (
            glue.execute("SELECT text, bytea, inet FROM Schema").await,
            Ok(select!(
              text                                | bytea               | inet
              Str                                 | Bytea               | Inet;
              "Dr. Delia Christiansen".to_owned()   bytea("57E6aC3aAa")   ip(16, 199, 176, 40);
              "Jody Stracke".to_owned()             bytea("f43249Da4d")   ip(120, 137, 6, 4);
              "Arnold Mraz".to_owned()              bytea("996ed6bC9f")   ip(104, 17, 25, 44);
              "Dr. Lila Pagac Jr.".to_owned()       bytea("D271DeD0B7")   ip(43, 18, 41, 224);
              "Gina Green".to_owned()               bytea("4C5f01dA5E")   ip(210, 26, 180, 136);
              "Wesley Trantow".to_owned()           bytea("2a22a81c3D")   ip(131, 50, 245, 4);
              "Andrew Bogan".to_owned()             bytea("44e285db9e")   ip(35, 223, 208, 13);
              "Whitney Lueilwitz".to_owned()        bytea("3e704141C9")   ip(34, 213, 220, 130);
              "Colin Bergstrom".to_owned()          bytea("19dc6FD2bA")   ip(242, 202, 162, 243);
              "Sylvia Nienow PhD".to_owned()        bytea("b70dC8703A")   ip(153, 83, 159, 41)
            )),
        ),
        (
            glue.execute("SELECT date, timestamp, time FROM Schema")
                .await,
            Ok(select!(
              date                | timestamp                    | time
              Date                | Timestamp                    | Time;
              date!("2022-12-09")   date!("2022-03-25T00:51:20")   date!("04:35:35");
              date!("2022-12-17")   date!("2022-03-31T19:15:31")   date!("12:07:22");
              date!("2022-04-26")   date!("2022-09-12T07:57:02")   date!("00:19:49");
              date!("2022-08-18")   date!("2023-02-18T19:38:19")   date!("13:27:34");
              date!("2023-02-20")   date!("2022-10-16T09:41:12")   date!("14:49:10");
              date!("2022-03-15")   date!("2023-02-12T17:32:52")   date!("01:54:59");
              date!("2022-12-09")   date!("2022-05-05T12:20:55")   date!("13:57:30");
              date!("2022-07-04")   date!("2022-08-08T02:46:25")   date!("16:22:34");
              date!("2022-04-29")   date!("2022-06-01T03:33:01")   date!("23:02:03");
              date!("2022-06-21")   date!("2022-04-11T06:34:02")   date!("22:59:49")
            )),
        ),
        (
            glue.execute("SELECT \"interval\" FROM Schema").await,
            Ok(select!(
              "\"interval\""
              Interval;
              Interval::hours(-86);
              Interval::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400);
              Interval::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400);
              Interval::hours(35);
              Interval::seconds(-(12 * 3600 + 30 * 60 + 12));
              Interval::minutes(84 * 60 + 30);
              Interval::months(14);
              Interval::minutes(84 * 60 + 30);
              Interval::seconds(-(30 * 60 + 11));
              Interval::minutes(84 * 60 + 30)
            )),
        ),
        (
            glue.execute("SELECT uuid FROM Schema").await,
            Ok(select!(
              uuid
              Uuid;
              parse_uuid("7e11a009-4392-4bb8-92a9-884252542bdc");
              parse_uuid("a3b66bfb-bf15-42f5-baf1-15910992e099");
              parse_uuid("84147534-82fd-46c3-bdc2-a70a1c6beaa0");
              parse_uuid("e5350af3-a575-44bb-a78a-8ea8a94a767b");
              parse_uuid("b604018e-7287-4bdc-96cb-4f4b5a2e675d");
              parse_uuid("725b9cfc-6d10-4fcf-b063-948be71e4b40");
              parse_uuid("59e69126-0a57-46b7-8c54-a106b836d436");
              parse_uuid("d99e7f87-7f64-4992-b2dd-25241b03cd30");
              parse_uuid("8070582a-7906-4201-971d-3323b3ef79c6");
              parse_uuid("d4223eea-9514-4366-833d-933a1221d8e3")
            )),
        ),
        (
            glue.execute("SELECT map FROM Schema").await,
            Ok(select!(
              map
              Map;
              m(r#"{"age": 84, "city": "Armstrongfurt"}"#);
              m(r#"{"age": 34, "city": "Fort Randiview"}"#);
              m(r#"{"age": 58, "city": "Cambridge"}"#);
              m(r#"{"age": 77, "city": "Gloverburgh"}"#);
              m(r#"{"age": 45, "city": "Taunton"}"#);
              m(r#"{"age": 41, "city": "West Rhiannaview"}"#);
              m(r#"{"age": 47, "city": "North Chaddtown"}"#);
              m(r#"{"age": 11, "city": "Joeyborough"}"#);
              m(r#"{"age": 86, "city": "Lake Quintenberg"}"#);
              m(r#"{"age": 53, "city": "Jazmyneville"}"#)
            )),
        ),
        (
            glue.execute("SELECT list FROM Schema").await,
            Ok(select_with_null!(
              list;
              l(r#"["olive", "turquoise", "plum"]"#);
              l(r#"["red", "sky blue", "grey"]"#);
              l(r#"["indigo", "turquoise", "indigo"]"#);
              l(r#"["black", "turquoise", "purple"]"#);
              l(r#"["turquoise", "lavender", "red"]"#);
              l(r#"["turquoise", "magenta", "salmon"]"#);
              l(r#"["maroon", "violet", "lavender"]"#);
              l(r#"["pink", "teal", "indigo"]"#);
              l(r#"["black", "red", "purple"]"#);
              l(r#"["pink", "indigo", "plum"]"#)
            )),
        ),
        (
            glue.execute("SELECT * FROM ArrayOfJsonsSchema").await,
            Ok(select!(
              id   | name
              I64  | Str;
              1      "Glue".to_owned();
              2      "SQL".to_owned()
            )),
        ),
        (
            glue.execute("SELECT * FROM SingleJsonSchema").await,
            Ok(select_with_null!(
              data;
              l(r#"[
                     {"id": 1, "name": "Glue"},
                     {"id": 2, "name": "SQL"}
                   ]"#
              )
            )),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
