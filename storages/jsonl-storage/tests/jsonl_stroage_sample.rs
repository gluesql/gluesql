use {
    gluesql_core::{
        data::{Interval, SchemaParseError, ValueError},
        prelude::{
            Glue, {Payload, Value},
        },
        result::Error,
    },
    gluesql_jsonl_storage::JsonlStorage,
    std::net::{IpAddr, Ipv4Addr},
    test_suite::test,
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
    let m = |s: &str| Value::parse_json_map(s).unwrap();
    let l = |s: &str| Value::parse_json_list(s).unwrap();

    let actual = glue.execute("SELECT * FROM Schema").unwrap();
    let actual = actual.get(0).unwrap();
    let expected = Payload::Select {
        labels: [
            "boolean",
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "text",
            "bytea",
            "inet",
            "date",
            "timestamp",
            "time",
            "interval",
            "uuid",
            "map",
            "list",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect(),
        rows: vec![
            vec![
                Value::Bool(true),
                Value::I8(7),
                Value::I16(18925),
                Value::I32(1201341259),
                Value::I64(7494898932984155000),
                Value::U8(120),
                Value::Str("Adrian Prosacco".to_owned()),
                Value::Bytea(bytea("5AcAbaFb96")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(172, 35, 132, 166))),
                Value::Date(date!("2022-10-02")),
                Value::Timestamp(date!("2022-10-09T23:01:10")),
                Value::Time(date!("01:41:00")),
                Value::Interval(Interval::days(12)),
                Value::Uuid(parse_uuid("0edd778d-459d-4522-b768-0e77ef99d36b")),
                m(r#"{"age": 49, "city": "Roytown"}"#),
                l(r#"["lavender", "pink", "cyan"]"#),
            ],
            vec![
                Value::Bool(false),
                Value::I8(-51),
                Value::I16(16490),
                Value::I32(338389841),
                Value::I64(5331201670632671000),
                Value::U8(2),
                Value::Str("Jasmine Walker".to_owned()),
                Value::Bytea(bytea("eF9B1583EB")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(166, 210, 97, 153))),
                Value::Date(date!("2022-10-02")),
                Value::Timestamp(date!("2022-09-21T06:45:54")),
                Value::Time(date!("10:43:36")),
                Value::Interval(Interval::minutes(84 * 60 + 30)),
                Value::Uuid(parse_uuid("84dbeb26-a123-4f74-bbb9-05c6e60d06e1")),
                m(r#"{"age": 29, "city": "Mertzburgh"}"#),
                l(r#"["gold", "white", "ivory"]"#),
            ],
            vec![
                Value::Bool(true),
                Value::I8(103),
                Value::I16(534),
                Value::I32(1862628437),
                Value::I64(599314352463257000),
                Value::U8(193),
                Value::Str("Mrs. Clayton Brekke".to_owned()),
                Value::Bytea(bytea("bCdCbB80Da")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(92, 195, 251, 154))),
                Value::Date(date!("2022-04-20")),
                Value::Timestamp(date!("2023-01-17T00:18:06")),
                Value::Time(date!("03:10:44")),
                Value::Interval(Interval::minutes(12)),
                Value::Uuid(parse_uuid("58e28e32-0bbd-45e1-bf24-1ae521cb4582")),
                m(r#"{"age": 78, "city": "Warner Robins"}"#),
                l(r#"["orchid", "purple", "olive"]"#),
            ],
            vec![
                Value::Bool(true),
                Value::I8(104),
                Value::I16(583),
                Value::I32(1165449472),
                Value::I64(2949641402019056000),
                Value::U8(89),
                Value::Str("Cesar Wilderman".to_owned()),
                Value::Bytea(bytea("566FFCb916")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(187, 92, 35, 164))),
                Value::Date(date!("2023-01-07")),
                Value::Timestamp(date!("2022-10-14T14:52:07")),
                Value::Time(date!("18:33:22")),
                Value::Interval(Interval::hours(35)),
                Value::Uuid(parse_uuid("b4e9015f-c7c1-4038-abe5-b5a95526e97e")),
                m(r#"{"age": 79, "city": "Castro Valley"}"#),
                l(r#"["salmon", "teal", "yellow"]"#),
            ],
            vec![
                Value::Bool(false),
                Value::I8(25),
                Value::I16(17806),
                Value::I32(1762106541),
                Value::I64(111996217845889040),
                Value::U8(196),
                Value::Str("Nathan Cronin".to_owned()),
                Value::Bytea(bytea("7A332bE8c9")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(80, 126, 116, 114))),
                Value::Date(date!("2022-12-06")),
                Value::Timestamp(date!("2022-10-20T17:30:07")),
                Value::Time(date!("19:17:34")),
                Value::Interval(Interval::microseconds(
                    (((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400,
                )),
                Value::Uuid(parse_uuid("17f0c77c-fee6-4dbf-a388-c68cabd94ede")),
                m(r#"{"age": 90, "city": "North Dashawn"}"#),
                l(r#"["pink", "maroon", "violet"]"#),
            ],
            vec![
                Value::Bool(false),
                Value::I8(120),
                Value::I16(14125),
                Value::I32(2086197552),
                Value::I64(1585833071771122200),
                Value::U8(238),
                Value::Str("Maria Runolfsdottir".to_owned()),
                Value::Bytea(bytea("ceD8dAFD85")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(93, 62, 65, 182))),
                Value::Date(date!("2022-05-11")),
                Value::Timestamp(date!("2022-09-01T23:27:32")),
                Value::Time(date!("06:01:27")),
                Value::Interval(Interval::seconds(-(30 * 60 + 11))),
                Value::Uuid(parse_uuid("ba5877f9-2f41-42a1-92d9-cd830131696f")),
                m(r#"{"age": 52, "city": "East Fidel"}"#),
                l(r#"["salmon", "red", "lime"]"#),
            ],
            vec![
                Value::Bool(true),
                Value::I8(-38),
                Value::I16(22679),
                Value::I32(841008626),
                Value::I64(5288019869742816000),
                Value::U8(191),
                Value::Str("Jesus Schmidt I".to_owned()),
                Value::Bytea(bytea("cF510aFA0d")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(174, 95, 35, 116))),
                Value::Date(date!("2022-04-07")),
                Value::Timestamp(date!("2023-01-28T21:33:25")),
                Value::Time(date!("23:34:43")),
                Value::Interval(Interval::hours(35)),
                Value::Uuid(parse_uuid("fcb7c9db-f43c-4970-84d3-4eb79c0b6e36")),
                m(r#"{"age": 10, "city": "Gastonworth"}"#),
                l(r#"["azure", "yellow", "magenta"]"#),
            ],
            vec![
                Value::Bool(true),
                Value::I8(-87),
                Value::I16(20401),
                Value::I32(126133300),
                Value::I64(1862113375602926600),
                Value::U8(129),
                Value::Str("Ms. Lucille Mann Jr.".to_owned()),
                Value::Bytea(bytea("acdBDdA1A5")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(61, 202, 236, 23))),
                Value::Date(date!("2022-12-09")),
                Value::Timestamp(date!("2022-12-03T23:26:08")),
                Value::Time(date!("14:16:09")),
                Value::Interval(Interval::seconds(-(30 * 60 + 11))),
                Value::Uuid(parse_uuid("93e6722e-7537-4ea5-9039-5917c76463b6")),
                m(r#"{"age": 39, "city": "Lake Viola"}"#),
                l(r#"["orange", "lavender", "teal"]"#),
            ],
            vec![
                Value::Bool(true),
                Value::I8(27),
                Value::I16(15926),
                Value::I32(1001101077),
                Value::I64(8343487625208526000),
                Value::U8(169),
                Value::Str("Gertrude Lang".to_owned()),
                Value::Bytea(bytea("5EA2bECF9b")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(63, 164, 84, 111))),
                Value::Date(date!("2023-01-04")),
                Value::Timestamp(date!("2022-04-18T08:12:12")),
                Value::Time(date!("08:09:18")),
                Value::Interval(Interval::minutes(5)),
                Value::Uuid(parse_uuid("311ac726-8ab1-4f08-9e90-6e07b736d227")),
                m(r#"{"age": 83, "city": "Port Thalia"}"#),
                l(r#"["olive", "olive", "cyan"]"#),
            ],
            vec![
                Value::Bool(false),
                Value::I8(27),
                Value::I16(5185),
                Value::I32(863367621),
                Value::I64(2836074866821069300),
                Value::U8(246),
                Value::Str("Lyle Jaskolski".to_owned()),
                Value::Bytea(bytea("FafAe1C0f0")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(162, 30, 187, 209))),
                Value::Date(date!("2023-02-15")),
                Value::Timestamp(date!("2022-07-29T22:22:18")),
                Value::Time(date!("18:52:46")),
                Value::Interval(Interval::minutes(84 * 60 + 30)),
                Value::Uuid(parse_uuid("a4aa6524-e49e-4d71-a2e1-20763c0a3344")),
                m(r#"{"age": 56, "city": "Lemkeboro"}"#),
                l(r#"["maroon", "magenta", "azure"]"#),
            ],
        ],
    };
    assert_eq!(actual, &expected);

    let actual = glue.execute("SELECT * FROM WrongFormat");
    let expected = Err(ValueError::InvalidJsonString("{".to_owned()).into());

    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT * FROM WrongSchema");
    let expected = Err(Error::Schema(SchemaParseError::CannotParseDDL));

    assert_eq!(actual, expected);

    test(
        glue.execute("SELECT * FROM UnsupportedPrimaryKey")
            .map(|mut payloads| payloads.remove(0)),
        Err(Error::StorageMsg("primary key is not supported".to_owned())),
    );
}
