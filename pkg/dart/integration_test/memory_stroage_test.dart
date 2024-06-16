import 'package:dart/src/rust/api/payload.dart';
import 'package:dart/src/rust/api/simple.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:dart/src/rust/frb_generated.dart';
import 'package:integration_test/integration_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  test("create", () async {
    expect(
      (await execute(
        sql: '''
CREATE TABLE AllTypes (
  boolean BOOLEAN,
  int8 INT8,
  int16 INT16,
  int32 INT32,
  int INT,
  int128 INT128,
  uinti8 UINT8,
  text TEXT,
  bytea BYTEA,
  inet INET,
  date DATE,
  timestamp TIMESTAMP,
  time TIME,
  interval INTERVAL,
  uuid UUID,
  map MAP,
  list LIST
);
''',
      ))
          .first,
      const DartPayload.create(),
    );
  });

  test("insert", () async {
    expect(
      (await execute(
        sql: '''
INSERT INTO AllTypes
 VALUES (
 true,
 1,
 2,
 3,
 4,
 5,
 6,
 'a',
 X'123456',
 '::1',
 DATE '2022-11-01',
 TIMESTAMP '2022-11-02',
 TIME '23:59:59',
 INTERVAL '1' DAY,
 '550e8400-e29b-41d4-a716-446655440000',
 '{"a": {"red": "apple", "blue": 1}, "b": 10}',
 '[{ "foo": 100, "bar": [true, 0, [10.5, false] ] }, 10, 20]'
 );,
''',
      ))
          .first,
      DartPayload.insert(BigInt.from(10)),
    );
  });

//   test("select", () async {
//     expect(
//       (await execute(
//         sql: '''
// SELECT * FROM Foo;
// ''',
//       ))
//           .first,
//       const DartPayload.select(
//         labels: [
//           "1",
//         ],
//         rows: [
//           [DartValue.i64(1)],
//         ],
//       ),
//     );
//   });
}
