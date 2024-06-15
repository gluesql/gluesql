import 'package:dart/src/rust/api/payload.dart';
import 'package:dart/src/rust/api/simple.dart';
import 'package:dart/src/rust/api/value.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:dart/src/rust/frb_generated.dart';
import 'package:integration_test/integration_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  test("select", () async {
    final result = await execute(sql: "SELECT 1;");
    expect(
      result.first,
      const DartPayload.select(
        labels: [
          "1",
        ],
        rows: [
          [DartValue.i64(1)],
        ],
      ),
    );
  });
}
