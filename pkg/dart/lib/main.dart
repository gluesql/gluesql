import 'package:dart/src/rust/api/payload.dart';
import 'package:dart/src/rust/api/value.dart';
import 'package:flutter/material.dart';
import 'package:dart/src/rust/api/simple.dart';
import 'package:dart/src/rust/frb_generated.dart';

Future<void> main() async {
  await RustLib.init();
  runApp(const MyApp());
}

class MyApp extends StatefulWidget {
  const MyApp({super.key});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  final TextEditingController _controller = TextEditingController();
  List<Payload>? data;

  @override
  void initState() {
    super.initState();
    _controller.text =
        "CREATE TABLE User (\n  id INT PRIMARY KEY,\n  name TEXT\n);\nINSERT INTO User VALUES (1, 'Alice');\nINSERT INTO User VALUES (2, 'Bob');\nSELECT * FROM User;\nSELECT COUNT(*) FROM User;";
  }

  void fetchData() async {
    try {
      List<Payload> result = await execute(sql: _controller.text);
      setState(() {
        data = result;
      });
    } catch (e) {
      debugPrint("fetchData: ${e.toString()}");
    }
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('GlueSQL')),
        body: SingleChildScrollView(
          child: Padding(
            padding: const EdgeInsets.all(16.0),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                SizedBox(
                  height: 220,
                  child: TextField(
                    controller: _controller,
                    keyboardType: TextInputType.multiline,
                    maxLines: null,
                    expands: true,
                    decoration: const InputDecoration(
                      border: OutlineInputBorder(),
                      hintText: 'Enter SQL',
                    ),
                    onChanged: (String value) {
                      _controller.text = value;
                    },
                  ),
                ),
                Center(
                  child: ElevatedButton(
                    onPressed: fetchData,
                    child: const Text('Execute'),
                  ),
                ),
                for (Payload payload in data ?? [])
                  switch (payload) {
                    Payload_Select(
                      labels: List<String> labels,
                      rows: List<List<Value>> rows
                    ) =>
                      DataGrid(
                        labels,
                        rows,
                      ),
                    Payload_Create() =>
                      const Center(child: Text("Table created")),
                    Payload_Insert(field0: BigInt count) =>
                      Center(child: Text("$count rows inserted")),
                    _ => const Text("Not implemented")
                  },
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class DataGrid extends StatelessWidget {
  final List<String> labels;
  final List<List<Value>> rows;

  const DataGrid(this.labels, this.rows, {super.key});

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      scrollDirection: Axis.vertical,
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: DataTable(
          columns: [
            for (String label in labels)
              DataColumn(
                label: Text(label),
              ),
          ],
          rows: [
            for (List<Value> row in rows)
              DataRow(
                cells: [
                  for (Value value in row)
                    DataCell(
                      Text(value.toString()),
                    ),
                ],
              ),
          ],
        ),
      ),
    );
  }
}
