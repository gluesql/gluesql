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
  String sql = '';
  List<DartPayload>? data;

  void fetchData() async {
    try {
      List<DartPayload> result = await execute(sql: sql);
      setState(() {
        data = result;
      });
    } catch (e) {
      print(e);
    }
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('GlueSQL')),
        body: SingleChildScrollView(
          child: Center(
            // Use FutureBuilder to handle asynchronous operation
            child: Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                children: [
                  SizedBox(
                    height: 200,
                    child: TextField(
                      keyboardType: TextInputType.multiline,
                      maxLines: null,
                      expands: true,
                      decoration: const InputDecoration(
                        border: OutlineInputBorder(),
                        hintText: 'Enter SQL',
                      ),
                      onChanged: (String value) {
                        setState(() {
                          sql = value;
                        });
                      },
                    ),
                  ),
                  ElevatedButton(
                    onPressed: fetchData,
                    child: const Text('Execute'),
                  ),
                  for (DartPayload payload in data ?? [])
                    switch (payload) {
                      DartPayload_Select(
                        labels: List<String> labels,
                        rows: List<List<DartValue>> rows
                      ) =>
                        DataGrid(
                          labels,
                          rows,
                        ),
                      _ => const Text("Not implemented")
                    }
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class DataGrid extends StatelessWidget {
  final List<String> labels;
  final List<List<DartValue>> rows;

  const DataGrid(this.labels, this.rows, {super.key});

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      height: 300,
      child: SingleChildScrollView(
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
              for (List<DartValue> row in rows)
                DataRow(
                  cells: [
                    for (DartValue value in row)
                      DataCell(
                        Text(value.toString()),
                      ),
                  ],
                ),
            ],
          ),
        ),
      ),
    );
  }
}
