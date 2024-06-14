import 'package:dart/src/rust/api/payload.dart';
import 'package:flutter/material.dart';
import 'package:dart/src/rust/api/simple.dart';
import 'package:dart/src/rust/frb_generated.dart';

import 'package:flutter/material.dart';
import 'package:dart/src/rust/api/simple.dart';
import 'package:dart/src/rust/frb_generated.dart';

Future<void> main() async {
  await RustLib.init();
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    var sql = "SELECT 'Hello', 42, 'World'";
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('GlueSQL')),
        body: Center(
          // Use FutureBuilder to handle asynchronous operation

          child: Padding(
            padding: const EdgeInsets.all(16.0),
            child: FutureBuilder<List<DartPayload>>(
              future: execute(sql: sql), // Your async function call
              builder: (context, snapshot) {
                if (snapshot.connectionState == ConnectionState.waiting) {
                  return const CircularProgressIndicator(); // Show loading indicator while waiting
                } else if (snapshot.hasError) {
                  return Text('Error: ${snapshot.error}'); // Show error if any
                } else {
                  // Display your data when available
                  return Text(
                      'SQL: $sql\nResult: `${snapshot.data.toString()}`');
                }
              },
            ),
          ),
        ),
      ),
    );
  }
}
