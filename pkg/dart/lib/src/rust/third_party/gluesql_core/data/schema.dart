// This file is automatically generated, so please do not edit it.
// Generated by `flutter_rust_bridge`@ 2.0.0-dev.40.

// ignore_for_file: invalid_use_of_internal_member, unused_import, unnecessary_import

import '../../../frb_generated.dart';
import '../../../lib.dart';
import '../ast.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';

// These types are ignored because they are not used by any `pub` functions: `SchemaParseError`

// Rust type: RustOpaqueMoi<flutter_rust_bridge::for_generated::RustAutoOpaqueInner<SchemaIndex>>
abstract class SchemaIndex implements RustOpaqueInterface {
  DateTime get created;

  Expr get expr;

  String get name;

  SchemaIndexOrd get order;

  void set created(DateTime created);

  void set expr(Expr expr);

  void set name(String name);

  void set order(SchemaIndexOrd order);
}

class Schema {
  final String tableName;
  final List<ColumnDef>? columnDefs;
  final List<SchemaIndex> indexes;
  final String? engine;
  final List<ForeignKey> foreignKeys;
  final String? comment;

  const Schema({
    required this.tableName,
    this.columnDefs,
    required this.indexes,
    this.engine,
    required this.foreignKeys,
    this.comment,
  });

  static Future<Schema> fromDdl({required String ddl}) =>
      RustLib.instance.api.gluesqlCoreDataSchemaSchemaFromDdl(ddl: ddl);

  Future<String> toDdl() =>
      RustLib.instance.api.gluesqlCoreDataSchemaSchemaToDdl(
        that: this,
      );

  @override
  int get hashCode =>
      tableName.hashCode ^
      columnDefs.hashCode ^
      indexes.hashCode ^
      engine.hashCode ^
      foreignKeys.hashCode ^
      comment.hashCode;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Schema &&
          runtimeType == other.runtimeType &&
          tableName == other.tableName &&
          columnDefs == other.columnDefs &&
          indexes == other.indexes &&
          engine == other.engine &&
          foreignKeys == other.foreignKeys &&
          comment == other.comment;
}

enum SchemaIndexOrd {
  asc,
  desc,
  both,
  ;
}
