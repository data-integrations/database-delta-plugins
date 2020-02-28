/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.SourceColumn;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities for converting Records and Schemas.
 */
public class Records {

  private Records() {
    // no-op
  }

  /**
   * Return the schema of a table.
   *
   * @param table
   * @param converters
   * @return
   */
  public static Schema getSchema(Table table, JdbcValueConverters converters) {
    List<Schema.Field> fields = new ArrayList<>(table.columns().size());
    for (Column column : table.columns()) {
      fields.add(Schema.Field.of(column.name(), convert(converters.schemaBuilder(column).build())));
    }
    return Schema.recordOf(table.id().table(), fields);
  }

  /**
   * Return a new structured record which will only contain selected columns for the table.
   *
   * @param record
   * @param columns
   */
  public static StructuredRecord keepSelectedColumns(StructuredRecord record, Set<SourceColumn> columns) {
    if (columns == null) {
      // this should not happen
      return null;
    }

    // If columns set is empty, it means user wanna have all the columns by default.
    if (columns.isEmpty()) {
      return record;
    }

    Schema schema = record.getSchema();
    List<Schema.Field> schemaFields = new ArrayList<>(columns.size());
    for (SourceColumn column : columns) {
      schemaFields.add(schema.getField(column.getName()));
    }

    Schema newSchema = Schema.recordOf(schema.getRecordName(), schemaFields);
    StructuredRecord.Builder builder = StructuredRecord.builder(newSchema);
    for (SourceColumn column : columns) {
      String columnName = column.getName();
      builder.set(columnName, record.get(columnName));
    }

    return builder.build();
  }

  /**
   * Convert a Debezium row, represented as a Kafka Connect Struct, into a CDAP StructuredRecord.
   *
   * TODO: investigate logical types
   *
   * @param struct
   * @return
   */
  public static StructuredRecord convert(Struct struct) {
    Schema schema = convert(struct.schema());
    schema = schema.isNullable() ? schema.getNonNullable() : schema;

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getFields() == null) {
      return builder.build();
    }

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Field debeziumField = struct.schema().field(fieldName);
      Object val = convert(debeziumField.schema(), struct.get(fieldName));
      Schema.LogicalType logicalType = field.getSchema().getLogicalType();
      // TODO: This is a special handling for DECIMAL logical type, further logical types like DATE, TIMESTAMP, etc
      // will be supported later on.
      if (Schema.LogicalType.DECIMAL == logicalType) {
        builder.setDecimal(fieldName, (BigDecimal) val);
      } else {
        builder.set(fieldName, val);
      }
    }

    return builder.build();
  }

  private static Object convert(org.apache.kafka.connect.data.Schema schema, Object val) {
    if (val == null) {
      return null;
    }
    switch (schema.type()) {
      case BOOLEAN:
      case BYTES:
      case STRING:
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        return val;
      case ARRAY:
        return ((List<?>) val).stream()
          .map(o -> convert(schema.valueSchema(), o))
          .collect(Collectors.toList());
      case MAP:
        return ((Map<?, ?>) val).entrySet().stream()
          .collect(Collectors.toMap(
            mapKey -> convert(schema.keySchema(), mapKey),
            mapVal -> convert(schema.valueSchema(), mapVal)));
      case STRUCT:
        return convert((Struct) val);
    }
    // should never happen, all values are listed above
    throw new IllegalStateException(String.format("Kafka type '%s' is not supported.", schema.type()));
  }

  /**
   * Converts a schema from Debezium, which uses Kafka Connect schemas, into a CDAP schema.
   *
   * @param schema
   * @return
   */
  public static Schema convert(org.apache.kafka.connect.data.Schema schema) {
    Schema converted;
    switch (schema.type()) {
      case BOOLEAN:
        converted = Schema.of(Schema.Type.BOOLEAN);
        break;
      case BYTES:
        // In debezium, it will convert NUMERIC/DECIMAL JDBC type to 'org.apache.kafka.connect.data.Decimal' by default.
        // In order to distinguish between this Decimal schema with other BYTES type schema. We will check if the schema
        // name is same with 'org.apache.kafka.connect.data.Decimal', if it is, then we will convert debezium Decimal to
        // CDAP Decimal.
        if (schema.name().equals(Decimal.class.getName())) {
          int precision = Integer.parseInt(schema.parameters().get("connect.decimal.precision"));
          int scale = Integer.parseInt(schema.parameters().get("scale"));
          converted = Schema.decimalOf(precision, scale);
        } else {
          converted = Schema.of(Schema.Type.BYTES);
        }
        break;
      case STRING:
        converted = Schema.of(Schema.Type.STRING);
        break;
      case INT8:
      case INT16:
      case INT32:
        converted = Schema.of(Schema.Type.INT);
        break;
      case INT64:
        converted = Schema.of(Schema.Type.LONG);
        break;
      case FLOAT32:
        converted = Schema.of(Schema.Type.FLOAT);
        break;
      case FLOAT64:
        converted = Schema.of(Schema.Type.DOUBLE);
        break;
      case ARRAY:
        converted = Schema.arrayOf(convert(schema.valueSchema()));
        break;
      case MAP:
        converted = Schema.mapOf(convert(schema.keySchema()), convert(schema.valueSchema()));
        break;
      case STRUCT:
        List<Field> fields = schema.fields();
        List<Schema.Field> cdapFields = new ArrayList<>(fields.size());
        for (Field field : fields) {
          cdapFields.add(Schema.Field.of(field.name(), convert(field.schema())));
        }
        converted = Schema.recordOf(schema.name(), cdapFields);
        break;
      default:
        // should never happen, all values are listed above
        throw new IllegalStateException(String.format("Kafka type '%s' is not supported.", schema.type()));
    }
    if (schema.isOptional()) {
      return Schema.nullableOf(converted);
    }
    return converted;
  }
}
