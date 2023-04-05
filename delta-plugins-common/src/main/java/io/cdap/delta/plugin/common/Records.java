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

package io.cdap.delta.plugin.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.SourceColumn;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utilities for converting Records and Schemas.
 */
public class Records {
  private static final Logger LOG = LoggerFactory.getLogger(Records.class);

  private static final String PRECISION_NAME = "connect.decimal.precision";
  private static final String SCALE_NAME = "scale";

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
      SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
      if (column.isOptional()) {
        schemaBuilder.optional();
      }
      fields.add(Schema.Field.of(column.name(), convert(schemaBuilder.build())));
    }
    return Schema.recordOf(table.id().table(), fields);
  }

  /**
   * Return the schema of a table.
   *
   * @param table
   * @param converters
   * @param columns
   * @return
   */
  public static Schema getSchema(Table table, JdbcValueConverters converters, Set<SourceColumn> columns) {
    // If columns set is empty, it means user wants to have all the columns by default.
    if (columns.isEmpty()) {
      return getSchema(table, converters);
    }

    List<Schema.Field> fields = new ArrayList<>(columns.size());
    for (SourceColumn sourceColumn : columns) {
      String columnName = sourceColumn.getName();
      Column column = table.columnWithName(columnName);
      SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
      if (column.isOptional()) {
        schemaBuilder.optional();
      }
      fields.add(Schema.Field.of(columnName,
                                 convert(schemaBuilder.build())));
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
   * @param struct
   * @return
   */
  public static StructuredRecord convert(Struct struct, SchemaMappingCache schemaMappingCache) {
    org.apache.kafka.connect.data.Schema schema = struct.schema();
    Schema mappedSchema = schemaMappingCache.get(schema);
    if (mappedSchema == null) {
      LOG.info("Creating CDAP schema from source schema");
      mappedSchema = convert(struct.schema());
      mappedSchema = mappedSchema.isNullable() ? mappedSchema.getNonNullable() : mappedSchema;
      schemaMappingCache.put(schema, mappedSchema);
    }
    return getStructuredRecord(struct, mappedSchema, schemaMappingCache);
  }

  private static StructuredRecord getStructuredRecord(Struct struct, Schema schema,
                                                      SchemaMappingCache schemaMappingCache) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getFields() == null) {
      return builder.build();
    }

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Field debeziumField = struct.schema().field(fieldName);
      String debeziumSchemaName = debeziumField.schema().name();
      Object val = convert(debeziumField.schema(), struct.get(fieldName), schemaMappingCache);
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType == null || val == null) {
        builder.set(fieldName, val);
      } else {
        switch (logicalType) {
          case DATE:
            builder.setDate(fieldName, LocalDate.ofEpochDay((int) val));
            break;
          case DATETIME:
            long value = (long) val;
            try {
              LocalDateTime localDateTime;
              if (NanoTimestamp.SCHEMA_NAME.equals(debeziumSchemaName)) {
                // DATETIME2(7) from SQL Server is represented as io.debezium.time.NanoTimestamp
                // which is the number of nanoseconds past the epoch, and does not include timezone information.
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, value), ZoneOffset.UTC);
              } else if (MicroTimestamp.SCHEMA_NAME.equals(debeziumSchemaName)) {
                // DATETIME2(4), DATETIME2(5), DATETIME2(6) from SQL Server and mysql are represented as
                // io.debezium.time.MicroTimestamp, which is the number of microseconds past the epoch, and does
                // not include timezone information.
                localDateTime
                  = LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, TimeUnit.MICROSECONDS.toNanos(value)),
                                            ZoneOffset.UTC);
              } else {
                // DATETIME, SMALLDATETIME, DATETIME2(0), DATETIME2(1), DATETIME2(2), DATETIME2(3) from SQL Server
                // and mysql are represented as io.debezium.time.Timestamp, which is the number of milliseconds
                // past the epoch, and does not include timezone information
                localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
              }
              builder.setDateTime(fieldName, localDateTime);
            } catch (DateTimeParseException exception) {
              throw new UnexpectedFormatException(
                String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
                              field.getName(), debeziumSchemaName, val), exception);
            }
            break;
          case TIMESTAMP_MILLIS:
            builder.setTimestamp(fieldName, getZonedDateTime((long) val, TimeUnit.MILLISECONDS));
            break;
          case TIMESTAMP_MICROS:
            if (val instanceof  Long) {
              builder.setTimestamp(fieldName, getZonedDateTime((long) val, TimeUnit.MICROSECONDS));
              break;
            }
            if (val instanceof String) {
              builder.setTimestamp(fieldName, ZonedDateTime.parse((String) val));
              break;
            }
            break;
          case TIME_MILLIS:
            builder.setTime(fieldName, LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos((int) val)));
            break;
          case TIME_MICROS:
            LocalTime localTime;
            if (NanoTime.SCHEMA_NAME.equals(debeziumSchemaName)) {
              // TIME(7) from SQL server represents the number of nanoseconds past midnight, and does not include
              // timezone information.
              localTime = LocalTime.ofNanoOfDay((long) val);
            } else {
              // TIME(4), TIME(5), TIME(6) which represents the number of microseconds past midnight,
              // and does not include timezone information.
              localTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((long) val));
            }
            builder.setTime(fieldName, localTime);
            break;
          case DECIMAL:
            builder.setDecimal(fieldName, (BigDecimal) val);
            break;
          default:
            builder.set(fieldName, val);
            break;
        }
      }
    }

    return builder.build();
  }

  private static ZonedDateTime getZonedDateTime(long ts, TimeUnit unit) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
   return getZonedDateTime(tsInSeconds, fraction, unit);
  }

  private static ZonedDateTime getZonedDateTime(long epochSecond, long fraction, TimeUnit fractionUnit) {
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(epochSecond, fractionUnit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
  }


  private static Object convert(org.apache.kafka.connect.data.Schema schema, Object val,
                                SchemaMappingCache schemaMappingCache) {
    if (val == null) {
      return null;
    }
    switch (schema.type()) {
      case BOOLEAN:
      case BYTES:
      case STRING:
      case INT8:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        return val;
      case INT16:
        return ((Short) val).intValue();
      case ARRAY:
        return ((List<?>) val).stream()
          .map(o -> convert(schema.valueSchema(), o, schemaMappingCache))
          .collect(Collectors.toList());
      case MAP:
        return ((Map<?, ?>) val).entrySet().stream()
          .collect(Collectors.toMap(
            mapKey -> convert(schema.keySchema(), mapKey, schemaMappingCache),
            mapVal -> convert(schema.valueSchema(), mapVal, schemaMappingCache)));
      case STRUCT:
        return convert((Struct) val, schemaMappingCache);
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
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
          int precision = Integer.parseInt(schema.parameters().get(PRECISION_NAME));
          int scale = Integer.parseInt(schema.parameters().get(SCALE_NAME));
          converted = Schema.decimalOf(precision, scale);
        } else {
          converted = Schema.of(Schema.Type.BYTES);
        }
        break;
      case STRING:
        if (ZonedTimestamp.SCHEMA_NAME.equals(schema.name())) {
          converted = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        } else {
          converted = Schema.of(Schema.Type.STRING);
        }
        break;
      case INT8:
      case INT16:
      case INT32:
        if (Date.SCHEMA_NAME.equals(schema.name())) {
          converted = Schema.of(Schema.LogicalType.DATE);
        } else if (Time.SCHEMA_NAME.equals(schema.name())) {
          converted = Schema.of(Schema.LogicalType.TIME_MILLIS);
        } else {
          converted = Schema.of(Schema.Type.INT);
        }
        break;
      case INT64:
        if (MicroTime.SCHEMA_NAME.equals(schema.name()) ||
          NanoTime.SCHEMA_NAME.equals(schema.name())) {
          converted = Schema.of(Schema.LogicalType.TIME_MICROS);
        } else if (MicroTimestamp.SCHEMA_NAME.equals(schema.name()) ||
          NanoTimestamp.SCHEMA_NAME.equals(schema.name()) ||
          Timestamp.SCHEMA_NAME.equals(schema.name())) {
          converted = Schema.of(Schema.LogicalType.DATETIME);
        } else {
          converted = Schema.of(Schema.Type.LONG);
        }
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

        converted = Schema.recordOf(schema.name() == null ? "dummy.schema.name" : schema.name(), cdapFields);
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
