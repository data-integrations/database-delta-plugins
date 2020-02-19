/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.delta.sqlserver;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.common.Records;
import io.debezium.connector.sqlserver.SourceInfo;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Sql server record consumer
 */
public class SqlServerRecordConsumer implements Consumer<SourceRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerRecordConsumer.class);

  private final EventEmitter emitter;
  // we need this since there is no way to get the db information from the source record
  private final String databaseName;
  // this is hack to track the tables getting created or not
  private final Set<SourceTable> trackingTables;

  public SqlServerRecordConsumer(EventEmitter emitter, String databaseName) {
    this.emitter = emitter;
    this.databaseName = databaseName;
    this.trackingTables = new HashSet<>();
  }

  @Override
  public void accept(SourceRecord sourceRecord) {
    if (sourceRecord.value() == null) {
      return;
    }

    Map<String, byte[]> deltaOffset = SqlServerConstantOffsetBackingStore.serializeOffsets(sourceRecord);
    Offset recordOffset = new Offset(deltaOffset);

    StructuredRecord val = Records.convert((Struct) sourceRecord.value());
    boolean isSnapshot = Boolean.TRUE.equals(sourceRecord.sourceOffset().get(SourceInfo.SNAPSHOT_KEY);

    DMLOperation op;
    String opStr = val.get("op");
    if ("c".equals(opStr) || "r".equals(opStr)) {
      op = DMLOperation.INSERT;
    } else if ("u".equals(opStr)) {
      op = DMLOperation.UPDATE;
    } else if ("d".equals(opStr)) {
      op = DMLOperation.DELETE;
    } else {
      LOG.warn("Skipping unknown operation type '{}'", opStr);
      return;
    }

    StructuredRecord key = Records.convert((Struct) sourceRecord.key());
    String recordName = key.getSchema().getRecordName();
    String tableName = recordName == null ? "" : recordName.split("\\.")[2];
    StructuredRecord value = op == DMLOperation.DELETE ? val.get("before") : val.get("after");

    if (value == null) {
      // this is a safety check to prevent npe warning, it should not be null
      LOG.warn("There is no value in the source record from table {} in database {}", tableName, databaseName);
      return;
    }

    // this is a hack to send DDL event if we first see this table, now all the stuff is in memory
    SourceTable table = new SourceTable(databaseName, tableName);
    DDLEvent.Builder builder = DDLEvent.builder()
                                 .setDatabase(databaseName)
                                 .setOffset(recordOffset)
                                 .setSnapshot(isSnapshot);

    Schema schema = value.getSchema();
    // send the ddl event if the first see the table and the it is in snapshot
    if (!trackingTables.contains(table) && isSnapshot) {
      List<Schema.Field> fields = key.getSchema().getFields();
      List<String> primaryFields = new ArrayList<>();
      if (fields != null && !fields.isEmpty()) {
        primaryFields = fields.stream().map(Schema.Field::getName).collect(Collectors.toList());
      }

      emitter.emit(builder.setOperation(DDLOperation.CREATE_TABLE)
                     .setTable(tableName)
                     .setSchema(schema)
                     .setPrimaryKey(primaryFields)
                     .build());
      trackingTables.add(table);
    }
    
    Long ingestTime = val.get("ts_ms");
    // TODO: [CDAP-16295] set up snapshot state for SqlServer Source
    DMLEvent dmlEvent = DMLEvent.builder()
      .setOffset(recordOffset)
      .setOperation(op)
      .setDatabase(databaseName)
      .setTable(tableName)
      .setRow(value)
      .setSnapshot(isSnapshot)
      .setTransactionId(null)
      .setIngestTimestamp(ingestTime == null ? 0L : ingestTime)
      .build();
    emitter.emit(dmlEvent);
  }
}
