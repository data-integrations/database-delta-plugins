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
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.plugin.common.Records;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.embedded.StopConnectorException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

  private final DeltaSourceContext context;
  private final EventEmitter emitter;
  // we need this since there is no way to get the db information from the source record
  private final String databaseName;
  private final SqlServerOffset sqlServerOffset;
  private final Map<String, SourceTable> sourceTableMap;

  SqlServerRecordConsumer(DeltaSourceContext context, EventEmitter emitter, String databaseName,
                          SqlServerOffset sqlServerOffset, Map<String, SourceTable> sourceTableMap) {
    this.context = context;
    this.emitter = emitter;
    this.databaseName = databaseName;
    this.sqlServerOffset = sqlServerOffset;
    this.sourceTableMap = sourceTableMap;
  }

  @Override
  public void accept(SourceRecord sourceRecord) {
    try {
      context.setOK();
    } catch (IOException e) {
      LOG.warn("Unable to set source state to OK.", e);
    }
    if (sourceRecord.value() == null) {
      return;
    }

    Map<String, String> deltaOffset = sqlServerOffset.generateCdapOffsets(sourceRecord);
    Set<String> snapshotTableSet = sqlServerOffset.getSnapshotTables();
    Offset recordOffset = new Offset(deltaOffset);

    StructuredRecord val = Records.convert((Struct) sourceRecord.value());
    boolean isSnapshot = Boolean.TRUE.equals(sourceRecord.sourceOffset().get(SourceInfo.SNAPSHOT_KEY));

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

    String topicName = sourceRecord.topic();
    // the topic name will always be like this: [db.server.name].[schema].[table]
    if (topicName == null) {
      return; // safety check to avoid NPE
    }
    String[] splits = topicName.split("\\.");
    String schemaName = splits[1];
    String tableName = splits[2];
    String sourceTableId = schemaName + "." + tableName;
    // If the map is empty, we should read all DDL/DML events and columns of all tables
    boolean readAllTables = sourceTableMap.isEmpty();
    SourceTable sourceTable = sourceTableMap.get(sourceTableId);
    if (!readAllTables && sourceTable == null) {
      // shouldn't happen
      return;
    }

    StructuredRecord before = val.get("before");
    StructuredRecord after = val.get("after");
    if (!readAllTables) {
      if (before != null) {
        before = Records.keepSelectedColumns(before, sourceTable.getColumns());
      }
      if (after != null) {
        after = Records.keepSelectedColumns(after, sourceTable.getColumns());
      }
    }
    StructuredRecord value = op == DMLOperation.DELETE ? before : after;

    if (value == null) {
      // this is a safety check to prevent npe warning, it should not be null
      LOG.warn("There is no value in the source record from table {} in database {}", tableName, databaseName);
      return;
    }

    DDLEvent.Builder builder = DDLEvent.builder()
                                 .setDatabase(databaseName)
                                 .setSnapshot(isSnapshot);

    Schema schema = value.getSchema();
    // send the ddl events only if we see the table at the first time in sqlserver offset
    // Note: the delta app itself have prevented adding CREATE_TABLE operation into DDL blacklist for all the tables.
    if (!snapshotTableSet.contains(sourceTableId)) {
      StructuredRecord key = Records.convert((Struct) sourceRecord.key());
      List<Schema.Field> fields = key.getSchema().getFields();
      List<String> primaryFields = new ArrayList<>();
      if (fields != null && !fields.isEmpty()) {
        primaryFields = fields.stream().map(Schema.Field::getName).collect(Collectors.toList());
      }
      snapshotTableSet.add(sourceTableId);
      deltaOffset.put(SqlServerOffset.SNAPSHOT_TABLES, String.join(SqlServerOffset.DELIMITER, snapshotTableSet));
      recordOffset = new Offset(deltaOffset);
      builder.setOffset(recordOffset);

      try {
        // try to always drop the table before snapshot the schema.
        emitter.emit(builder.setOperation(DDLOperation.DROP_TABLE)
                       .setTable(tableName)
                       .build());

        // try to emit create database event before create table event
        emitter.emit(builder.setOperation(DDLOperation.CREATE_DATABASE).build());

        emitter.emit(builder.setOperation(DDLOperation.CREATE_TABLE)
                       .setTable(tableName)
                       .setSchema(schema)
                       .setPrimaryKey(primaryFields)
                       .build());
      } catch (InterruptedException e) {
        // happens when the event reader is stopped. throwing this exception tells Debezium to stop right away
        throw new StopConnectorException("Interrupted while emitting an event.");
      }
      sqlServerOffset.setSnapshotTables(snapshotTableSet);
    }

    if (!readAllTables && sourceTable.getDmlBlacklist().contains(op)) {
      // do nothing due to it was not set to read all tables and the DML op has been blacklisted for this table
      return;
    }

    Long ingestTime = val.get("ts_ms");
    DMLEvent.Builder dmlBuilder = DMLEvent.builder()
      .setOffset(recordOffset)
      .setOperation(op)
      .setDatabase(databaseName)
      .setTable(tableName)
      .setRow(value)
      .setSnapshot(isSnapshot)
      .setTransactionId(null)
      .setIngestTimestamp(ingestTime == null ? 0L : ingestTime);

    // It is required for the source to provide the previous row if the operation is 'UPDATE'
    if (op == DMLOperation.UPDATE) {
      dmlBuilder.setPreviousRow(before);
    }

    try {
      emitter.emit(dmlBuilder.build());
    } catch (InterruptedException e) {
      // happens when the event reader is stopped. throwing this exception tells Debezium to stop right away
      throw new StopConnectorException("Interrupted while emitting an event.");
    }
  }
}
