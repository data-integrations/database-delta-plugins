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

package io.cdap.delta.oracle;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.debezium.connector.oracle.SourceInfo;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Source record consumer for Oracle DB.
 */
public class OracleSourceRecordConsumer implements Consumer<SourceRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceRecordConsumer.class);

  private final String databaseName;
  private final EventEmitter emitter;
  private final Map<String, SourceTable> sourceTableMap;
  // used to track the tables created or not
  private final Set<SourceTable> snapshotTrackingTables;

  public OracleSourceRecordConsumer(String databaseName, EventEmitter emitter,
                                    Map<String, SourceTable> sourceTableMap) {
    this.databaseName = databaseName;
    this.emitter = emitter;
    this.sourceTableMap = sourceTableMap;
    this.snapshotTrackingTables = new HashSet<>();
  }

  @Override
  public void accept(SourceRecord sourceRecord) {
    if (sourceRecord.value() == null) {
      return;
    }

    Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
    Boolean snapshot = (Boolean) sourceOffset.get(SourceInfo.SNAPSHOT_KEY);
    Boolean snapshotCompleted = (Boolean) sourceOffset.get(OracleConstantOffsetBackingStore.SNAPSHOT_COMPLETED);
    boolean isSnapshot = Boolean.TRUE.equals(snapshot);
    boolean isSnapshotCompleted = Boolean.TRUE.equals(snapshotCompleted);

    Map<String, byte[]> deltaOffset = generateCdapOffsets(sourceRecord);
    StructuredRecord val = Records.convert((Struct) sourceRecord.value());
    StructuredRecord source = val.get("source");
    String recordName = val.getSchema().getRecordName();
    // the record name will always be like this: [db.server.name].[schema].[table].Envelope
    if (recordName == null) {
      return; // safety check to avoid NPE
    }
    String[] splits = recordName.split("\\.");
    String schemaName = splits[1];
    String tableName  = splits[2];
    String sourceTableId = schemaName + "." + tableName;
    // If the map is empty, we should read all DDL/DML events and columns of all tables, basically, we should not do
    // any blacklist for tables.
    boolean readAllTables = sourceTableMap.isEmpty();
    SourceTable sourceTable = sourceTableMap.get(sourceTableId);
    if (!readAllTables && sourceTable == null) {
      // shouldn't happen
      return;
    }
    String transactionId = source.get("txId");
    StructuredRecord before = readAllTables ? val.get("before") :
      Records.keepSelectedColumns(val.get("before"), sourceTable.getColumns());
    StructuredRecord after = readAllTables ? val.get("after") :
      Records.keepSelectedColumns(val.get("after"), sourceTable.getColumns());
    Long ingestTime = val.get("ts_ms");
    Offset recordOffset = new Offset(deltaOffset);
    SourceTable trackingTable = new SourceTable(databaseName, tableName);

    DMLOperation op;
    String opStr = val.get("op");
    if (opStr == null) { // it is a safety check to avoid potential NPE
      opStr = "";
    }
    switch (opStr) {
      case "c":
      case "r":
        op = DMLOperation.INSERT;
        break;
      case "u":
        op = DMLOperation.UPDATE;
        break;
      case "d":
        op = DMLOperation.DELETE;
        break;
      default:
        LOG.warn("Skipping unknown operation type '{}'", opStr);
        return;
    }

    // send the ddl event iff all the following conditions have been met:
    // 1. it was set to read all tables or CREATE_TABLE DDL op is not blacklisted for this table
    // 2. it was not in tracking before
    // 3. it was marked as under snapshotting
    if ((readAllTables || !sourceTable.getDdlBlacklist().contains(DDLOperation.CREATE_TABLE)) &&
      !snapshotTrackingTables.contains(trackingTable) && isSnapshot) {
      LOG.info("Snapshotting for table {} in database {} started", tableName, databaseName);
      StructuredRecord key = Records.convert((Struct) sourceRecord.key());
      List<Schema.Field> fields = key.getSchema().getFields();
      List<String> primaryKeyFields = fields.stream().map(Schema.Field::getName).collect(Collectors.toList());
      // this is a hack to get schema from either 'before' or 'after' field
      Schema schema = op == DMLOperation.DELETE ? before.getSchema() : after.getSchema();

      emitter.emit(DDLEvent.builder()
                     .setDatabase(databaseName)
                     .setOffset(recordOffset)
                     .setOperation(DDLOperation.CREATE_TABLE)
                     .setTable(tableName)
                     .setSchema(schema)
                     .setPrimaryKey(primaryKeyFields)
                     .setSnapshot(isSnapshot)
                     .build());
      snapshotTrackingTables.add(trackingTable);
    }

    if (!readAllTables && sourceTable.getDmlBlacklist().contains(op)) {
      // do nothing due to it was not set to read all tables and this DML op has been blacklisted for this table
      return;
    }

    DMLEvent.Builder builder = DMLEvent.builder()
      .setDatabase(databaseName)
      .setOffset(recordOffset)
      .setTable(tableName)
      .setIngestTimestamp(ingestTime)
      .setOperation(op)
      .setSnapshot(isSnapshot)
      .setTransactionId(transactionId);

    switch (op) {
      case INSERT:
        emitter.emit(builder.setRow(after).build());
        break;
      case DELETE:
        emitter.emit(builder.setRow(before).build());
        break;
      case UPDATE:
        emitter.emit(builder.setPreviousRow(before).setRow(after).build());
        break;
      default:
        LOG.warn("Skipping unknown operation type '{}'", op);
        return;
    }

    if (isSnapshotCompleted) {
      LOG.info("Finished reading snapshot events for table {} in database {}", tableName, databaseName);
    }
  }

  // This method is used for generating a cdap offsets from debezium sourceRecord.
  private Map<String, byte[]> generateCdapOffsets(SourceRecord sourceRecord) {
    Map<String, byte[]> deltaOffset = new HashMap<>();
    Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
    Struct value = (Struct) sourceRecord.value();
    if (value == null) { // safety check to avoid NPE
      return deltaOffset;
    }
    Struct source = (Struct) value.get("source");
    if (source == null) { // safety check to avoid NPE
      return deltaOffset;
    }

    Long scn = (Long) source.get(SourceInfo.SCN_KEY);
    String lcrPosition = (String) source.get(SourceInfo.LCR_POSITION_KEY);
    Boolean snapshot = (Boolean) source.get(SourceInfo.SNAPSHOT_KEY);
    Boolean snapshotCompleted = (Boolean) sourceOffset.get(OracleConstantOffsetBackingStore.SNAPSHOT_COMPLETED);

    if (scn != null) {
      deltaOffset.put(SourceInfo.SCN_KEY, Bytes.toBytes(scn.toString()));
    }
    if (lcrPosition != null) {
      deltaOffset.put(SourceInfo.LCR_POSITION_KEY, Bytes.toBytes(lcrPosition));
    }
    if (snapshot != null) {
      deltaOffset.put(SourceInfo.SNAPSHOT_KEY, Bytes.toBytes(snapshot.toString()));
    }
    if (snapshotCompleted != null) {
      deltaOffset.put(OracleConstantOffsetBackingStore.SNAPSHOT_COMPLETED, Bytes.toBytes(snapshotCompleted.toString()));
    }

    return deltaOffset;
  }
}
