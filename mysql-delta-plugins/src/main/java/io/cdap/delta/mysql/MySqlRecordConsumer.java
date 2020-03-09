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

package io.cdap.delta.mysql;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.common.Records;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Record consumer for MySQL.
 */
public class MySqlRecordConsumer implements Consumer<SourceRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordConsumer.class);

  private final DeltaSourceContext context;
  private final EventEmitter emitter;
  private final DdlParser ddlParser;
  private final MySqlValueConverters mySqlValueConverters;
  private final Tables tables;
  private final Map<String, SourceTable> sourceTableMap;

  public MySqlRecordConsumer(DeltaSourceContext context, EventEmitter emitter,
                             DdlParser ddlParser, MySqlValueConverters mySqlValueConverters,
                             Tables tables, Map<String, SourceTable> sourceTableMap) {
    this.context = context;
    this.emitter = emitter;
    this.ddlParser = ddlParser;
    this.mySqlValueConverters = mySqlValueConverters;
    this.tables = tables;
    this.sourceTableMap = sourceTableMap;
  }

  @Override
  public void accept(SourceRecord sourceRecord) {
    /*
       For ddl, struct contains 3 top level fields:
         source struct
         databaseName string
         ddl string

       Before every DDL event, a weird event with ddl='# Dum' is consumed before the actual event.

       source is a struct with 14 fields:
         0 - version string (debezium version)
         1 - connector string
         2 - name string (name of the consumer, set when creating the Configuration)
         3 - server_id int64
         4 - ts_sec int64
         5 - gtid string
         6 - file string
         7 - pos int64     (there can be multiple events for the same file and position.
                            Everything in same position seems to be anything done in the same query)
         8 - row int32     (if multiple rows are involved in the same transaction, this is the row #)
         9 - snapshot boolean (null is the same as false)
         10 - thread int64
         11 - db string
         12 - table string
         13 - query string

       For dml, struct contains 5 top level fields:
         0 - before struct
         1 - after struct
         2 - source struct
         3 - op string (c for create or insert, u for update, d for delete, and r for read)
                        not sure when 'r' happens, it's not for select queries...
         4 - ts_ms int64 (this is *not* the timestamp of the event, but the timestamp when Debezium read it)
       before is a struct representing the row before the operation. It will have a schema matching the table schema
       after is a struct representing the row after the operation. It will have a schema matching the table schema
     */

    try {
      context.setOK();
    } catch (IOException e) {
      LOG.warn("Unable to set source state to OK.", e);
    }

    if (sourceRecord.value() == null) {
      return;
    }

    Map<String, String> deltaOffset = generateCdapOffsets(sourceRecord);
    Offset recordOffset = new Offset(deltaOffset);

    StructuredRecord val = Records.convert((Struct) sourceRecord.value());
    String ddl = val.get("ddl");
    StructuredRecord source = val.get("source");
    if (source == null) {
      // This should not happen, 'source' is a mandatory field in sourceRecord from debezium
      return;
    }
    boolean isSnapshot = Boolean.TRUE.equals(source.get(MySqlConstantOffsetBackingStore.SNAPSHOT));
    // If the map is empty, we should read all DDL/DML events and columns of all tables
    boolean readAllTables = sourceTableMap.isEmpty();

    if (ddl != null) {
      ddlParser.getDdlChanges().reset();
      ddlParser.parse(ddl, tables);
      ddlParser.getDdlChanges().groupEventsByDatabase((database, events) -> {
        for (DdlParserListener.Event event : events) {
          DDLEvent.Builder builder = DDLEvent.builder()
            .setDatabase(database)
            .setOffset(recordOffset)
            .setSnapshot(isSnapshot);
          // since current ddl blacklist implementation is bind with table level, we will only do the dll blacklist
          // checking only for table change related ddl event(ALTER_TABLE/DROP_TABLE/CREATE_TABLE/TRUNCATE_TABLE).
          switch (event.type()) {
            case ALTER_TABLE:
              DdlParserListener.TableAlteredEvent alteredEvent = (DdlParserListener.TableAlteredEvent) event;
              TableId tableId = alteredEvent.tableId();
              Table table = tables.forTable(tableId);
              SourceTable sourceTable = getSourceTable(database, tableId.table());

              if (alteredEvent.previousTableId() != null) {
                builder.setOperation(DDLOperation.RENAME_TABLE)
                  .setPrevTable(alteredEvent.previousTableId().table());
              } else {
                builder.setOperation(DDLOperation.ALTER_TABLE);
              }
              emitter.emit(builder.setTable(tableId.table())
                             .setSchema(readAllTables ? Records.getSchema(table, mySqlValueConverters) :
                                          Records.getSchema(table, mySqlValueConverters, sourceTable.getColumns()))
                             .setPrimaryKey(table.primaryKeyColumnNames())
                             .build());
              break;
            case DROP_TABLE:
              DdlParserListener.TableDroppedEvent droppedEvent = (DdlParserListener.TableDroppedEvent) event;
              sourceTable = getSourceTable(database, droppedEvent.tableId().table());
              if (!sourceTableNotValid(readAllTables, sourceTable) &&
                !isDDLOperationBlacklisted(readAllTables, sourceTable, DDLOperation.DROP_TABLE)) {
                emitter.emit(builder.setOperation(DDLOperation.DROP_TABLE)
                               .setTable(droppedEvent.tableId().table())
                               .build());
              }
              break;
            case CREATE_TABLE:
              DdlParserListener.TableCreatedEvent createdEvent = (DdlParserListener.TableCreatedEvent) event;
              tableId = createdEvent.tableId();
              table = tables.forTable(tableId);
              sourceTable = getSourceTable(database, tableId.table());
              if (!sourceTableNotValid(readAllTables, sourceTable) &&
                !isDDLOperationBlacklisted(readAllTables, sourceTable, DDLOperation.CREATE_TABLE)) {
                emitter.emit(builder.setOperation(DDLOperation.CREATE_TABLE)
                               .setTable(tableId.table())
                               .setSchema(readAllTables ? Records.getSchema(table, mySqlValueConverters) :
                                            Records.getSchema(table, mySqlValueConverters, sourceTable.getColumns()))
                               .setPrimaryKey(table.primaryKeyColumnNames())
                               .build());
              }
              break;
            case DROP_DATABASE:
              emitter.emit(builder.setOperation(DDLOperation.DROP_DATABASE).build());
              break;
            case CREATE_DATABASE:
              emitter.emit(builder.setOperation(DDLOperation.CREATE_DATABASE).build());
              break;
            case TRUNCATE_TABLE:
              DdlParserListener.TableTruncatedEvent truncatedEvent =
                (DdlParserListener.TableTruncatedEvent) event;
              sourceTable = getSourceTable(database, truncatedEvent.tableId().table());
              if (!sourceTableNotValid(readAllTables, sourceTable) &&
                !isDDLOperationBlacklisted(readAllTables, sourceTable, DDLOperation.TRUNCATE_TABLE)) {
                emitter.emit(builder.setOperation(DDLOperation.TRUNCATE_TABLE)
                               .setTable(truncatedEvent.tableId().table())
                               .build());
              }
              break;
            default:
              return;
          }
        }
      });
      return;
    }

    String databaseName = source.get("db");
    String tableName = source.get("table");
    SourceTable sourceTable = getSourceTable(databaseName, tableName);
    if (sourceTableNotValid(readAllTables, sourceTable)) {
      return;
    }

    DMLOperation op;
    String opStr = val.get("op");
    if ("c".equals(opStr)) {
      op = DMLOperation.INSERT;
    } else if ("u".equals(opStr)) {
      op = DMLOperation.UPDATE;
    } else if ("d".equals(opStr)) {
      op = DMLOperation.DELETE;
    } else {
      LOG.warn("Skipping unknown operation type '{}'", opStr);
      return;
    }

    if (!readAllTables && sourceTable.getDmlBlacklist().contains(op)) {
      // do nothing due to it was not set to read all tables and the DML op has been blacklisted for this table
      return;
    }

    String transactionId = source.get("gtid");
    if (transactionId == null) {
      // this is not really a transaction id, but we don't get an event when a transaction started/ended
      transactionId = String.format("%s:%d",
                                    source.get(MySqlConstantOffsetBackingStore.FILE),
                                    source.get(MySqlConstantOffsetBackingStore.POS));
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

    Long ingestTime = val.get("ts_ms");
    DMLEvent.Builder builder = DMLEvent.builder()
      .setOffset(recordOffset)
      .setOperation(op)
      .setDatabase(databaseName)
      .setTable(tableName)
      .setTransactionId(transactionId)
      .setIngestTimestamp(ingestTime)
      .setSnapshot(isSnapshot);

    // It is required for the source to provide the previous row if the dml operation is 'UPDATE'
    if (op == DMLOperation.UPDATE) {
      emitter.emit(builder.setPreviousRow(before).setRow(after).build());
    } else if (op == DMLOperation.DELETE) {
      emitter.emit(builder.setRow(before).build());
    } else {
      emitter.emit(builder.setRow(after).build());
    }
  }

  private boolean isDDLOperationBlacklisted(boolean readAllTables, SourceTable sourceTable, DDLOperation op) {
    // return true if record consumer was not set to read all table events and the DDL op has been
    // blacklisted for this table
    return !readAllTables && sourceTable.getDdlBlacklist().contains(op);
  }

  private boolean sourceTableNotValid(boolean readAllTables, SourceTable sourceTable) {
    // this should not happen, in this case, just return the result here and let caller to handle
    return !readAllTables && sourceTable == null;
  }

  private SourceTable getSourceTable(String database, String table) {
    String sourceTableId = database + "." + table;
    return sourceTableMap.get(sourceTableId);
  }

  // This method is used for generating a cdap offsets from debezium sourceRecord.
  private Map<String, String> generateCdapOffsets(SourceRecord sourceRecord) {
    Map<String, String> deltaOffset = new HashMap<>();
    Struct value = (Struct) sourceRecord.value();
    if (value == null) { // safety check to avoid NPE
      return deltaOffset;
    }

    Struct source = (Struct) value.get("source");
    if (source == null) { // safety check to avoid NPE
      return deltaOffset;
    }

    String binlogFile = (String) source.get(MySqlConstantOffsetBackingStore.FILE);
    Long binlogPosition = (Long) source.get(MySqlConstantOffsetBackingStore.POS);
    Boolean snapshot = (Boolean) source.get(MySqlConstantOffsetBackingStore.SNAPSHOT);

    if (binlogFile != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.FILE, binlogFile);
    }
    if (binlogPosition != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.POS, String.valueOf(binlogPosition));
    }
    if (snapshot != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.SNAPSHOT, String.valueOf(snapshot));
    }

    return deltaOffset;
  }
}
