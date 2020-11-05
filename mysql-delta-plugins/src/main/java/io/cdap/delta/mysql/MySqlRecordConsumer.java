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
import io.cdap.delta.plugin.common.Records;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.embedded.StopConnectorException;
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
import java.util.concurrent.atomic.AtomicReference;
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

    try {
      if (ddl != null) {
        handleDDL(ddl, recordOffset, isSnapshot, readAllTables);
        return;
      }

      String databaseName = source.get("db");
      String tableName = source.get("table");
      SourceTable sourceTable = getSourceTable(databaseName, tableName);
      if (sourceTableNotValid(readAllTables, sourceTable)) {
        return;
      }

      handleDML(source, val, recordOffset, isSnapshot, readAllTables);
    } catch (InterruptedException e) {
      // happens when the event reader is stopped. throwing this exception tells Debezium to stop right away
      throw new StopConnectorException("Interrupted while emitting event.");
    }
  }

  private void handleDML(StructuredRecord source, StructuredRecord val, Offset recordOffset,
                         boolean isSnapshot, boolean readAllTables) throws InterruptedException {
    String databaseName = source.get("db");
    String tableName = source.get("table");
    SourceTable sourceTable = getSourceTable(databaseName, tableName);
    if (sourceTableNotValid(readAllTables, sourceTable)) {
      return;
    }

    DMLOperation.Type op;
    String opStr = val.get("op");
    if ("c".equals(opStr)) {
      op = DMLOperation.Type.INSERT;
    } else if ("u".equals(opStr)) {
      op = DMLOperation.Type.UPDATE;
    } else if ("d".equals(opStr)) {
      op = DMLOperation.Type.DELETE;
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
      .setOperationType(op)
      .setDatabase(databaseName)
      .setTable(tableName)
      .setTransactionId(transactionId)
      .setIngestTimestamp(ingestTime)
      .setSnapshot(isSnapshot);

    // It is required for the source to provide the previous row if the dml operation is 'UPDATE'
    if (op == DMLOperation.Type.UPDATE) {
      emitter.emit(builder.setPreviousRow(before).setRow(after).build());
    } else if (op == DMLOperation.Type.DELETE) {
      emitter.emit(builder.setRow(before).build());
    } else {
      emitter.emit(builder.setRow(after).build());
    }
  }

  private void handleDDL(String ddlStatement, Offset recordOffset,
                         boolean isSnapshot, boolean readAllTables) throws InterruptedException {
    ddlParser.getDdlChanges().reset();
    ddlParser.parse(ddlStatement, tables);
    AtomicReference<InterruptedException> interrupted = new AtomicReference<>();
    ddlParser.getDdlChanges().groupEventsByDatabase((database, events) -> {
      if (interrupted.get() != null) {
        return;
      }
      for (DdlParserListener.Event event : events) {
        DDLEvent.Builder builder = DDLEvent.builder()
          .setOffset(recordOffset)
          .setDatabase(database)
          .setSnapshot(isSnapshot);
        DDLEvent ddlEvent = null;
        // since current ddl blacklist implementation is bind with table level, we will only do the ddl blacklist
        // checking only for table change related ddl event, includes: ALTER_TABLE, RENAME_TABLE, DROP_TABLE,
        // CREATE_TABLE and TRUNCATE_TABLE.
        switch (event.type()) {
          case ALTER_TABLE:
            DdlParserListener.TableAlteredEvent alteredEvent = (DdlParserListener.TableAlteredEvent) event;
            TableId tableId = alteredEvent.tableId();
            Table table = tables.forTable(tableId);
            SourceTable sourceTable = getSourceTable(database, tableId.table());
            DDLOperation.Type ddlOp;
            if (alteredEvent.previousTableId() != null) {
              ddlOp = DDLOperation.Type.RENAME_TABLE;
              builder.setPrevTable(alteredEvent.previousTableId().table());
            } else {
              ddlOp = DDLOperation.Type.ALTER_TABLE;
            }

            if (shouldEmitDdlEventForOperation(readAllTables, sourceTable, ddlOp)) {
              ddlEvent = builder.setOperation(ddlOp)
                .setTable(tableId.table())
                .setSchema(readAllTables ? Records.getSchema(table, mySqlValueConverters) :
                             Records.getSchema(table, mySqlValueConverters, sourceTable.getColumns()))
                .setPrimaryKey(table.primaryKeyColumnNames())
                .build();
            }
            break;
          case DROP_TABLE:
            DdlParserListener.TableDroppedEvent droppedEvent = (DdlParserListener.TableDroppedEvent) event;
            sourceTable = getSourceTable(database, droppedEvent.tableId().table());
            if (shouldEmitDdlEventForOperation(readAllTables, sourceTable, DDLOperation.Type.DROP_TABLE)) {
              ddlEvent = builder.setOperation(DDLOperation.Type.DROP_TABLE)
                .setTable(droppedEvent.tableId().table())
                .build();
            }
            break;
          case CREATE_TABLE:
            DdlParserListener.TableCreatedEvent createdEvent = (DdlParserListener.TableCreatedEvent) event;
            tableId = createdEvent.tableId();
            table = tables.forTable(tableId);
            sourceTable = getSourceTable(database, tableId.table());
            if (shouldEmitDdlEventForOperation(readAllTables, sourceTable, DDLOperation.Type.CREATE_TABLE)) {
              ddlEvent = builder.setOperation(DDLOperation.Type.CREATE_TABLE)
                .setTable(tableId.table())
                .setSchema(readAllTables ? Records.getSchema(table, mySqlValueConverters) :
                             Records.getSchema(table, mySqlValueConverters, sourceTable.getColumns()))
                .setPrimaryKey(table.primaryKeyColumnNames())
                .build();
            }
            break;
          case DROP_DATABASE:
            ddlEvent = builder.setOperation(DDLOperation.Type.DROP_DATABASE).build();
            break;
          case CREATE_DATABASE:
            // due to a bug in io.debezium.relational.ddl.AbstractDdlParser#signalDropDatabase
            // a DROP_DATABASE event will be mistakenly categorized as a CREATE_DATABASE event.
            // TODO: check if this is fixed in a newer debezium version
            if (event.statement() != null && event.statement().startsWith("DROP DATABASE")) {
              ddlEvent = builder.setOperation(DDLOperation.Type.DROP_DATABASE).build();
            } else {
              ddlEvent = builder.setOperation(DDLOperation.Type.CREATE_DATABASE).build();
            }
            break;
          case TRUNCATE_TABLE:
            DdlParserListener.TableTruncatedEvent truncatedEvent =
              (DdlParserListener.TableTruncatedEvent) event;
            sourceTable = getSourceTable(database, truncatedEvent.tableId().table());
            if (shouldEmitDdlEventForOperation(readAllTables, sourceTable, DDLOperation.Type.TRUNCATE_TABLE)) {
              ddlEvent = builder.setOperation(DDLOperation.Type.TRUNCATE_TABLE)
                .setTable(truncatedEvent.tableId().table())
                .build();
            }
            break;
        }
        if (ddlEvent != null) {
          try {
            emitter.emit(ddlEvent);
          } catch (InterruptedException e) {
            interrupted.set(e);
          }
        }
      }
    });
    if (interrupted.get() != null) {
      throw interrupted.get();
    }
  }

  private boolean shouldEmitDdlEventForOperation(boolean readAllTables, SourceTable sourceTable, DDLOperation.Type op) {
    return (!sourceTableNotValid(readAllTables, sourceTable)) &&
      (!isDDLOperationBlacklisted(readAllTables, sourceTable, op));
  }

  private boolean isDDLOperationBlacklisted(boolean readAllTables, SourceTable sourceTable, DDLOperation.Type op) {
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
    Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
    String binlogFile = (String) sourceOffset.get(MySqlConstantOffsetBackingStore.FILE);
    Long binlogPosition = (Long) sourceOffset.get(MySqlConstantOffsetBackingStore.POS);
    Boolean snapshot = (Boolean) sourceOffset.get(MySqlConstantOffsetBackingStore.SNAPSHOT);
    Long rowsToSkip = (Long) sourceOffset.get(MySqlConstantOffsetBackingStore.ROW);
    Long eventsToSkip = (Long) sourceOffset.get(MySqlConstantOffsetBackingStore.EVENT);
    String gtidSet = (String) sourceOffset.get(MySqlConstantOffsetBackingStore.GTID_SET);

    if (binlogFile != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.FILE, binlogFile);
    }
    if (binlogPosition != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.POS, String.valueOf(binlogPosition));
    }
    if (snapshot != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.SNAPSHOT, String.valueOf(snapshot));
    }
    if (rowsToSkip != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.ROW, String.valueOf(rowsToSkip));
    }
    if (eventsToSkip != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.EVENT, String.valueOf(eventsToSkip));
    }
    if (gtidSet != null) {
      deltaOffset.put(MySqlConstantOffsetBackingStore.GTID_SET, gtidSet);
    }

    return deltaOffset;
  }
}
