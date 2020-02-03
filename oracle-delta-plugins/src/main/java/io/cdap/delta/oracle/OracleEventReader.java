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
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.common.ConstantOffsetBackingStore;
import io.cdap.delta.common.DBSchemaHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Event reader for oracle.
 */
public class OracleEventReader implements EventReader {
  private static final Logger LOG = LoggerFactory.getLogger(OracleEventReader.class);
  private final OracleConfig config;
  private final DeltaSourceContext context;
  private final ExecutorService executorService;
  private final EventEmitter emitter;
  private EmbeddedEngine engine;
  private Set<String> snapshotTableStore;

  public OracleEventReader(OracleConfig config, DeltaSourceContext context, EventEmitter emitter) {
    this.config = config;
    this.context = context;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadExecutor();
    this.snapshotTableStore = new HashSet<>();
  }

  @Override
  public void start(Offset offset) {
    byte[] scnBytes = offset.get().get("scn");
    String scn = scnBytes == null ? "" : Long.toString(Bytes.toLong(scnBytes));
    LOG.info("scn : " + scn);

    Configuration debeziumConf = Configuration.create()
      .with("connector.class", OracleConnector.class.getName())
      .with("offset.storage", OracleConstantOffsetBackingStore.class.getName())
      .with("offset.storage.file.filename", scn)
      .with("offset.flush.interval.ms", 1000)
      /* begin connector properties */
      .with("name", "delta")
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("database.dbname", config.getDbName())
      .with("database.pdb.name", config.getPdbName())
      .with("database.out.server.name", config.getOutServerName())
      .with("database.history", DBSchemaHistory.class.getName())
      // workaround fix for issue: https://groups.google.com/forum/#!topic/debezium/FrN5PFRFlmQ
      .with("database.oracle.version", 11)
      .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
      .with("table.whitelist", config.getTableWhiteList())
      .build();

    DBSchemaHistory.deltaRuntimeContext = context;
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

    try {
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .using(debeziumConf)
        .notifying(sourceRecord -> {
          if (sourceRecord.value() == null) {
            return;
          }

          Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
          Boolean snapshotCompleted = (Boolean) sourceOffset.get("snapshot_completed");
          Map<String, byte[]> deltaOffset = new HashMap<>();

          StructuredRecord val = Records.convert((Struct) sourceRecord.value());
          StructuredRecord source = val.get("source");
          String databaseName = config.getDbName();
          String recordName = val.getSchema().getRecordName();
          String[] parts = recordName.split("\\.");
          String tableName = parts[1] + "_" + parts[2]; // schema_table as name
          String transactionId = source.get("txId");
          StructuredRecord before = val.get("before");
          StructuredRecord after = val.get("after");
          Long ingestTime = val.get("ts_ms");
          Long logPosition;

          if (snapshotCompleted == null) {
            logPosition = source.get("scn");
          } else {
            logPosition = (Long) sourceOffset.get("scn");
          }

          deltaOffset.put("scn", Bytes.toBytes(logPosition));
          Offset recordOffset = new Offset(deltaOffset);

          DMLOperation op;
          String opStr = val.get("op");

          if ("c".equals(opStr)) {
            op = DMLOperation.INSERT;
            context.getMetrics().count("ddl.source.INSERT.count", 1);
          } else if ("u".equals(opStr)) {
            op = DMLOperation.UPDATE;
            context.getMetrics().count("ddl.source.UPDATE.count", 1);
          } else if ("d".equals(opStr)) {
            op = DMLOperation.DELETE;
            context.getMetrics().count("ddl.source.DELETE.count", 1);
          } else if ("r".equals(opStr)) {
            // create a normal INSERT DML event, only during snapshotting process that opStr is equal to 'r'
            op = DMLOperation.INSERT;
            context.getMetrics().count("ddl.source.Snapshot.INSERT.count", 1);
          } else {
            LOG.warn("Skipping unknown operation type '{}'", opStr);
            context.getMetrics().count("ddl.source.OTHER.count", 1);
            return;
          }

          if (!snapshotTableStore.contains(tableName)) {
            LOG.info("Snapshotting for table " + tableName + " started.");
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
                           .build());
            snapshotTableStore.add(tableName);
          }

          if (op == DMLOperation.DELETE) {
            emitter.emit(new DMLEvent(recordOffset, op, databaseName, tableName, before, transactionId, ingestTime));
          } else {
            emitter.emit(new DMLEvent(recordOffset, op, databaseName, tableName, after, transactionId, ingestTime));

            if (snapshotCompleted != null) {
              if (snapshotCompleted) {
                LOG.info("Snapshotting for table " + tableName + " completed.");
              } else {
                LOG.debug("Snapshotting for table " + tableName + " was still undergoing.");
              }
            } else {
              LOG.debug("New DML event " + opStr + " for table " + tableName);
            }
          }
        })
        .using((success, message, error) -> {
          if (!success) {
            LOG.error("Failed - {}", message, error);
          }
        })
        .build();

      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  @Override
  public void stop() throws InterruptedException {
    if (engine != null && engine.stop()) {
      engine.await(1, TimeUnit.MINUTES);
    }
    executorService.shutdown();
  }
}
