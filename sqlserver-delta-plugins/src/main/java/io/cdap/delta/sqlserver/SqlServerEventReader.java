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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.plugin.common.DBSchemaHistory;
import io.cdap.delta.plugin.common.NotifyingCompletionCallback;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Driver;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Sql server event reader
 */
public class SqlServerEventReader implements EventReader {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerEventReader.class);
  private final SqlServerConfig config;
  private final EventEmitter emitter;
  private final DeltaSourceContext context;
  private final ExecutorService executorService;
  private final Set<SourceTable> tables;
  private volatile boolean failedStopping;
  private EmbeddedEngine engine;

  public SqlServerEventReader(Set<SourceTable> tables, SqlServerConfig config,
                              DeltaSourceContext context, EventEmitter emitter) {
    this.config = config;
    this.emitter = emitter;
    this.context = context;
    this.tables = tables;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.failedStopping = false;
  }

  @Override
  public void start(Offset offset) {

    LOG.info("starting event reader with offset:");
    for (Map.Entry<String, String> entry : offset.get().entrySet()) {
      LOG.info(" {} = {}", entry.getKey(), entry.getValue());
    }
    // load sql server jdbc driver into class loader and use this loaded jdbc class to set the static factory
    // variable in SqlServerConnection for instantiation purpose later on.
    Class<? extends Driver> jdbcDriverClass = context.loadPluginClass(config.getJDBCPluginId());
    String urlPattern = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}:${" +
      JdbcConfiguration.PORT + "};databaseName=${" + JdbcConfiguration.DATABASE + "}";
    SqlServerConnection.factory = JdbcConnection.patternBasedFactory(urlPattern,
                                                                     jdbcDriverClass.getName(),
                                                                     jdbcDriverClass.getClassLoader());

    // this is needed since sql server does not return the database information in the record
    String databaseName = config.getDatabase();

    Map<String, SourceTable> sourceTableMap = tables.stream().collect(
      Collectors.toMap(
        t -> {
          String schema = t.getSchema();
          String table = t.getTable();
          return schema == null ? table : schema + "." + table;
        }, t -> t));

    Map<String, String> state = offset.get(); // this will never be null
    // offset config
    String isSnapshotCompleted = state.getOrDefault(SqlServerConstantOffsetBackingStore.SNAPSHOT_COMPLETED, "");
    Configuration debeziumConf = Configuration.create()
      .with("connector.class", SqlServerConnector.class.getName())
      .with("offset.storage", SqlServerConstantOffsetBackingStore.class.getName())
      .with("offset.flush.interval.ms", 1000)
      /* bind offset configs with debeizumConf */
      .with("change_lsn", state.getOrDefault(SourceInfo.CHANGE_LSN_KEY, ""))
      .with("commit_lsn", state.getOrDefault(SourceInfo.COMMIT_LSN_KEY, ""))
      .with("snapshot", state.getOrDefault(SourceInfo.SNAPSHOT_KEY, ""))
      .with("snapshot_completed", isSnapshotCompleted)
      /* begin connector properties */
      .with("name", "delta")
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("database.history", DBSchemaHistory.class.getName())
      .with("database.dbname", databaseName)
      .with("table.whitelist", String.join(",", sourceTableMap.keySet()))
      .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
      .with("database.serverTimezone", config.getServerTimezone())
      .build();

    DBSchemaHistory.deltaRuntimeContext = context;

    String snapshotTablesStr = state.get(SqlServerOffset.SNAPSHOT_TABLES);
    Set<String> snapshotTables = Strings.isNullOrEmpty(snapshotTablesStr) ? new HashSet<>() :
      new HashSet<>(Arrays.asList(snapshotTablesStr.split(SqlServerOffset.DELIMITER)));

    /*
     * All snapshot events or schema history record have same position/offset
     * if replicator was stopped  or paused from middle of snapshot, it
     * will resume from beginning.
     */
    if (offset.get().isEmpty() || !"true".equalsIgnoreCase(isSnapshotCompleted)) {
      try {
        DBSchemaHistory.wipeHistory();
      } catch (IOException e) {
        throw new RuntimeException("Unable to wipe schema history at start of replication.", e);
      }
    }

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

    try {
      LOG.info("creating new EmbeddedEngine...");
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .notifying(new SqlServerRecordConsumer(context, emitter, databaseName, snapshotTables, sourceTableMap, offset))
        .using(debeziumConf)
        .using(new NotifyingCompletionCallback(context))
        .build();
      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  @Override
  public void stop() throws InterruptedException {
    executorService.shutdownNow();
    if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
      failedStopping = true;
      LOG.warn("Unable to cleanly shutdown reader within the timeout.");
    }
  }

  @VisibleForTesting
  boolean failedToStop() {
    return failedStopping;
  }
}
