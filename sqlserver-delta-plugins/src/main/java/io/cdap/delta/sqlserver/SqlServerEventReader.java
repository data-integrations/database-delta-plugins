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

import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.common.DBSchemaHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.embedded.EmbeddedEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
  private final List<SourceTable> tables;
  private EmbeddedEngine engine;

  public SqlServerEventReader(List<SourceTable> tables, SqlServerConfig config,
                              DeltaSourceContext context, EventEmitter emitter) {
    this.config = config;
    this.emitter = emitter;
    this.context = context;
    this.tables = tables;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start(Offset offset) {
    // this is needed since sql server does not return the database information in the record
    String databaseName = tables.isEmpty() ? null : tables.get(0).getDatabase();
    List<String> tableList = tables.stream().map(SourceTable::getTable).collect(Collectors.toList());

    // offset config
    Configuration.Builder builder = Configuration.create()
                                   .with("connector.class", SqlServerConnector.class.getName())
                                   .with("offset.storage", SqlServerConstantOffsetBackingStore.class.getName())
                                   .with("offset.flush.interval.ms", 1000);
    SqlServerConstantOffsetBackingStore.deserializeOffsets(offset.get()).forEach(builder::with);

    Configuration debeziumConf =
      builder
        /* begin connector properties */
        .with("name", "delta")
        .with("database.hostname", config.getHost())
        .with("database.port", config.getPort())
        .with("database.user", config.getUser())
        .with("database.password", config.getPassword())
        .with("database.history", DBSchemaHistory.class.getName())
        .with("database.dbname", databaseName)
        .with("table.whitelist", String.join(",", tableList))
        .with("database.server.name", "dummy") // this is the kafka topic for hosted debezium - it doesn't matter
        .with("database.serverTimezone", config.getServerTimezone())
        .build();
    DBSchemaHistory.deltaRuntimeContext = context;

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

    try {
      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
                 .using(debeziumConf)
                 .notifying(new SqlServerRecordConsumer(emitter, databaseName))
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
