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

package io.cdap.delta.mysql;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.StopContext;
import io.cdap.delta.plugin.common.DBSchemaHistory;
import io.cdap.delta.plugin.common.NotifyingCompletionCallback;
import io.cdap.delta.plugin.common.RuntimeArguments;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Driver;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reads events from MySQL
 */
public class MySqlEventReader implements EventReader {
  public static final Logger LOG = LoggerFactory.getLogger(MySqlEventReader.class);
  protected static final String SOURCE_CONNECTOR_PREFIX = "source.connector.";
  private final MySqlConfig config;
  private final EventEmitter emitter;
  private final ExecutorService executorService;
  private final DeltaSourceContext context;
  private final Set<SourceTable> sourceTables;
  private final Map<String, String> debeziumConnectorConfigs;
  private EmbeddedEngine engine;
  private volatile boolean failedToStop;

  public MySqlEventReader(Set<SourceTable> sourceTables, MySqlConfig config,
                          DeltaSourceContext context, EventEmitter emitter) {
    this.sourceTables = sourceTables;
    this.config = config;
    this.context = context;
    this.emitter = emitter;
    // Debezium 2.x loads connector classes through the thread context classloader, so the engine thread
    // must use the plugin classloader.
    ClassLoader pluginClassLoader = getClass().getClassLoader();
    this.executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable);
      thread.setContextClassLoader(pluginClassLoader);
      return thread;
    });
    this.failedToStop = false;
    this.debeziumConnectorConfigs = RuntimeArguments.extractPrefixed(SOURCE_CONNECTOR_PREFIX,
                                                                     context.getRuntimeArguments());
  }

  @Override
  public void start(Offset offset) {
    // load mysql jdbc driver into class loader here and use this loaded jdbc class and class loader to set static
    // variable 'jdbcDriverClass' in MySqlConnection and static variable 'jdbcClassLoader' in MySqlAntlrDdlParser.
    // note that, this is a short-term hacky solution for us to solve the problem of excluding mysql-jdbc jar from MySql
    // delta plugin's dependencies in CDAP.
    Class<? extends Driver> jdbcDriverClass = context.loadPluginClass(config.getJDBCPluginId());
    MySqlConnection.jdbcDriverClass = jdbcDriverClass;
    MySqlAntlrDdlParser.jdbcClassLoader = jdbcDriverClass.getClassLoader();

    // For MySQL, the unique table identifier in debezium is 'databaseName.tableName'
    Map<String, SourceTable> sourceTableMap = sourceTables.stream().collect(
      Collectors.toMap(t -> config.getDatabase() + "." + t.getTable(), t -> t));
    Map<String, String> state = offset.get(); // state map is always not null
    String isSnapshot = state.getOrDefault(MySqlConstantOffsetBackingStore.SNAPSHOT, "");
    String replicationConnectorName = "delta" + context.getInstanceId();
    Configuration.Builder configBuilder = Configuration.create()
      .with("connector.class", MySqlConnector.class.getName())
      .with("offset.storage", MySqlConstantOffsetBackingStore.class.getName())
      .with("offset.flush.interval.ms", 1000)
      /* bind offset configs with debeizumConf */
      .with("file", state.getOrDefault(MySqlConstantOffsetBackingStore.FILE, ""))
      .with("pos", state.getOrDefault(MySqlConstantOffsetBackingStore.POS, ""))
      .with("snapshot", isSnapshot)
      .with("row", state.getOrDefault(MySqlConstantOffsetBackingStore.ROW, ""))
      .with("event", state.getOrDefault(MySqlConstantOffsetBackingStore.EVENT, ""))
      .with("gtids", state.getOrDefault(MySqlConstantOffsetBackingStore.GTID_SET, ""))
      /* begin connector properties */
      .with("name", replicationConnectorName)
      .with("database.hostname", config.getHost())
      .with("database.port", config.getPort())
      .with("database.user", config.getUser())
      .with("database.password", config.getPassword())
      .with("schema.history.internal", DBSchemaHistory.class.getName())
      .with("database.include.list", config.getDatabase())
      .with("topic.prefix", "dummy") // this is the kafka topic prefix for hosted debezium - it doesn't matter
      .with("database.serverTimezone", config.getServerTimezone())
      .with("schema.history.internal.store.only.captured.tables.ddl", true)
      .with("table.include.list", String.join(",", sourceTableMap.keySet()))
      .with("snapshot.mode", config.getReplicateExistingData() ? "initial" : "schema_only")
      .with(MySqlConstantOffsetBackingStore.REPLICATION_CONNECTOR_NAME, replicationConnectorName);

    if (config.getConsumerID() != null) {
      configBuilder = configBuilder.with("database.server.id", config.getConsumerID() + context.getInstanceId());
    } else {
      // Debezium 2.x no longer picks a default, so keep the Debezium 1.x behavior of a random id in [5400, 6400).
      configBuilder = configBuilder.with("database.server.id", 5400 + new Random().nextInt(1000));
    }

    LOG.info("Overriding mysql connector configs with arguments {}", debeziumConnectorConfigs);
    for (Map.Entry<String, String> entry: debeziumConnectorConfigs.entrySet()) {
      configBuilder = configBuilder.with(entry.getKey(), entry.getValue());
      // Debezium 2.x renamed 'database.history.*' to 'schema.history.internal.*'; keep honoring the old names.
      if (entry.getKey().equals("database.history.store.only.monitored.tables.ddl")) {
        configBuilder = configBuilder.with("schema.history.internal.store.only.captured.tables.ddl", entry.getValue());
      } else if (entry.getKey().startsWith("database.history.")) {
        configBuilder = configBuilder.with(
          "schema.history.internal." + entry.getKey().substring("database.history.".length()), entry.getValue());
      }
    }

    Configuration debeziumConf = configBuilder.build();
    LOG.info("Debezium configuration : {}", debeziumConf);

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      MySqlConnectorConfig mysqlConf = new MySqlConnectorConfig(debeziumConf);
      DBSchemaHistory.deltaRuntimeContext = context;
      /*
       * All snapshot events or schema history record have same position/offset
       * if replicator was stopped  or paused from middle of snapshot, it
       * will resume from beginning.
       */
      if (offset.get().isEmpty() || "true".equalsIgnoreCase(isSnapshot)) {
        try {
          DBSchemaHistory.wipeHistory();
        } catch (IOException e) {
          throw new RuntimeException("Unable to wipe schema history at start of replication.", e);
        }
      }

      MySqlValueConverters mySqlValueConverters = getValueConverters(mysqlConf);
      DdlParser ddlParser = new MySqlAntlrDdlParser(mySqlValueConverters, tableId -> true);

      // Create the engine with this configuration ...
      engine = EmbeddedEngine.create()
        .using(debeziumConf)
        .notifying(new MySqlRecordConsumer(context, emitter, ddlParser, mySqlValueConverters,
                                           new Tables(), sourceTableMap, config.getReplicateExistingData()))
        .using(new NotifyingCompletionCallback(context))
        .build();
      executorService.submit(engine);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  public void stop(StopContext stopContext) throws InterruptedException {
    LOG.info("Stopping debezium engine, reason: " + stopContext.getOrigin());
    // Debezium engine is implicitly stopped as part of this call
    // Refer EmbeddedEngine docs
    executorService.shutdownNow();
    if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
      LOG.warn("Unable to cleanly shutdown reader within the timeout.");
      failedToStop = true;
    }
  }

  @VisibleForTesting
  boolean failedToStop() {
    return failedToStop;
  }

  private static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
    // Use MySQL-specific converters and schemas for values ...

    String timePrecisionModeStr = configuration.getConfig().getString(MySqlConnectorConfig.TIME_PRECISION_MODE);
    TemporalPrecisionMode timePrecisionMode = TemporalPrecisionMode.parse(timePrecisionModeStr);

    JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();

    String bigIntUnsignedHandlingModeStr = configuration.getConfig()
      .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
    MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
      MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
    JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

    boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
    return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                                    configuration.binaryHandlingMode(),
                                    timeAdjusterEnabled ? MySqlEventReader::adjustTemporal : x -> x,
                                    (message, exception) -> {
      throw new DebeziumException(message, exception);
    });
  }

  private static Temporal adjustTemporal(Temporal temporal) {
    if (temporal.isSupported(ChronoField.YEAR)) {
      int year = temporal.get(ChronoField.YEAR);
      if (0 <= year && year <= 69) {
        temporal = temporal.plus(2000, ChronoUnit.YEARS);
      } else if (70 <= year && year <= 99) {
        temporal = temporal.plus(1900, ChronoUnit.YEARS);
      }
    }
    return temporal;
  }
}
